/**
 * dndRouter.ts — DND-Aware Routing Engine for Nigerian Telecoms
 *
 * Implements Do-Not-Disturb (DND) compliance logic required by Nigerian
 * telecom regulations. The engine checks a Redis-cached DND registry to
 * determine the correct routing path for each message:
 *
 *   - OTP/Transactional messages to DND numbers: Route through whitelisted
 *     "Direct/Corporate" SMPP bindings (exempt from DND restrictions).
 *   - Promotional messages to DND numbers: Reject or divert to an alternative
 *     marketing lane with lower priority.
 *   - Non-DND numbers: Normal routing through the standard operator pool.
 *
 * The DND set is cached in Redis (SISMEMBER check) for sub-millisecond lookups.
 * A TTL-based refresh mechanism ensures the cache stays current.
 */

import Redis, { Cluster } from 'ioredis';
import { TelcoOperator } from './types';
import { REDIS_KEYS } from './config';
import { createLogger } from './logger';
import { redisSismember, redisSmembers, redisSadd, redisSrem } from './lib/redisClient';

const log = createLogger('DndRouter');

// ─── DND Route Action ──────────────────────────────────────────────────────

export enum DndAction {
  /** Route through corporate/whitelisted SMPP bind (for OTP/transactional) */
  ROUTE_CORPORATE = 'route_corporate',
  /** Route through standard marketing lane with lower priority */
  ROUTE_MARKETING = 'route_marketing',
  /** Reject the message entirely */
  REJECT = 'reject',
  /** No DND restrictions — route normally */
  NORMAL = 'normal',
}

export interface DndRouteResult {
  action: DndAction;
  reason: string;
  operator: TelcoOperator;
}

// ─── DND Router ────────────────────────────────────────────────────────────

export class DndRouter {
  private redisClient: Redis | Cluster | null = null;
  private localDndCache: Set<string> = new Set();
  private cacheLastRefresh: number = 0;
  private readonly cacheTtlMs: number = 60_000; // Refresh from Redis every 60s
  private redisAvailable: boolean = false;

  constructor(redisClient?: Redis | Cluster) {
    if (redisClient) {
      this.redisClient = redisClient;
      this.redisAvailable = true;
    }
    log.info('DND Router initialized' + (this.redisAvailable ? '' : ' (no Redis — local cache only)'));
  }

  // ─── Core Routing Decision ───────────────────────────────────────────────

  /**
   * Determine the routing action for a given recipient number and message type.
   *
   * @param normalizedRecipient - E.164 normalized phone number (e.g., "2348031234567")
   * @param operator - The detected telco operator
   * @param messageType - The type of message ('otp', 'transactional', 'bulk')
   * @returns DndRouteResult with the routing action and reason
   */
  async determineRoute(
    normalizedRecipient: string,
    operator: TelcoOperator,
    messageType: 'otp' | 'transactional' | 'bulk' | 'promotional'
  ): Promise<DndRouteResult> {
    const isDnd = await this.isDndNumber(normalizedRecipient);

    if (!isDnd) {
      return {
        action: DndAction.NORMAL,
        reason: 'Number not on DND registry',
        operator,
      };
    }

    // Number is on DND — apply routing rules based on message type
    switch (messageType) {
      case 'otp':
      case 'transactional':
        // OTP and transactional are exempt from DND restrictions
        return {
          action: DndAction.ROUTE_CORPORATE,
          reason: `DND number, ${messageType} exempt — routing via corporate bind`,
          operator,
        };

      case 'bulk':
      case 'promotional':
        // Promotional messages must be blocked or diverted for DND numbers
        return {
          action: DndAction.REJECT,
          reason: 'DND number — promotional messages not permitted',
          operator,
        };

      default:
        return {
          action: DndAction.NORMAL,
          reason: 'Unknown message type, routing normally',
          operator,
        };
    }
  }

  // ─── DND Number Check ────────────────────────────────────────────────────

  /**
   * Check if a phone number is registered on the DND list.
   *
   * Priority:
   *   1. Try Redis SISMEMBER (sub-millisecond)
   *   2. Fall back to local in-memory cache
   *   3. If neither available, assume NOT DND (fail open for safety)
   *
   * @param normalizedNumber - E.164 formatted phone number
   * @returns true if the number is on the DND registry
   */
  private async isDndNumber(normalizedNumber: string): Promise<boolean> {
    // Try Redis first (production path)
    if (this.redisAvailable && this.redisClient) {
      try {
        return await redisSismember(
          this.redisClient,
          REDIS_KEYS.DND_SET,
          normalizedNumber
        );
      } catch (err) {
        log.warn(`Redis DND SISMEMBER failed, falling back to local cache`);
        this.redisAvailable = false;
      }
    }

    // Fallback: local in-memory cache
    return this.localDndCache.has(normalizedNumber);
  }

  // ─── Cache Management ────────────────────────────────────────────────────

  /**
   * Refresh the local DND cache from Redis.
   * Called periodically to keep the cache in sync.
   */
  async refreshCache(): Promise<void> {
    const now = Date.now();
    if (now - this.cacheLastRefresh < this.cacheTtlMs) return;

    if (!this.redisClient) {
      this.cacheLastRefresh = now;
      return;
    }

    try {
      const dndNumbers = await redisSmembers(this.redisClient, REDIS_KEYS.DND_SET);
      this.localDndCache = new Set(dndNumbers);
      this.cacheLastRefresh = now;
      this.redisAvailable = true;
      log.info(`DND cache refreshed: ${dndNumbers.length} numbers loaded`);
    } catch (err) {
      log.warn('Failed to refresh DND cache, using stale data');
      this.redisAvailable = false;
    }
  }

  /**
   * Add a number to the DND registry (admin operation).
   */
  async addDndNumber(normalizedNumber: string): Promise<void> {
    this.localDndCache.add(normalizedNumber);

    if (this.redisClient) {
      try {
        await redisSadd(this.redisClient, REDIS_KEYS.DND_SET, normalizedNumber);
      } catch (err) {
        log.warn(`Failed to add DND number to Redis: ${(err as Error).message}`);
      }
    }

    log.info(`Added ${normalizedNumber} to DND registry`);
  }

  /**
   * Remove a number from the DND registry (admin operation).
   */
  async removeDndNumber(normalizedNumber: string): Promise<void> {
    this.localDndCache.delete(normalizedNumber);

    if (this.redisClient) {
      try {
        await redisSrem(this.redisClient, REDIS_KEYS.DND_SET, normalizedNumber);
      } catch (err) {
        log.warn(`Failed to remove DND number from Redis: ${(err as Error).message}`);
      }
    }

    log.info(`Removed ${normalizedNumber} from DND registry`);
  }

  /**
   * Set the Redis client after construction (for late binding).
   */
  setRedisClient(client: Redis | Cluster): void {
    this.redisClient = client;
    this.redisAvailable = true;
  }
}