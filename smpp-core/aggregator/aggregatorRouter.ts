/**
 * aggregatorRouter.ts — Intent-Based Aggregator Routing Engine
 *
 * Routes messages to the appropriate aggregator provider based on:
 *   - Message intent (Transactional vs. Promotional)
 *   - Provider health (rolling success rate)
 *   - Admin overrides (stored in Redis)
 *
 * Routing rules:
 *   - Transactional (OTP, alerts) → Termii (fast, reliable, ~65kobo/seg)
 *   - Promotional (marketing)    → BulkSMS (cost-effective, ~40kobo/seg)
 *   - If primary is unhealthy    → failover to fallback provider
 *   - Admin can override via Redis
 */

import Redis, { Cluster } from 'ioredis';
import { AggregatorProvider, MessageIntent, AggregatorHealth, AggregatorSendResult } from '../types';
import { AGGREGATORS } from '../config';
import { createLogger } from '../logger';
import { RouteDecision, ProviderHealthState } from './aggregatorTypes';
import { AggregatorHttpClient } from './aggregatorTypes';

const log = createLogger('AggregatorRouter');

// ─── Redis Keys ───────────────────────────────────────────────────────────

const REDIS_ROUTE_OVERRIDE = (intent: string) => `aggregator:route:intent:${intent}`;
const REDIS_PROVIDER_HEALTH = (provider: string) => `aggregator:health:${provider}`;

// ─── Aggregator Router ────────────────────────────────────────────────────

export class AggregatorRouter {
  private clients: Map<AggregatorProvider, AggregatorHttpClient> = new Map();
  private healthStates: Map<AggregatorProvider, ProviderHealthState> = new Map();
  private redisClient: Redis | Cluster | null = null;

  constructor(redisClient?: Redis | Cluster) {
    if (redisClient) {
      this.redisClient = redisClient;
    }
  }

  /**
   * Register an aggregator HTTP client with the router.
   * Must be called before routing.
   */
  registerClient(client: AggregatorHttpClient): void {
    this.clients.set(client.provider, client);
    this.healthStates.set(client.provider, {
      provider: client.provider,
      failureTimestamps: [],
      requestCount: 0,
      totalResponseTimeMs: 0,
      lastCheckedAt: Date.now(),
      isAvailable: true,
    });
    log.info(`Aggregator client registered: ${client.provider}`);
  }

  // ─── Route Decision ─────────────────────────────────────────────────────

  /**
   * Determine the optimal route for a message based on its intent.
   *
   * @param intent - The message intent (transactional or promotional)
   * @returns RouteDecision with primary and optional fallback provider
   */
  async getRoute(intent: MessageIntent): Promise<RouteDecision> {
    // Check for admin override in Redis
    const override = await this.getRouteOverride(intent);
    if (override) {
      return {
        intent,
        primaryProvider: override,
        reason: `Admin override: ${override}`,
      };
    }

    // Determine configured primary provider for this intent
    const primaryProvider = intent === MessageIntent.TRANSACTIONAL
      ? AGGREGATORS.ROUTE.TRANSACTIONAL_PROVIDER as AggregatorProvider
      : AGGREGATORS.ROUTE.PROMOTIONAL_PROVIDER as AggregatorProvider;

    const fallbackProvider = AGGREGATORS.ROUTE.FALLBACK_PROVIDER as AggregatorProvider;

    // Check health of primary provider
    if (this.isProviderHealthy(primaryProvider)) {
      return {
        intent,
        primaryProvider,
        fallbackProvider: primaryProvider !== fallbackProvider ? fallbackProvider : undefined,
        reason: `${intent} → ${primaryProvider} (healthy)`,
      };
    }

    // Primary is unhealthy — use fallback
    log.warn(
      `Primary provider ${primaryProvider} unhealthy for ${intent}, ` +
      `falling back to ${fallbackProvider}`
    );

    return {
      intent,
      primaryProvider: fallbackProvider,
      reason: `${intent} → ${fallbackProvider} (${primaryProvider} unhealthy)`,
    };
  }

  // ─── Health Tracking ────────────────────────────────────────────────────

  /**
   * Record a successful send through a provider.
   */
  recordSuccess(provider: AggregatorProvider, responseTimeMs: number): void {
    const state = this.healthStates.get(provider);
    if (!state) return;

    state.requestCount++;
    state.totalResponseTimeMs += responseTimeMs;
    state.lastCheckedAt = Date.now();
    state.isAvailable = true;
  }

  /**
   * Record a failed send through a provider.
   */
  recordFailure(provider: AggregatorProvider): void {
    const state = this.healthStates.get(provider);
    if (!state) return;

    const now = Date.now();
    state.failureTimestamps.push(now);
    state.requestCount++;
    state.lastCheckedAt = now;
  }

  /**
   * Check if a provider is healthy based on the rolling error window.
   */
  private isProviderHealthy(provider: AggregatorProvider): boolean {
    const state = this.healthStates.get(provider);
    if (!state) return true; // Unknown → assume healthy

    const now = Date.now();
    const cutoff = now - AGGREGATORS.ROUTE.HEALTH_WINDOW_MS;

    // Prune old failures
    state.failureTimestamps = state.failureTimestamps.filter(t => t > cutoff);

    // Not enough data → assume healthy
    if (state.requestCount < AGGREGATORS.ROUTE.MIN_REQUEST_COUNT) {
      return true;
    }

    const errorRate = state.failureTimestamps.length / Math.max(state.requestCount, 1);
    const healthy = errorRate <= AGGREGATORS.ROUTE.ERROR_THRESHOLD;

    state.isAvailable = healthy;
    return healthy;
  }

  // ─── Health Snapshot ────────────────────────────────────────────────────

  /**
   * Get health snapshot for all registered providers.
   */
  getHealthSnapshot(): AggregatorHealth[] {
    const snapshot: AggregatorHealth[] = [];

    for (const [provider, state] of this.healthStates) {
      const now = Date.now();
      const cutoff = now - AGGREGATORS.ROUTE.HEALTH_WINDOW_MS;
      const recentFailures = state.failureTimestamps.filter(t => t > cutoff).length;
      const successRate = state.requestCount > 0
        ? 1 - (recentFailures / Math.max(state.requestCount, 1))
        : 1;

      snapshot.push({
        provider,
        successRate: Math.max(0, successRate),
        totalSent: state.requestCount,
        totalFailed: recentFailures,
        avgResponseTimeMs: state.requestCount > 0
          ? Math.round(state.totalResponseTimeMs / state.requestCount)
          : 0,
        lastCheckedAt: state.lastCheckedAt,
        isAvailable: state.isAvailable,
      });
    }

    return snapshot;
  }

  // ─── Redis Admin Override ───────────────────────────────────────────────

  /**
   * Set an admin override for a specific intent.
   * Stores in Redis: aggregator:route:intent:{intent} = provider
   */
  async setRouteOverride(intent: MessageIntent, provider: AggregatorProvider): Promise<void> {
    if (!this.redisClient) {
      log.warn('Cannot set route override — no Redis client');
      return;
    }

    try {
      await this.redisClient.set(
        REDIS_ROUTE_OVERRIDE(intent),
        provider,
        'EX',
        86400 // 24 hour expiry
      );
      log.info(`Route override set: ${intent} → ${provider}`);
    } catch (err) {
      log.error(`Failed to set route override in Redis: ${(err as Error).message}`);
    }
  }

  /**
   * Clear an admin override for a specific intent.
   */
  async clearRouteOverride(intent: MessageIntent): Promise<void> {
    if (!this.redisClient) return;

    try {
      await this.redisClient.del(REDIS_ROUTE_OVERRIDE(intent));
      log.info(`Route override cleared for ${intent}`);
    } catch (err) {
      log.error(`Failed to clear route override: ${(err as Error).message}`);
    }
  }

  /**
   * Get the current admin override for an intent, if any.
   */
  private async getRouteOverride(intent: MessageIntent): Promise<AggregatorProvider | null> {
    if (!this.redisClient) return null;

    try {
      const override = await this.redisClient.get(REDIS_ROUTE_OVERRIDE(intent));
      if (override && (override === 'termii' || override === 'bulksms')) {
        return override as AggregatorProvider;
      }
      return null;
    } catch {
      return null;
    }
  }

  /**
   * Get the current route override from Redis (public version for admin API).
   */
  async getRouteOverridePublic(intent: MessageIntent): Promise<AggregatorProvider | null> {
    return this.getRouteOverride(intent);
  }
}