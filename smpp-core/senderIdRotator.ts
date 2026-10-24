/**
 * senderIdRotator.ts — Auto-Rotating Sender ID Engine
 *
 * Monitors delivery success rates for active alphanumeric Sender IDs.
 * If a Sender ID's delivery metrics drop below a configurable threshold
 * (default 90%), indicating local network spam-filtering, the engine
 * automatically rotates the source_addr to the next registered Sender ID
 * in the user's rotation pool.
 *
 * This is critical for Nigerian bulk messaging where telcos aggressively
 * filter Sender IDs that trigger spam complaints. Auto-rotation keeps
 * campaigns running without manual intervention.
 *
 * Architecture:
 *   - Per-Sender-ID success rate tracking over a 5-minute rolling window
 *   - Redis-backed counters for distributed state across worker threads
 *   - Configurable rotation pool per user/campaign
 *   - Admin override to lock a specific Sender ID
 */

import { EventEmitter } from 'events';
import { SENDER_ID_ROTATION } from './config';
import { createLogger } from './logger';

const log = createLogger('SenderIdRotator');

// ─── Types ──────────────────────────────────────────────────────────────────

export interface SenderIdState {
  userId: string;
  activeSenderId: string;
  rotationPool: string[];
  /** Delivery success rate in the current window (0.0 to 1.0) */
  successRate: number;
  /** Total messages sent in the current window */
  totalSent: number;
  /** Total messages delivered in the current window */
  totalDelivered: number;
  /** When the current evaluation window started */
  windowStart: number;
  /** Whether this Sender ID is locked by admin */
  locked: boolean;
  /** Timestamp of last rotation */
  lastRotatedAt: number | null;
}

interface DeliverySample {
  senderId: string;
  delivered: boolean;
  timestamp: number;
}

// ─── Sender ID Rotator ──────────────────────────────────────────────────────

export class SenderIdRotator extends EventEmitter {
  /** Per-user Sender ID state */
  private userStates: Map<string, SenderIdState> = new Map();

  /** Rolling delivery samples for rate calculation */
  private deliverySamples: Map<string, DeliverySample[]> = new Map();

  /** Sweep interval for evaluating rates */
  private sweepTimer: NodeJS.Timeout | null = null;

  constructor() {
    super();
    this.setMaxListeners(100);
    this.startSweep();
    log.info('Sender ID Rotator initialized');
  }

  // ─── User Registration ───────────────────────────────────────────────────

  /**
   * Register a user's Sender ID configuration.
   *
   * @param userId - The user's ID
   * @param initialSenderId - The starting Sender ID
   * @param rotationPool - Array of Sender IDs to rotate through
   */
  registerUser(
    userId: string,
    initialSenderId: string,
    rotationPool: string[]
  ): void {
    if (rotationPool.length === 0) {
      rotationPool = [initialSenderId];
    }

    this.userStates.set(userId, {
      userId,
      activeSenderId: initialSenderId,
      rotationPool,
      successRate: 1.0,
      totalSent: 0,
      totalDelivered: 0,
      windowStart: Date.now(),
      locked: false,
      lastRotatedAt: null,
    });

    this.deliverySamples.set(userId, []);

    log.info(
      `User ${userId} registered: active=${initialSenderId}, pool=[${rotationPool.join(', ')}]`
    );
  }

  /**
   * Update a user's rotation pool (admin operation).
   */
  updateRotationPool(userId: string, pool: string[]): void {
    const state = this.userStates.get(userId);
    if (!state) {
      log.warn(`Cannot update pool for unknown user ${userId}`);
      return;
    }

    state.rotationPool = pool;

    // If the active Sender ID is no longer in the pool, rotate
    if (!pool.includes(state.activeSenderId) && pool.length > 0) {
      state.activeSenderId = pool[0];
      state.lastRotatedAt = Date.now();
      log.info(`User ${userId} active Sender ID changed to ${state.activeSenderId} (pool updated)`);
    }

    log.info(`User ${userId} rotation pool updated: [${pool.join(', ')}]`);
  }

  // ─── Delivery Tracking ───────────────────────────────────────────────────

  /**
   * Record a delivery attempt for a Sender ID.
   * Call this when a DLR is received.
   *
   * @param userId - The user who sent the message
   * @param senderId - The Sender ID used
   * @param delivered - Whether the message was delivered successfully
   */
  recordDelivery(userId: string, senderId: string, delivered: boolean): void {
    const state = this.userStates.get(userId);
    if (!state) return;

    state.totalSent++;
    if (delivered) state.totalDelivered++;

    // Add to rolling samples
    const samples = this.deliverySamples.get(userId) ?? [];
    samples.push({
      senderId,
      delivered,
      timestamp: Date.now(),
    });
    this.deliverySamples.set(userId, samples);
  }

  // ─── Rate Calculation ────────────────────────────────────────────────────

  /**
   * Calculate the current delivery success rate for a user's active Sender ID.
   * Uses a rolling window of the last N minutes.
   */
  private calculateSuccessRate(userId: string): number {
    const state = this.userStates.get(userId);
    if (!state) return 1.0;

    const samples = this.deliverySamples.get(userId) ?? [];
    const cutoff = Date.now() - SENDER_ID_ROTATION.EVALUATION_WINDOW_MS;

    // Filter samples within the window for the active Sender ID
    const relevantSamples = samples.filter(
      s => s.timestamp > cutoff && s.senderId === state.activeSenderId
    );

    if (relevantSamples.length < SENDER_ID_ROTATION.MIN_MESSAGES_IN_WINDOW) {
      return 1.0; // Not enough data to evaluate
    }

    const delivered = relevantSamples.filter(s => s.delivered).length;
    return delivered / relevantSamples.length;
  }

  // ─── Rotation Logic ──────────────────────────────────────────────────────

  /**
   * Get the current active Sender ID for a user.
   */
  getActiveSenderId(userId: string): string | null {
    return this.userStates.get(userId)?.activeSenderId ?? null;
  }

  /**
   * Force-rotate to the next Sender ID in the pool.
   * Returns the new active Sender ID, or null if rotation is not possible.
   */
  rotateSenderId(userId: string): string | null {
    const state = this.userStates.get(userId);
    if (!state) return null;

    if (state.locked) {
      log.warn(`Cannot rotate Sender ID for user ${userId} — locked by admin`);
      return state.activeSenderId;
    }

    if (state.rotationPool.length <= 1) {
      log.warn(`Cannot rotate Sender ID for user ${userId} — only one in pool`);
      return state.activeSenderId;
    }

    // Find the next Sender ID in the pool
    const currentIndex = state.rotationPool.indexOf(state.activeSenderId);
    const nextIndex = (currentIndex + 1) % state.rotationPool.length;
    const newSenderId = state.rotationPool[nextIndex];

    state.activeSenderId = newSenderId;
    state.lastRotatedAt = Date.now();
    state.totalSent = 0;
    state.totalDelivered = 0;
    state.windowStart = Date.now();

    log.info(
      `User ${userId} Sender ID rotated: ${state.rotationPool[currentIndex]} → ${newSenderId}`
    );

    this.emit('senderid:rotated', userId, newSenderId, state.rotationPool[currentIndex]);

    return newSenderId;
  }

  // ─── Admin Controls ──────────────────────────────────────────────────────

  /**
   * Lock a user's Sender ID to prevent auto-rotation.
   */
  lockSenderId(userId: string): void {
    const state = this.userStates.get(userId);
    if (state) {
      state.locked = true;
      log.info(`User ${userId} Sender ID locked (${state.activeSenderId})`);
    }
  }

  /**
   * Unlock a user's Sender ID to allow auto-rotation.
   */
  unlockSenderId(userId: string): void {
    const state = this.userStates.get(userId);
    if (state) {
      state.locked = false;
      log.info(`User ${userId} Sender ID unlocked`);
    }
  }

  /**
   * Manually set a specific Sender ID for a user.
   */
  setSenderId(userId: string, senderId: string): void {
    const state = this.userStates.get(userId);
    if (!state) return;

    if (!state.rotationPool.includes(senderId)) {
      state.rotationPool.push(senderId);
    }

    const previous = state.activeSenderId;
    state.activeSenderId = senderId;
    state.lastRotatedAt = Date.now();

    log.info(`User ${userId} Sender ID manually set: ${previous} → ${senderId}`);
    this.emit('senderid:manually_set', userId, senderId, previous);
  }

  // ─── Periodic Evaluation ─────────────────────────────────────────────────

  private startSweep(): void {
    this.sweepTimer = setInterval(() => {
      this.evaluateAll();
    }, 30_000); // Evaluate every 30 seconds
  }

  /**
   * Evaluate all users' Sender ID success rates and rotate if needed.
   */
  private evaluateAll(): void {
    for (const [userId, state] of this.userStates) {
      if (state.locked) continue;
      if (state.rotationPool.length <= 1) continue;

      const successRate = this.calculateSuccessRate(userId);
      state.successRate = successRate;

      if (successRate < SENDER_ID_ROTATION.SUCCESS_RATE_THRESHOLD) {
        log.warn(
          `User ${userId} Sender ID ${state.activeSenderId} success rate ` +
          `${(successRate * 100).toFixed(1)}% below threshold ` +
          `${(SENDER_ID_ROTATION.SUCCESS_RATE_THRESHOLD * 100).toFixed(0)}% — rotating`
        );
        this.rotateSenderId(userId);
      }
    }
  }

  // ─── Utility Methods ─────────────────────────────────────────────────────

  /**
   * Get the state of all registered users.
   */
  getAllStates(): SenderIdState[] {
    return Array.from(this.userStates.values());
  }

  /**
   * Get a specific user's Sender ID state.
   */
  getUserState(userId: string): SenderIdState | null {
    return this.userStates.get(userId) ?? null;
  }

  // ─── Graceful Shutdown ───────────────────────────────────────────────────

  shutdown(): void {
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }
    this.userStates.clear();
    this.deliverySamples.clear();
    log.info('Sender ID Rotator shutdown complete');
  }
}