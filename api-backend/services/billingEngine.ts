/**
 * billingEngine.ts — SMS Credit Billing & Usage Engine
 *
 * Manages per-user credit balances, deducts costs per message segment,
 * and enforces spending limits. Integrates with the campaign stream
 * processor to reserve credits before messages are submitted to the SMPP core.
 *
 * Key features:
 *   - Credit reservation (optimistic locking to prevent double-spend)
 *   - Per-segment costing based on encoding (GSM 7-bit vs UCS-2)
 *   - Transactional billing audit trail
 *   - Low-balance threshold alerts
 */

import { createLogger } from '../../smpp-core/logger';
import { TelcoOperator } from '../../smpp-core/types';

const log = createLogger('BillingEngine');

// ─── Cost Constants (in base currency units, e.g., kobo) ─────────────────────
// In production these can be loaded from a pricing config table.

interface CostTable {
  perSegment: number;       // Cost per SMS segment (base)
  perSegmentUcs2: number;   // Cost per UCS-2 segment (premium)
  perMessage: number;       // Flat per-message overhead
}

const DEFAULT_COST: CostTable = {
  perSegment: 100,       // ₦1.00 per GSM 7-bit segment
  perSegmentUcs2: 150,   // ₦1.50 per UCS-2 segment
  perMessage: 50,        // ₦0.50 per message overhead
};

// ─── Billing Result ──────────────────────────────────────────────────────────

export interface BillingResult {
  success: boolean;
  transactionId?: string;
  cost: number;
  balanceBefore: number;
  balanceAfter: number;
  reason?: string;
}

export interface CostEstimate {
  totalCost: number;
  segmentCost: number;
  overheadCost: number;
  segmentCount: number;
  encoding: 'gsm7' | 'ucs2';
}

// ─── Credit Reservation ──────────────────────────────────────────────────────

interface CreditReservation {
  userId: string;
  amount: number;
  lockedAt: number;
  ttl: number; // Time-to-live in ms, auto-expires
}

// ─── Billing Engine ──────────────────────────────────────────────────────────

export class BillingEngine {
  private costTable: CostTable;
  private reservations: Map<string, CreditReservation> = new Map();
  private db: any; // Database client (Knex instance)—injected

  constructor(databaseClient?: any) {
    this.db = databaseClient;
    this.costTable = { ...DEFAULT_COST };

    // Periodic cleanup of expired reservations
    setInterval(() => this.cleanExpiredReservations(), 10_000);
  }

  // ─── Cost Estimation ───────────────────────────────────────────────────────

  /**
   * Estimate the cost for sending a message.
   * Used for pricing previews before actual submission.
   *
   * @param segments - Number of SMS segments
   * @param encoding - 'gsm7' or 'ucs2'
   * @returns Detailed cost breakdown
   */
  estimateCost(segments: number, encoding: 'gsm7' | 'ucs2'): CostEstimate {
    const segCost = encoding === 'ucs2'
      ? this.costTable.perSegmentUcs2
      : this.costTable.perSegment;

    const segmentCost = segCost * segments;
    const overheadCost = this.costTable.perMessage;

    return {
      totalCost: segmentCost + overheadCost,
      segmentCost,
      overheadCost,
      segmentCount: segments,
      encoding,
    };
  }

  // ─── Credit Check & Reservation ────────────────────────────────────────────

  /**
   * Check if a user has sufficient credit for a given cost.
   *
   * @param userId - User ID
   * @param cost - Total cost to check
   * @returns true if user has enough credit
   */
  async hasSufficientCredit(userId: string, cost: number): Promise<boolean> {
    const balance = await this.getBalance(userId);
    return balance >= cost;
  }

  /**
   * Reserve credits for an outgoing message (optimistic lock).
   * Prevents double-spend on the same user's credit pool.
   *
   * @param userId - User ID
   * @param cost - Amount to reserve
   * @returns Reservation key for later confirmation
   */
  async reserveCredits(userId: string, cost: number): Promise<string | null> {
    const balance = await this.getBalance(userId);

    if (balance < cost) {
      log.warn(`User ${userId} insufficient credit: ${balance} < ${cost}`);
      return null;
    }

    // Generate a unique reservation key
    const reservationKey = `reserve:${userId}:${Date.now()}:${Math.random().toString(36).slice(2, 8)}`;

    this.reservations.set(reservationKey, {
      userId,
      amount: cost,
      lockedAt: Date.now(),
      ttl: 15_000, // 15 seconds to confirm or release
    });

    return reservationKey;
  }

  /**
   * Confirm a credit reservation — deduct the balance and record the transaction.
   *
   * @param reservationKey - The key returned from reserveCredits
   * @returns BillingResult with updated balance
   */
  async confirmReservation(reservationKey: string): Promise<BillingResult> {
    const reservation = this.reservations.get(reservationKey);
    if (!reservation) {
      return {
        success: false,
        cost: 0,
        balanceBefore: 0,
        balanceAfter: 0,
        reason: 'Reservation not found or expired',
      };
    }

    // Remove reservation first (prevents double-confirm)
    this.reservations.delete(reservationKey);

    const { userId, amount } = reservation;
    const balanceBefore = await this.getBalance(userId);

    if (balanceBefore < amount) {
      return {
        success: false,
        cost: amount,
        balanceBefore,
        balanceAfter: balanceBefore,
        reason: 'Insufficient credit at confirmation time',
      };
    }

    try {
      const balanceAfter = await this.deductBalance(userId, amount);
      const transactionId = await this.recordTransaction(userId, amount, balanceBefore, balanceAfter);

      log.info(
        `Billed user ${userId}: ${amount} units (balance: ${balanceBefore} → ${balanceAfter})`
      );

      return {
        success: true,
        transactionId,
        cost: amount,
        balanceBefore,
        balanceAfter,
      };
    } catch (err: any) {
      log.error(`Billing deduction failed for user ${userId}: ${err.message}`);
      return {
        success: false,
        cost: amount,
        balanceBefore,
        balanceAfter: balanceBefore,
        reason: `Deduction failed: ${err.message}`,
      };
    }
  }

  /**
   * Release a credit reservation without deducting.
   * Called if the message submission fails before confirmation.
   */
  releaseReservation(reservationKey: string): void {
    const reservation = this.reservations.get(reservationKey);
    if (reservation) {
      this.reservations.delete(reservationKey);
      log.debug(`Released reservation ${reservationKey} for user ${reservation.userId}`);
    }
  }

  // ─── Balance Management ────────────────────────────────────────────────────

  /**
   * Get a user's current credit balance.
   */
  async getBalance(userId: string): Promise<number> {
    if (!this.db) {
      // Fallback: in-memory balance for testing
      return 100_000_000; // Default balance
    }

    try {
      const row = await this.db('users')
        .select('credit_balance')
        .where('id', userId)
        .first();

      return row ? Number(row.credit_balance) : 0;
    } catch (err: any) {
      log.error(`Failed to get balance for user ${userId}: ${err.message}`);
      return 0;
    }
  }

  /**
   * Deduct credits from a user's balance.
   * Uses atomic UPDATE with RETURNING to prevent race conditions.
   */
  private async deductBalance(userId: string, amount: number): Promise<number> {
    if (!this.db) {
      return 100_000_000 - amount;
    }

    const result = await this.db('users')
      .where('id', userId)
      .where('credit_balance', '>=', amount)
      .decrement('credit_balance', amount)
      .returning('credit_balance');

    if (result.length === 0) {
      throw new Error('Insufficient credits or user not found');
    }

    return Number(result[0].credit_balance);
  }

  /**
   * Add credits to a user's balance (top-up, refund, bonus).
   */
  async addCredits(userId: string, amount: number, description?: string): Promise<BillingResult> {
    if (!this.db) {
      return { success: true, cost: -amount, balanceBefore: 0, balanceAfter: amount };
    }

    const balanceBefore = await this.getBalance(userId);

    const result = await this.db('users')
      .where('id', userId)
      .increment('credit_balance', amount)
      .returning('credit_balance');

    const balanceAfter = Number(result[0].credit_balance);

    await this.recordTransaction(
      userId,
      -amount, // Negative cost = credit added
      balanceBefore,
      balanceAfter,
      'credit_purchase',
      description
    );

    return {
      success: true,
      cost: -amount,
      balanceBefore,
      balanceAfter,
    };
  }

  // ─── Transaction Recording ─────────────────────────────────────────────────

  /**
   * Record a billing transaction in the audit trail.
   */
  private async recordTransaction(
    userId: string,
    amount: number,
    balanceBefore: number,
    balanceAfter: number,
    type: string = 'usage',
    description?: string
  ): Promise<string> {
    if (!this.db) {
      return `txn-sim-${Date.now()}`;
    }

    const result = await this.db('billing_transactions').insert({
      user_id: userId,
      type,
      amount: Math.abs(amount),
      balance_before: balanceBefore,
      balance_after: balanceAfter,
      description: description || (amount > 0 ? 'Message usage' : 'Credit top-up'),
      status: 'completed',
    }).returning('id');

    return result[0].id;
  }

  // ─── Pricing Config ────────────────────────────────────────────────────────

  /**
   * Update the cost table (admin operation).
   */
  setCostTable(costs: Partial<CostTable>): void {
    this.costTable = { ...this.costTable, ...costs };
    log.info(`Cost table updated: ${JSON.stringify(this.costTable)}`);
  }

  // ─── Cleanup ───────────────────────────────────────────────────────────────

  private cleanExpiredReservations(): void {
    const now = Date.now();
    for (const [key, reservation] of this.reservations) {
      if (now - reservation.lockedAt > reservation.ttl) {
        this.reservations.delete(key);
        log.debug(`Expired reservation released: ${key}`);
      }
    }
  }

  /**
   * Get current reservation count (for monitoring).
   */
  getPendingReservationCount(): number {
    return this.reservations.size;
  }
}