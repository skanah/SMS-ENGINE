/**
 * slidingWindowProcessor.ts — 128-Window Async SMPP Submit Engine
 *
 * Implements a credit-based sliding window protocol for SMPP submit_sm
 * pipelining. Instead of awaiting each submit_sm_resp before sending the
 * next PDU, this processor aggressively fires submit_sm packets into
 * the socket up to the maximum window limit (configurable, default 128).
 *
 * Key behaviors:
 *   - Credit counter: incremented on submit_sm, decremented on submit_sm_resp
 *   - Backpressure: when window is full, messages queue in an internal buffer
 *   - Timeout recovery: lost-in-window packets detected via periodic sweep
 *   - Session load balancing: distributes across healthiest bindings
 *   - Batch processing: groups up to 200 messages per tick for efficiency
 *
 * Architecture:
 *   - Each worker thread owns one SlidingWindowProcessor instance
 *   - The processor pulls messages from a Kafka consumer (or in-memory queue)
 *   - It selects the optimal SMPP session per operator using SmppSessionManager
 *   - submit_sm PDUs are fire-and-forget; credit tracking is async
 */

import { EventEmitter } from 'events';
import { createLogger } from './logger';
import { SmppSessionManager } from './smppSessionManager';
import { TelcoOperator } from './types';
import { SLIDING_WINDOW } from './config';

const log = createLogger('SlidingWindowProcessor');

// ─── Constants ──────────────────────────────────────────────────────────────

const DEFAULT_WINDOW_SIZE = SLIDING_WINDOW.MAX_WINDOW_SIZE;
const RESPONSE_TIMEOUT_MS = SLIDING_WINDOW.RESPONSE_TIMEOUT_MS;
const SWEEP_INTERVAL_MS = SLIDING_WINDOW.WINDOW_SWEEP_INTERVAL_MS;
const BATCH_SIZE = SLIDING_WINDOW.BATCH_SIZE_PER_TICK;

// ─── Types ──────────────────────────────────────────────────────────────────

export interface QueuedMessage {
  id: string;
  sourceAddr: string;
  destinationAddr: string;
  shortMessage: Buffer;
  dataCoding: number;
  esmClass: number;
  priority: 'otp' | 'bulk' | 'transactional';
  operator: TelcoOperator;
  userId?: string;
  campaignId?: string;
  scheduledAt?: number;
  registeredDelivery?: number;
  validityPeriod?: string;
}

interface PendingSubmit {
  messageId: string;
  sequenceNumber: number;
  sessionId: string;
  submittedAt: number;
  operator: TelcoOperator;
  retryCount: number;
}

interface WindowSlot {
  sessionId: string;
  sequenceNumber: number;
  messageId: string;
  submittedAt: number;
}

// ─── Events ─────────────────────────────────────────────────────────────────

export interface SlidingWindowEvents {
  'message:sent': (messageId: string, sessionId: string, sequenceNumber: number) => void;
  'message:timeout': (messageId: string, sessionId: string, retryCount: number) => void;
  'message:failed': (messageId: string, error: string) => void;
  'window:full': (sessionId: string, queueDepth: number) => void;
  'window:credit_available': (sessionId: string, credits: number) => void;
  'backpressure:warning': (queueDepth: number) => void;
}

// ─── Sliding Window Processor ───────────────────────────────────────────────

export class SlidingWindowProcessor extends EventEmitter {
  private sessionManager: SmppSessionManager;

  /** Per-session credit counter — max outstanding PDUs */
  private sessionCredits: Map<string, number> = new Map();

  /** Per-session pending PDUs awaiting response */
  private pendingSubmit: Map<string, WindowSlot[]> = new Map();

  /** In-memory message queue (when window is full or no session available) */
  private messageQueue: QueuedMessage[] = [];

  /** Maximum queue depth before we start dropping */
  private readonly maxQueueDepth: number;

  /** Sweep interval timer reference */
  private sweepTimer: NodeJS.Timeout | null = null;

  /** Processing flag to prevent concurrent ticks */
  private isProcessing: boolean = false;

  /** Total messages processed through this instance */
  private totalProcessed: number = 0;

  /** Total messages that timed out */
  private totalTimeouts: number = 0;

  constructor(
    sessionManager: SmppSessionManager,
    options?: { maxQueueDepth?: number }
  ) {
    super();
    this.sessionManager = sessionManager;
    this.maxQueueDepth = options?.maxQueueDepth ?? 100_000;
    this.setMaxListeners(200);

    // Initialize credits for all sessions
    this.initializeCredits();

    // Listen for submit_sm_resp to return credits
    this.sessionManager.on('submit_sm_resp', (sessionId: string, pdu: any) => {
      this.returnCredit(sessionId, pdu);
    });

    // Listen for session bound events to initialize credits
    this.sessionManager.on('session:bound', (sessionId: string) => {
      this.sessionCredits.set(sessionId, DEFAULT_WINDOW_SIZE);
      log.info(`Session ${sessionId} bound, credits initialized to ${DEFAULT_WINDOW_SIZE}`);
    });

    // Start the periodic sweep for lost/stalled packets
    this.startSweepTimer();
  }

  // ─── Initialization ───────────────────────────────────────────────────────

  private initializeCredits(): void {
    const metrics = this.sessionManager.getAllSessionMetrics();
    for (const m of metrics) {
      this.sessionCredits.set(m.sessionId, DEFAULT_WINDOW_SIZE);
      this.pendingSubmit.set(m.sessionId, []);
    }
    log.info(`Initialized credits for ${metrics.length} sessions`);
  }

  // ─── Core Processing ──────────────────────────────────────────────────────

  /**
   * Enqueue a batch of messages for processing.
   * This is the main entry point called by the Kafka consumer or API server.
   *
   * @param messages - Array of prepared SMPP messages ready for submission
   */
  async enqueueBatch(messages: QueuedMessage[]): Promise<void> {
    if (messages.length === 0) return;

    // Filter messages and add to queue
    for (const msg of messages) {
      if (this.messageQueue.length >= this.maxQueueDepth) {
        log.warn(`Message queue full (${this.maxQueueDepth}), dropping message ${msg.id}`);
        this.emit('message:failed', msg.id, 'Queue full');
        continue;
      }
      this.messageQueue.push(msg);
    }

    // Trigger processing if not already in progress
    if (!this.isProcessing) {
      this.processNextBatch();
    }
  }

  /**
   * Process the next batch of messages from the queue.
   * This runs in a non-blocking loop until the queue is empty or
   * all session windows are full.
   */
  private async processNextBatch(): Promise<void> {
    if (this.isProcessing) return;
    this.isProcessing = true;

    try {
      let batchCount = 0;

      while (this.messageQueue.length > 0 && batchCount < BATCH_SIZE) {
        // Check if we have any available credit across all sessions
        if (!this.hasAvailableCredits()) {
          this.emit('backpressure:warning', this.messageQueue.length);
          break; // Wait for credits to return via submit_sm_resp
        }

        const message = this.messageQueue.shift();
        if (!message) break;

        const sent = this.sendMessage(message);
        if (sent) {
          batchCount++;
          this.totalProcessed++;
        } else {
          // Push back to front of queue if we couldn't send
          this.messageQueue.unshift(message);
          break;
        }
      }

      if (batchCount > 0) {
        log.debug(`Processed ${batchCount} messages in batch, queue depth: ${this.messageQueue.length}`);
      }
    } catch (err: any) {
      log.error(`Error processing batch: ${err.message}`);
    } finally {
      this.isProcessing = false;
    }

    // If there are still messages, schedule next batch asynchronously
    if (this.messageQueue.length > 0 && this.hasAvailableCredits()) {
      setImmediate(() => this.processNextBatch());
    }
  }

  /**
   * Send a single message through the optimal SMPP session.
   *
   * @param message - The prepared message to send
   * @returns true if the message was sent, false if no session available
   */
  private sendMessage(message: QueuedMessage): boolean {
    // Select the best session for this operator
    const sessionId = this.sessionManager.selectSession(message.operator);

    if (!sessionId) {
      log.warn(`No available session for operator ${message.operator}, message ${message.id} queued`);
      return false;
    }

    // Check if this session has credit available
    const credits = this.sessionCredits.get(sessionId) ?? 0;
    if (credits <= 0) {
      // No credit — message stays in queue
      return false;
    }

    // Decrement credit
    this.sessionCredits.set(sessionId, credits - 1);

    // Build submit_sm parameters
    const params: any = {
      source_addr: message.sourceAddr,
      destination_addr: message.destinationAddr,
      short_message: message.shortMessage,
      data_coding: message.dataCoding,
      esm_class: message.esmClass,
      registered_delivery: message.registeredDelivery ?? 1,
      priority_flag: message.priority === 'otp' ? 1 : message.priority === 'transactional' ? 0 : 0,
      validity_period: message.validityPeriod,
    };

    // Send via session manager (fire-and-forget)
    const sequenceNumber = this.sessionManager.sendSubmitSm(sessionId, params);

    if (sequenceNumber === -1) {
      // Send failed — return credit
      this.sessionCredits.set(sessionId, credits);
      return false;
    }

    // Track pending PDU
    const pending = this.pendingSubmit.get(sessionId) ?? [];
    pending.push({
      sessionId,
      sequenceNumber,
      messageId: message.id,
      submittedAt: Date.now(),
    });
    this.pendingSubmit.set(sessionId, pending);

    this.emit('message:sent', message.id, sessionId, sequenceNumber);

    return true;
  }

  // ─── Credit Management ────────────────────────────────────────────────────

  /**
   * Return credit when a submit_sm_resp is received.
   * Also removes the pending PDU from the tracking list.
   */
  private returnCredit(sessionId: string, pdu: any): void {
    const currentCredits = this.sessionCredits.get(sessionId) ?? 0;
    const newCredits = Math.min(currentCredits + 1, DEFAULT_WINDOW_SIZE);
    this.sessionCredits.set(sessionId, newCredits);

    // Remove from pending list
    const pending = this.pendingSubmit.get(sessionId) ?? [];
    const sequenceNumber = pdu.sequence_number;
    const updatedPending = pending.filter(p => p.sequenceNumber !== sequenceNumber);
    this.pendingSubmit.set(sessionId, updatedPending);

    // Log if we recovered a timed-out packet
    if (updatedPending.length < pending.length) {
      log.debug(`Credit returned for session ${sessionId}, seq=${sequenceNumber}, credits=${newCredits}`);
    }

    // If we now have credit, trigger processing
    if (this.messageQueue.length > 0 && !this.isProcessing) {
      setImmediate(() => this.processNextBatch());
    }

    this.emit('window:credit_available', sessionId, newCredits);
  }

  /**
   * Check if any session has available credits.
   */
  private hasAvailableCredits(): boolean {
    for (const credits of this.sessionCredits.values()) {
      if (credits > 0) return true;
    }
    return false;
  }

  /**
   * Get the total number of available credits across all sessions.
   */
  getTotalAvailableCredits(): number {
    let total = 0;
    for (const credits of this.sessionCredits.values()) {
      total += credits;
    }
    return total;
  }

  // ─── Sweep Timer: Detect Lost Packets ─────────────────────────────────────

  private startSweepTimer(): void {
    this.sweepTimer = setInterval(() => {
      this.sweepPendingPackets();
    }, SWEEP_INTERVAL_MS);
  }

  /**
   * Periodic sweep that checks for stalled/lost PDUs.
   * If a PDU has been pending longer than RESPONSE_TIMEOUT_MS,
   * it is considered lost and we return its credit.
   */
  private sweepPendingPackets(): void {
    const now = Date.now();
    let recoveredCount = 0;

    for (const [sessionId, pendingList] of this.pendingSubmit.entries()) {
      const stillPending: WindowSlot[] = [];

      for (const slot of pendingList) {
        const elapsed = now - slot.submittedAt;

        if (elapsed > RESPONSE_TIMEOUT_MS) {
          // Packet lost — return credit and emit timeout
          const currentCredits = this.sessionCredits.get(sessionId) ?? 0;
          this.sessionCredits.set(sessionId, Math.min(currentCredits + 1, DEFAULT_WINDOW_SIZE));

          this.totalTimeouts++;
          recoveredCount++;

          this.emit('message:timeout', slot.messageId, sessionId, 0);

          log.warn(
            `Packet timeout for message ${slot.messageId} on session ${sessionId}, ` +
            `elapsed=${elapsed}ms, credits returned`
          );
        } else {
          stillPending.push(slot);
        }
      }

      this.pendingSubmit.set(sessionId, stillPending);
    }

    // If we recovered credits and there are queued messages, trigger processing
    if (recoveredCount > 0 && this.messageQueue.length > 0 && !this.isProcessing) {
      setImmediate(() => this.processNextBatch());
    }
  }

  // ─── Queue Management ─────────────────────────────────────────────────────

  /**
   * Get the current depth of the internal message queue.
   */
  getQueueDepth(): number {
    return this.messageQueue.length;
  }

  /**
   * Get the count of pending (unresponded) PDUs per session.
   */
  getPendingCount(sessionId?: string): number {
    if (sessionId) {
      return this.pendingSubmit.get(sessionId)?.length ?? 0;
    }
    let total = 0;
    for (const list of this.pendingSubmit.values()) {
      total += list.length;
    }
    return total;
  }

  /**
   * Get processing statistics.
   */
  getStats(): {
    totalProcessed: number;
    totalTimeouts: number;
    queueDepth: number;
    totalPending: number;
    availableCredits: number;
  } {
    return {
      totalProcessed: this.totalProcessed,
      totalTimeouts: this.totalTimeouts,
      queueDepth: this.messageQueue.length,
      totalPending: this.getPendingCount(),
      availableCredits: this.getTotalAvailableCredits(),
    };
  }

  // ─── Priority Message Injection ──────────────────────────────────────────

  /**
   * Inject a high-priority message (OTP) at the front of the queue.
   * This ensures OTP messages bypass bulk marketing traffic.
   *
   * @param message - The high-priority message to inject
   */
  injectPriorityMessage(message: QueuedMessage): void {
    this.messageQueue.unshift(message);

    // Immediately try to process
    if (!this.isProcessing) {
      setImmediate(() => this.processNextBatch());
    }
  }

  // ─── Graceful Shutdown ────────────────────────────────────────────────────

  /**
   * Shutdown the processor gracefully.
   * Waits for pending PDUs to complete or timeout.
   */
  async shutdown(timeoutMs: number = 10_000): Promise<void> {
    log.info('Shutting down sliding window processor...');

    // Stop the sweep timer
    if (this.sweepTimer) {
      clearInterval(this.sweepTimer);
      this.sweepTimer = null;
    }

    // Wait for pending PDUs to complete or timeout
    const startTime = Date.now();
    while (this.getPendingCount() > 0) {
      if (Date.now() - startTime > timeoutMs) {
        log.warn(`Shutdown timeout reached with ${this.getPendingCount()} pending PDUs`);
        break;
      }
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    // Drain the message queue
    const remaining = this.messageQueue.length;
    if (remaining > 0) {
      log.warn(`Dropping ${remaining} queued messages during shutdown`);
      this.messageQueue = [];
    }

    this.sessionCredits.clear();
    this.pendingSubmit.clear();

    log.info(
      `Sliding window processor shutdown complete. ` +
      `Processed: ${this.totalProcessed}, Timeouts: ${this.totalTimeouts}`
    );
  }
}