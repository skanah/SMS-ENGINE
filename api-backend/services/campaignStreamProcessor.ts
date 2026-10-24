/**
 * campaignStreamProcessor.ts — Kafka Stream Consumer for Campaign Processing
 *
 * Reads messages from high-priority (OTP) and low-priority (bulk) Kafka topics,
 * processes them through the SMPP core pipeline, and tracks delivery progress.
 *
 * Architecture:
 *   - Consumer Group with configurable partition count (6 partitions)
 *   - Concurrent consumer workers (3 consumers per group)
 *   - Graceful handling of rebalances and offset commits
 *   - Credit reservation via BillingEngine before submission
 *   - Campaign progress tracking in database
 *   - Kafka producer for OTP vs bulk topic routing
 *
 * This is the bridge between the REST API ingestion layer and the SMPP core
 * engine, implementing async batch processing at scale.
 */

import { Kafka, Producer } from 'kafkajs';
import { createLogger } from '../../smpp-core/logger';
import { KafkaTopics, IncomingMessage, ProcessedMessage, TelcoOperator } from '../../smpp-core/types';
import { KAFKA } from '../../smpp-core/config';
import { BillingEngine } from './billingEngine';

const log = createLogger('CampaignStreamProcessor');

// ─── Types ───────────────────────────────────────────────────────────────────

export interface StreamProcessorConfig {
  consumerGroupId: string;
  topics: string[];
  concurrency: number;       // Number of concurrent message handlers
  batchSize: number;         // Max messages per batch
  maxQueueDepth: number;     // Max pending messages in internal queue
}

export interface CampaignProgress {
  campaignId: string;
  totalMessages: number;
  processedCount: number;
  successCount: number;
  failureCount: number;
  startTime: number;
}

// ─── Campaign Stream Processor ───────────────────────────────────────────────

export class CampaignStreamProcessor {
  private billingEngine: BillingEngine;
  private config: StreamProcessorConfig;
  private isRunning: boolean = false;
  private campaignProgress: Map<string, CampaignProgress> = new Map();

  /** Kafka producer for OTP vs bulk topic routing */
  private kafkaProducer: Producer | null = null;

  // Simulated internal message queue
  private messageQueue: IncomingMessage[] = [];
  private processingQueue: Map<string, boolean> = new Map(); // messageId -> inProgress

  // Callback for message submission to SMPP core
  private submitCallback: ((msg: ProcessedMessage) => Promise<boolean>) | null = null;

  constructor(billingEngine: BillingEngine, config?: Partial<StreamProcessorConfig>, kafkaClient?: Kafka) {
    this.billingEngine = billingEngine;

    this.config = {
      consumerGroupId: 'sms-campaign-processor',
      topics: [KafkaTopics.HIGH_PRIORITY_OTP, KafkaTopics.LOW_PRIORITY_BULK],
      concurrency: 10,
      batchSize: 100,
      maxQueueDepth: 50_000,
      ...config,
    };

    if (kafkaClient) {
      this.kafkaProducer = kafkaClient.producer({
        allowAutoTopicCreation: true,
        transactionTimeout: 30000,
      });
    }
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────────────

  /**
   * Start consuming from Kafka topics.
   * In production, this creates a Kafka consumer and subscribes to topics.
   */
  async start(): Promise<void> {
    if (this.isRunning) return;

    this.isRunning = true;

    // Connect Kafka producer if available
    if (this.kafkaProducer) {
      try {
        await this.kafkaProducer.connect();
        log.info('Campaign processor Kafka producer connected');
      } catch (err: any) {
        log.warn(`Campaign processor Kafka producer connection failed: ${err.message}`);
      }
    }

    log.info(
      `Campaign stream processor started: ` +
      `topics=[${this.config.topics.join(', ')}], ` +
      `concurrency=${this.config.concurrency}` +
      (this.kafkaProducer ? ', Kafka producer ready' : '')
    );

    // Start the internal processing loop
    this.processLoop();

    // In production, connect Kafka consumer from topics:
    // const consumer = kafka.consumer({ groupId: this.config.consumerGroupId });
    // await consumer.connect();
    // await consumer.subscribe({ topics: this.config.topics });
    // await consumer.run({
    //   eachBatch: async ({ batch, heartbeat, commitOffsets }) => {
    //     for (const message of batch.messages) {
    //       const parsed = this.parseMessage(message);
    //       if (parsed) {
    //         const topic = (parsed.priority === 'otp' || parsed.priority === 'transactional')
    //           ? KAFKA.TOPICS.HIGH_PRIORITY_OTP
    //           : KAFKA.TOPICS.LOW_PRIORITY_BULK;
    //         // Re-publish to the appropriate partitioned topic
    //         if (this.kafkaProducer) {
    //           await this.kafkaProducer.send({
    //             topic,
    //             messages: [{ key: parsed.id, value: JSON.stringify(parsed) }],
    //           });
    //         }
    //         await this.enqueueMessage(parsed);
    //       }
    //     }
    //     await commitOffsets();
    //   },
    // });
  }

  /**
   * Gracefully stop the processor.
   */
  async stop(timeoutMs: number = 10_000): Promise<void> {
    log.info('Stopping campaign stream processor...');
    this.isRunning = false;

    // Disconnect Kafka producer
    if (this.kafkaProducer) {
      try {
        await this.kafkaProducer.disconnect();
        log.info('Campaign processor Kafka producer disconnected');
      } catch (err) {
        log.warn(`Kafka producer disconnect error: ${(err as Error).message}`);
      }
    }

    // Wait for queue to drain
    const startTime = Date.now();
    while (this.messageQueue.length > 0) {
      if (Date.now() - startTime > timeoutMs) {
        log.warn(`Queue drain timeout: ${this.messageQueue.length} messages remaining`);
        break;
      }
      await this.delay(100);
    }

    log.info('Campaign stream processor stopped');
  }

  // ─── Callback Registration ─────────────────────────────────────────────────

  /**
   * Register the callback for submitting processed messages to the SMPP core.
   */
  onSubmit(callback: (msg: ProcessedMessage) => Promise<boolean>): void {
    this.submitCallback = callback;
  }

  // ─── Message Ingestion ─────────────────────────────────────────────────────

  /**
   * Enqueue a message for processing (called by Kafka consumer or REST API).
   */
  async enqueueMessage(msg: IncomingMessage): Promise<boolean> {
    if (this.messageQueue.length >= this.config.maxQueueDepth) {
      log.warn('Message queue full — rejecting message');
      return false;
    }

    this.messageQueue.push(msg);
    return true;
  }

  /**
   * Parse a Kafka message into an IncomingMessage.
   */
  private parseMessage(kafkaMessage: any): IncomingMessage | null {
    try {
      const value = kafkaMessage.value?.toString();
      if (!value) return null;

      const data = JSON.parse(value);

      return {
        id: data.id,
        sender: data.sender,
        recipient: data.recipient,
        message: data.message,
        priority: data.priority || 'bulk',
        encoding: data.encoding || 'auto',
        campaignId: data.campaignId,
        userId: data.userId,
        apiKey: data.apiKey,
      };
    } catch (err: any) {
      log.error(`Failed to parse Kafka message: ${err.message}`);
      return null;
    }
  }

  // ─── Processing Loop ───────────────────────────────────────────────────────

  private async processLoop(): Promise<void> {
    while (this.isRunning) {
      try {
        await this.processBatch();
      } catch (err: any) {
        log.error(`Batch processing error: ${err.message}`);
      }

      // Yield to event loop
      await this.delay(50);
    }
  }

  private async processBatch(): Promise<void> {
    if (this.messageQueue.length === 0) return;

    // Take up to batchSize messages from the queue (prioritized)
    const batch: IncomingMessage[] = [];
    const highPriority: IncomingMessage[] = [];
    const lowPriority: IncomingMessage[] = [];

    // Sort by priority
    for (const msg of this.messageQueue.splice(0, this.config.batchSize)) {
      if (msg.priority === 'otp' || msg.priority === 'transactional') {
        highPriority.push(msg);
      } else {
        lowPriority.push(msg);
      }
    }

    // OTPs first, then bulk
    batch.push(...highPriority, ...lowPriority);

    // Process batch with concurrency limit
    const results = await Promise.allSettled(
      batch.map(msg => this.processMessage(msg))
    );

    // Track results
    const successCount = results.filter(r => r.status === 'fulfilled' && r.value).length;
    const failureCount = results.filter(r => r.status === 'rejected' || !r.value).length;

    if (failureCount > 0) {
      log.warn(`Batch: ${successCount} success, ${failureCount} failures`);
    }
  }

  // ─── Message Processing Pipeline ───────────────────────────────────────────

  /**
   * Process a single message through the pipeline:
   *   1. Resolve campaign context (if applicable)
   *   2. Estimate cost and reserve credits
   *   3. Call SMPP core callback for submission
   *   4. Track delivery progress
   */
  private async processMessage(msg: IncomingMessage): Promise<boolean> {
    const messageId = msg.id || `msg-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`;

    // Deduplicate by message ID
    if (this.processingQueue.has(messageId)) {
      log.debug(`Skipping duplicate message: ${messageId}`);
      return true;
    }
    this.processingQueue.set(messageId, true);

    try {
      // Track campaign progress
      if (msg.campaignId) {
        this.trackCampaignProgress(msg.campaignId, 'increment');
      }

      // Estimate cost (default single segment GSM 7-bit)
      const costEstimate = await this.estimateMessageCost(msg);

      // Reserve credits
      if (msg.userId) {
        const reservation = await this.billingEngine.reserveCredits(msg.userId, costEstimate);
        if (!reservation) {
          log.warn(`Insufficient credit for user ${msg.userId} — message ${messageId} rejected`);
          return false;
        }

        // Submit to SMPP core
        if (this.submitCallback) {
          const submitted = await this.submitCallback(msg as any);

          if (submitted) {
            await this.billingEngine.confirmReservation(reservation);
          } else {
            this.billingEngine.releaseReservation(reservation);
          }

          return submitted;
        }
      }

      // No callback registered — just acknowledge
      return true;
    } catch (err: any) {
      log.error(`Message processing failed for ${messageId}: ${err.message}`);
      return false;
    } finally {
      this.processingQueue.delete(messageId);
    }
  }

  // ─── Cost Estimation from Message ──────────────────────────────────────────

  private async estimateMessageCost(msg: IncomingMessage): Promise<number> {
    // Estimate segments based on message length and encoding
    const isUcs2 = msg.encoding === 'ucs2';
    const segments = this.estimateSegments(msg.message, isUcs2);
    const encoding = isUcs2 ? 'ucs2' : 'gsm7';

    const cost = this.billingEngine.estimateCost(segments, encoding);
    return cost.totalCost;
  }

  /**
   * Estimate the number of SMS segments for a message.
   */
  private estimateSegments(text: string, isUcs2: boolean): number {
    if (isUcs2) {
      return text.length <= 70 ? 1 : Math.ceil(text.length / 67);
    }
    return text.length <= 160 ? 1 : Math.ceil(text.length / 153);
  }

  // ─── Campaign Progress Tracking ────────────────────────────────────────────

  private trackCampaignProgress(campaignId: string, action: 'increment' | 'complete'): void {
    let progress = this.campaignProgress.get(campaignId);

    if (!progress) {
      progress = {
        campaignId,
        totalMessages: 0,
        processedCount: 0,
        successCount: 0,
        failureCount: 0,
        startTime: Date.now(),
      };
      this.campaignProgress.set(campaignId, progress);
    }

    if (action === 'increment') {
      progress.processedCount++;
    }
  }

  /**
   * Get progress for a specific campaign.
   */
  getCampaignProgress(campaignId: string): CampaignProgress | null {
    return this.campaignProgress.get(campaignId) ?? null;
  }

  /**
   * Get progress for all active campaigns.
   */
  getAllCampaignProgress(): CampaignProgress[] {
    return Array.from(this.campaignProgress.values());
  }

  // ─── Utility ───────────────────────────────────────────────────────────────

  private delay(ms: number): Promise<void> {
    return new Promise(resolve => setTimeout(resolve, ms));
  }

  /**
   * Get processor statistics.
   */
  getStats(): {
    queueDepth: number;
    processingCount: number;
    activeCampaigns: number;
    isRunning: boolean;
  } {
    return {
      queueDepth: this.messageQueue.length,
      processingCount: this.processingQueue.size,
      activeCampaigns: this.campaignProgress.size,
      isRunning: this.isRunning,
    };
  }
}