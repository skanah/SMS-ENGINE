/**
 * dlrHandler.ts — High-Volume DLR (Delivery Receipt) Processing Engine
 *
 * Handles asynchronous delivery receipts from Nigerian telco gateways.
 * Telcos bombard the server with up to 50,000 deliver_sm PDUs per second.
 * This engine:
 *
 *   1. Instantly ACKs the telco with success (command_status=0) to keep
 *      the SMPP channel open and avoid throttling.
 *   2. Parses the raw DLR PDU fields (message_id, stat, done_date, error code).
 *   3. Offloads the parsed DLR to a Kafka topic (sms-dlr-processing) for
 *      asynchronous batch processing.
 *   4. Provides a consumer stub that batches 1,000 DLR records and performs
 *      an optimized database UPSERT.
 *
 * Performance: O(1) per DLR — no blocking I/O in the hot path.
 * The only synchronous operation is the Kafka produce (async).
 */

import { EventEmitter } from 'events';
import { Kafka, Producer, Consumer, EachBatchPayload } from 'kafkajs';
import { createLogger } from './logger';
import { DeliveryReceipt, DlrStatus, TelcoOperator } from './types';
import { KAFKA } from './config';

const log = createLogger('DlrHandler');

// ─── DLR Status Mapping ────────────────────────────────────────────────────
//
// Maps SMPP 3.4 delivery receipt status codes to our internal DlrStatus enum.

const DLR_STATUS_MAP: Record<string, DlrStatus> = {
  'DELIVRD': DlrStatus.DELIVERED,
  'DELIVERED': DlrStatus.DELIVERED,
  'ACCEPTD': DlrStatus.ACCEPTED,
  'ACCEPTED': DlrStatus.ACCEPTED,
  'EXPIRED': DlrStatus.EXPIRED,
  'REJECTD': DlrStatus.REJECTED,
  'REJECTED': DlrStatus.REJECTED,
  'UNDELIV': DlrStatus.UNDELIVERABLE,
  'UNDELIVERABLE': DlrStatus.UNDELIVERABLE,
  'ENROUTE': DlrStatus.PENDING,
  'PENDING': DlrStatus.PENDING,
  'UNKNOWN': DlrStatus.UNKNOWN,
};

// ─── DLR Batch Buffer ──────────────────────────────────────────────────────

interface DlrBatch {
  records: DeliveryReceipt[];
  bufferStartTime: number;
  recordCount: number;
}

// ─── Database Repository Interface (Pluggable) ─────────────────────────────

export interface DlrRepository {
  /**
   * Batch UPSERT delivery receipts.
   * Implementations should use ON CONFLICT (message_id) DO UPDATE.
   */
  batchUpsert(records: DeliveryReceipt[]): Promise<void>;
}

// ─── DLR Handler ───────────────────────────────────────────────────────────

export class DlrHandler extends EventEmitter {
  /** Batch buffer for accumulating DLRs before flush */
  private batchBuffer: DlrBatch;
  private flushTimer: NodeJS.Timeout | null = null;
  private totalDlrsProcessed: number = 0;
  private totalDlrsFailed: number = 0;

  /** Kafka producer for offloading DLRs */
  private kafkaProducer: Producer | null = null;
  private kafkaConsumer: Consumer | null = null;
  private kafka: Kafka | null = null;

  /** Pluggable database repository */
  private repository: DlrRepository | null = null;

  /** ACK callback — called immediately to respond to telco */
  private ackCallback: ((pdu: any, status: number) => void) | null = null;

  constructor(kafkaClient?: Kafka, repository?: DlrRepository) {
    super();
    this.setMaxListeners(100);

    this.batchBuffer = {
      records: [],
      bufferStartTime: Date.now(),
      recordCount: 0,
    };

    if (kafkaClient) {
      this.kafka = kafkaClient;
      this.kafkaProducer = kafkaClient.producer({
        allowAutoTopicCreation: true,
        transactionTimeout: 30000,
      });
    }

    if (repository) {
      this.repository = repository;
    }

    // Start periodic flush timer
    this.startFlushTimer();

    log.info('DLR Handler initialized' +
      (this.kafkaProducer ? ' (Kafka producer ready)' : ' (no Kafka)') +
      (this.repository ? ' (DB repository ready)' : ' (no DB repository)'));
  }

  // ─── ACK Callback Registration ───────────────────────────────────────────

  /**
   * Register the ACK callback that responds to the telco SMPP session.
   * This must be called immediately (synchronously) to keep the SMPP
   * channel flowing.
   *
   * @param callback - Function that sends deliver_sm_resp with command_status
   */
  setAckCallback(callback: (pdu: any, status: number) => void): void {
    this.ackCallback = callback;
  }

  // ─── Hot Path: Process Incoming DLR ──────────────────────────────────────

  /**
   * Process a raw deliver_sm PDU from the SMPP session manager.
   * This is called on the hot path (up to 50,000 times/second).
   *
   * Steps:
   *   1. Immediately ACK the telco with status 0 (success)
   *   2. Parse the DLR fields from the PDU
   *   3. Emit for Kafka offload
   *   4. Add to batch buffer for database UPSERT
   *
   * @param sessionId - The SMPP session that received this DLR
   * @param pdu - The raw deliver_sm PDU from the telco
   * @param operator - The telco operator
   */
  processDlr(sessionId: string, pdu: any, operator: TelcoOperator): void {
    try {
      // Step 1: Immediately ACK the telco (synchronous, non-blocking)
      if (this.ackCallback) {
        this.ackCallback(pdu, 0); // command_status = 0 (success)
      }

      // Step 2: Parse the DLR
      const dlr = this.parseDlrPdu(sessionId, pdu, operator);

      if (!dlr) {
        this.totalDlrsFailed++;
        return;
      }

      // Step 3: Emit for Kafka offload (async, non-blocking)
      this.emit('dlr:received', dlr);

      // Step 4: Add to batch buffer for database UPSERT
      this.addToBatch(dlr);

      this.totalDlrsProcessed++;
    } catch (err: any) {
      log.error(`Error processing DLR: ${err.message}`);
      this.totalDlrsFailed++;
    }
  }

  // ─── DLR Parsing ─────────────────────────────────────────────────────────

  /**
   * Parse a deliver_sm PDU into a structured DeliveryReceipt.
   *
   * The DLR is typically delivered as a text string in the short_message field
   * with the format:
   *   "id:<message_id> sub:<sub> dlvrd:<dlvrd> submit date:<submit_date>
   *    done date:<done_date> stat:<status> err:<error_code> text:<text>"
   *
   * Some telcos also send DLRs in the message_id field with the status
   * encoded in the PDU header.
   *
   * @param sessionId - The SMPP session ID
   * @param pdu - The raw deliver_sm PDU
   * @param operator - The telco operator
   * @returns Parsed DeliveryReceipt or null if parsing failed
   */
  private parseDlrPdu(
    sessionId: string,
    pdu: any,
    operator: TelcoOperator
  ): DeliveryReceipt | null {
    // Extract the short_message as a string
    const rawMessage = pdu.short_message
      ? pdu.short_message.toString('utf-8')
      : '';

    // Try to parse the DLR text format
    const dlr = this.parseDlrText(rawMessage);

    if (dlr) {
      return {
        messageId: dlr.messageId || pdu.message_id || 'unknown',
        recipient: pdu.destination_addr || '',
        status: dlr.status,
        doneDate: dlr.doneDate || '',
        submitDate: dlr.submitDate || '',
        errorCode: dlr.errorCode || '000',
        rawPdu: rawMessage,
        receivedAt: Date.now(),
        operator,
        sessionId,
      };
    }

    // Fallback: use PDU fields directly
    return {
      messageId: pdu.message_id || 'unknown',
      recipient: pdu.destination_addr || '',
      status: this.mapStatus(pdu.stat || 'UNKNOWN'),
      doneDate: pdu.done_date || '',
      submitDate: pdu.submit_date || '',
      errorCode: pdu.error_code?.toString() || '000',
      rawPdu: rawMessage,
      receivedAt: Date.now(),
      operator,
      sessionId,
    };
  }

  /**
   * Parse a DLR text string in the standard SMPP format.
   *
   * Format: "id:IIIIIIII sub:NNN dlvrd:NNN submit date:YYMMDDhhmm
   *          done date:YYMMDDhhmm stat:DDDDDDD err:EEE text:....."
   */
  private parseDlrText(
    text: string
  ): {
    messageId: string;
    status: DlrStatus;
    doneDate: string;
    submitDate: string;
    errorCode: string;
  } | null {
    if (!text || text.length < 10) return null;

    // Check if this looks like a DLR text format
    if (!text.includes('id:') && !text.includes('stat:')) {
      return null;
    }

    const extract = (prefix: string): string => {
      const start = text.indexOf(prefix);
      if (start === -1) return '';
      const valueStart = start + prefix.length;
      const spaceAfter = text.indexOf(' ', valueStart);
      return spaceAfter === -1
        ? text.slice(valueStart)
        : text.slice(valueStart, spaceAfter);
    };

    const messageId = extract('id:');
    const stat = extract('stat:');
    const doneDate = extract('done date:');
    const submitDate = extract('submit date:');
    const errorCode = extract('err:');

    return {
      messageId,
      status: this.mapStatus(stat),
      doneDate,
      submitDate,
      errorCode: errorCode || '000',
    };
  }

  /**
   * Map SMPP status string to our DlrStatus enum.
   */
  private mapStatus(stat: string): DlrStatus {
    const upper = stat.toUpperCase().trim();
    return DLR_STATUS_MAP[upper] || DlrStatus.UNKNOWN;
  }

  // ─── Batch Buffer Management ─────────────────────────────────────────────

  /**
   * Add a parsed DLR to the batch buffer.
   * When the buffer reaches KAFKA.DLR_BATCH_SIZE (1000), it auto-flushes.
   */
  private addToBatch(dlr: DeliveryReceipt): void {
    this.batchBuffer.records.push(dlr);
    this.batchBuffer.recordCount++;

    if (this.batchBuffer.recordCount >= KAFKA.DLR_BATCH_SIZE) {
      this.flushBatch();
    }
  }

  /**
   * Flush the current batch buffer.
   * Emits a 'dlr:batch' event with the accumulated records for the consumer
   * to perform a database UPSERT.
   */
  private flushBatch(): void {
    if (this.batchBuffer.recordCount === 0) return;

    const batch = {
      records: [...this.batchBuffer.records],
      recordCount: this.batchBuffer.recordCount,
      elapsed: Date.now() - this.batchBuffer.bufferStartTime,
    };

    // Emit for the database consumer to process
    this.emit('dlr:batch', batch);

    // Reset buffer
    this.batchBuffer = {
      records: [],
      bufferStartTime: Date.now(),
      recordCount: 0,
    };

    log.debug(`DLR batch flushed: ${batch.recordCount} records in ${batch.elapsed}ms`);
  }

  /**
   * Start the periodic flush timer.
   * Ensures DLRs don't sit in the buffer indefinitely.
   */
  private startFlushTimer(): void {
    this.flushTimer = setInterval(() => {
      this.flushBatch();
    }, KAFKA.DLR_BATCH_TIMEOUT_MS); // Default: 2000ms
  }

  // ─── Kafka Producer ──────────────────────────────────────────────────────

  /**
   * Start the Kafka producer and connect.
   */
  async startKafkaProducer(): Promise<void> {
    if (!this.kafkaProducer) {
      log.warn('Cannot start Kafka producer — no Kafka client configured');
      return;
    }

    try {
      await this.kafkaProducer.connect();
      log.info('Kafka DLR producer connected');

      // Wire the 'dlr:received' event to produce to Kafka
      this.on('dlr:received', (dlr: DeliveryReceipt) => {
        this.produceToKafka(dlr).catch(err => {
          log.error(`Kafka produce error: ${err.message}`);
        });
      });
    } catch (err: any) {
      log.error(`Failed to connect Kafka DLR producer: ${err.message}`);
    }
  }

  /**
   * Produce a DLR to the sms-dlr-processing Kafka topic.
   */
  async produceToKafka(dlr: DeliveryReceipt): Promise<void> {
    if (!this.kafkaProducer) return;

    try {
      await this.kafkaProducer.send({
        topic: KAFKA.TOPICS.DLR_PROCESSING,
        messages: [
          {
            key: dlr.messageId,
            value: JSON.stringify(dlr),
            headers: {
              'messageType': 'dlr',
              'operator': dlr.operator,
              'timestamp': String(dlr.receivedAt),
            },
          },
        ],
      });
    } catch (err: any) {
      log.error(`Failed to produce DLR ${dlr.messageId} to Kafka: ${err.message}`);
      // Re-emit for retry
      this.emit('dlr:produce_failed', dlr);
    }
  }

  // ─── Kafka Consumer (DLR Batch Processor) ────────────────────────────────

  /**
   * Start the Kafka consumer that reads from sms-dlr-processing topic
   * and performs batch UPSERTs to the database.
   *
   * This runs in a separate consumer group for horizontal scaling.
   */
  async startDlrConsumer(): Promise<void> {
    if (!this.kafka) {
      log.warn('Cannot start DLR consumer — no Kafka client configured');
      return;
    }

    try {
      this.kafkaConsumer = this.kafka.consumer({
        groupId: 'sms-dlr-processor-group',
        maxBytesPerPartition: 10485760, // 10 MB
        minBytes: 1,
        maxBytes: 10485760,
        maxWaitTimeInMs: 100,
        retry: {
          maxRetryTime: 30000,
          initialRetryTime: 300,
          retries: 8,
        },
      });

      await this.kafkaConsumer.connect();
      await this.kafkaConsumer.subscribe({
        topic: KAFKA.TOPICS.DLR_PROCESSING,
        fromBeginning: false,
      });

      await this.kafkaConsumer.run({
        eachBatch: async (payload: EachBatchPayload) => {
          await this.processKafkaBatch(payload);
        },
        autoCommit: true,
        autoCommitInterval: 5000,
        autoCommitThreshold: 1000,
      });

      log.info('DLR Kafka consumer started');
    } catch (err: any) {
      log.error(`Failed to start DLR consumer: ${err.message}`);
    }
  }

  /**
   * Process a batch of DLR messages from Kafka.
   * Accumulates up to 1,000 records and performs a single UPSERT transaction.
   */
  private async processKafkaBatch(payload: EachBatchPayload): Promise<void> {
    const { batch, resolveOffset, heartbeat, commitOffsetsIfNecessary } = payload;
    const batchRecords: DeliveryReceipt[] = [];
    const startTime = Date.now();

    try {
      for (const message of batch.messages) {
        if (!message.value) continue;

        try {
          const dlr: DeliveryReceipt = JSON.parse(message.value.toString());
          batchRecords.push(dlr);
        } catch (parseErr) {
          log.warn(`Failed to parse DLR message from Kafka`);
        }

        // Resolve offset as we go
        resolveOffset(message.offset);

        // Heartbeat every 100 records to avoid rebalance
        if (batchRecords.length % 100 === 0) {
          await heartbeat();
        }

        // Flush every 1,000 records
        if (batchRecords.length >= KAFKA.DLR_BATCH_SIZE) {
          await this.flushDlrBatch(batchRecords);
          batchRecords.length = 0; // Clear array
          await commitOffsetsIfNecessary();
        }
      }

      // Flush remaining records
      if (batchRecords.length > 0) {
        await this.flushDlrBatch(batchRecords);
      }

      await commitOffsetsIfNecessary();

      const elapsed = Date.now() - startTime;
      log.debug(`Kafka DLR batch processed in ${elapsed}ms`);
    } catch (err: any) {
      log.error(`Kafka DLR batch processing error: ${err.message}`);
    }
  }

  /**
   * Flush a batch of DLR records to the database via the repository.
   */
  private async flushDlrBatch(records: DeliveryReceipt[]): Promise<void> {
    if (records.length === 0) return;

    const startTime = Date.now();

    try {
      // Use the pluggable repository if available
      if (this.repository) {
        await this.repository.batchUpsert(records);
      } else {
        // Fallback: emit event for external handling
        this.emit('dlr:batch_persist', {
          records,
          recordCount: records.length,
          elapsed: Date.now() - startTime,
        });
      }

      const elapsed = Date.now() - startTime;
      log.info(
        `DLR batch UPSERT: ${records.length} records in ${elapsed}ms ` +
        `(${Math.round(records.length / (elapsed / 1000))} records/sec)`
      );
    } catch (err: any) {
      log.error(`DLR batch UPSERT failed: ${err.message}`);
      // Re-emit for retry
      this.emit('dlr:batch_retry', { records, recordCount: records.length });
    }
  }

  // ─── Database Consumer Stub (Legacy) ─────────────────────────────────────

  /**
   * Process a batch of DLRs for database UPSERT.
   * This is the consumer that reads from the sms-dlr-processing topic.
   *
   * In production, this runs in a separate consumer group.
   *
   * @param batch - The batch of DLR records to persist
   */
  async processBatchForDb(
    batch: { records: DeliveryReceipt[]; recordCount: number }
  ): Promise<void> {
    if (batch.recordCount === 0) return;

    const startTime = Date.now();

    try {
      if (this.repository) {
        await this.repository.batchUpsert(batch.records);
      } else {
        // Emit for external handling
        this.emit('dlr:batch_persist', {
          records: batch.records,
          recordCount: batch.recordCount,
          elapsed: Date.now() - startTime,
        });
      }

      const elapsed = Date.now() - startTime;
      log.info(
        `DB batch UPSERT: ${batch.recordCount} records in ${elapsed}ms ` +
        `(${Math.round(batch.recordCount / (elapsed / 1000))} records/sec)`
      );
    } catch (err: any) {
      log.error(`DB batch UPSERT failed: ${err.message}`);
      this.emit('dlr:batch_retry', batch);
    }
  }

  // ─── Utility Methods ─────────────────────────────────────────────────────

  /**
   * Get processing statistics.
   */
  getStats(): {
    totalProcessed: number;
    totalFailed: number;
    currentBufferSize: number;
  } {
    return {
      totalProcessed: this.totalDlrsProcessed,
      totalFailed: this.totalDlrsFailed,
      currentBufferSize: this.batchBuffer.recordCount,
    };
  }

  /**
   * Force-flush the current buffer (called during shutdown).
   */
  forceFlush(): void {
    this.flushBatch();
  }

  // ─── Graceful Shutdown ───────────────────────────────────────────────────

  async shutdown(): Promise<void> {
    log.info('Shutting down DLR handler...');

    // Stop the flush timer
    if (this.flushTimer) {
      clearInterval(this.flushTimer);
      this.flushTimer = null;
    }

    // Flush any remaining records
    this.forceFlush();

    // Disconnect Kafka consumer
    if (this.kafkaConsumer) {
      try {
        await this.kafkaConsumer.disconnect();
        log.info('DLR Kafka consumer disconnected');
      } catch (err) {
        log.warn(`DLR consumer disconnect error: ${(err as Error).message}`);
      }
    }

    // Disconnect Kafka producer
    if (this.kafkaProducer) {
      try {
        await this.kafkaProducer.disconnect();
        log.info('DLR Kafka producer disconnected');
      } catch (err) {
        log.warn(`DLR producer disconnect error: ${(err as Error).message}`);
      }
    }

    log.info(
      `DLR handler shutdown complete. ` +
      `Processed: ${this.totalDlrsProcessed}, Failed: ${this.totalDlrsFailed}`
    );
  }
}