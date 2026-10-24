/**
 * index.ts — SMS Gateway Core Entry Point
 *
 * Bootstraps the entire SMPP engine:
 *   1. Loads environment configuration
 *   2. Initializes the SMPP session manager (TRX connection pool)
 *   3. Initializes the sliding window processor (credit-based pipelining)
 *   4. Initializes the circuit breaker (per-session error monitoring)
 *   5. Initializes the DLR handler (delivery receipt processing)
 *   6. Initializes the DND router (Nigerian DND compliance)
 *   7. Initializes the sender ID rotator (auto-rotation on delivery drop)
 *   8. Initializes Redis client (single-node or cluster auto-detect)
 *   9. Initializes Kafka client for DLR and message routing
 *  10. Starts the API server (Fastify ingestion endpoint)
 *  11. Handles graceful shutdown on SIGTERM/SIGINT
 *
 * Architecture: Designed to run in cluster/worker_threads mode.
 * Each worker thread runs its own instance of this bootstrap.
 */

import { Kafka } from 'kafkajs';
import { createLogger } from './logger';
import { SmppSessionManager } from './smppSessionManager';
import { SlidingWindowProcessor } from './slidingWindowProcessor';
import { CircuitBreaker } from './circuitBreaker';
import { DlrHandler } from './dlrHandler';
import { DndRouter } from './dndRouter';
import { SenderIdRotator } from './senderIdRotator';
import { API, KAFKA, REDIS_CONFIG, resolveRedisUrl, AGGREGATORS } from './config';
import { createRedisClient, connectRedis, disconnectRedis } from './lib/redisClient';
import { ApiServer } from '../api-backend/apiServer';
import { AggregatorRouter, AggregatorDispatcher, TermiiClient, BulkSmsClient } from './aggregator';

const log = createLogger('Bootstrap');

// ─── Global State ───────────────────────────────────────────────────────────

let sessionManager: SmppSessionManager;
let windowProcessor: SlidingWindowProcessor;
let circuitBreaker: CircuitBreaker;
let dlrHandler: DlrHandler;
let dndRouter: DndRouter;
let senderIdRotator: SenderIdRotator;
let redisClient: ReturnType<typeof createRedisClient> | null = null;
let kafkaClient: Kafka | null = null;
let apiServer: ApiServer | null = null;
let aggregatorRouter: AggregatorRouter | null = null;
let aggregatorDispatcher: AggregatorDispatcher | null = null;

// ─── Initialization ─────────────────────────────────────────────────────────

async function initialize(): Promise<void> {
  log.info('========================================');
  log.info('  SMS Gateway Core v1.0.0');
  log.info('  Nigerian Telecom Gateway Engine');
  log.info('========================================');
  log.info('Initializing components...');

  // 0. Initialize Redis client (single-node or cluster auto-detect)
  const redisUrl = resolveRedisUrl();
  log.info(`Connecting to Redis: ${redisUrl.replace(/\/\/.*@/, '//***@')}`);
  try {
    redisClient = createRedisClient({
      url: redisUrl,
      keyPrefix: REDIS_CONFIG.KEY_PREFIX,
      maxRetries: REDIS_CONFIG.MAX_RETRIES,
      retryDelayMs: REDIS_CONFIG.RETRY_DELAY_MS,
    });
    await connectRedis(redisClient);
    log.info('Redis client connected');
  } catch (err: any) {
    log.warn(`Redis connection failed (non-fatal): ${err.message}`);
    log.warn('Running without Redis — DND checks will use local cache only');
  }

  // 0b. Initialize Kafka client
  log.info(`Connecting to Kafka: ${KAFKA.BROKERS.join(', ')}`);
  try {
    kafkaClient = new Kafka({
      clientId: KAFKA.CLIENT_ID,
      brokers: KAFKA.BROKERS,
      retry: {
        maxRetryTime: 30000,
        initialRetryTime: 300,
        retries: 8,
      },
    });
    log.info('Kafka client initialized');
  } catch (err: any) {
    log.warn(`Kafka initialization failed (non-fatal): ${err.message}`);
    log.warn('Running without Kafka — messages will use in-memory queue');
  }

  // 1. SMPP Session Manager — TRX connection pool
  log.info('Starting SMPP session manager...');
  sessionManager = new SmppSessionManager();
  await sessionManager.initialize();

  const boundCount = sessionManager.getBoundCount();
  log.info(`SMPP sessions: ${boundCount} bound`);

  // 2. Sliding Window Processor — credit-based pipelining
  log.info('Starting sliding window processor...');
  windowProcessor = new SlidingWindowProcessor(sessionManager, {
    maxQueueDepth: 100_000,
  });

  // 3. Circuit Breaker — per-session error monitoring
  log.info('Starting circuit breaker...');
  circuitBreaker = new CircuitBreaker(sessionManager);

  // Wire circuit breaker to session manager events
  sessionManager.on('submit_sm_resp', (sessionId: string, pdu: any) => {
    if (pdu.command_status === 0) {
      circuitBreaker.recordSuccess(sessionId);
    } else {
      circuitBreaker.recordFailure(sessionId);
    }
  });

  sessionManager.on('session:error', (sessionId: string) => {
    circuitBreaker.recordFailure(sessionId);
  });

  // 4. DLR Handler — delivery receipt processing
  log.info('Starting DLR handler...');
  dlrHandler = new DlrHandler(kafkaClient || undefined);

  // Wire DLR handler to session manager deliver_sm events
  sessionManager.on('deliver_sm', (sessionId: string, pdu: any) => {
    const metrics = sessionManager.getSession(sessionId);
    const operator = metrics?.operator ?? 'unknown' as any;
    dlrHandler.processDlr(sessionId, pdu, operator);
  });

  // Log DLR batch flushes
  dlrHandler.on('dlr:batch', (batch: any) => {
    log.info(
      `DLR batch ready for DB UPSERT: ${batch.recordCount} records ` +
      `(${Math.round(batch.recordCount / (batch.elapsed / 1000))} records/sec)`
    );
  });

  // Start Kafka producer for DLR offload
  if (kafkaClient) {
    await dlrHandler.startKafkaProducer();
    // Start DLR consumer for batch DB UPSERT
    await dlrHandler.startDlrConsumer();
  }

  // 5. DND Router — Nigerian DND compliance
  log.info('Starting DND router...');
  dndRouter = new DndRouter(redisClient || undefined);

  // 6. Sender ID Rotator — auto-rotation on delivery drop
  log.info('Starting sender ID rotator...');
  senderIdRotator = new SenderIdRotator();

  // Register a default test user (in production, loaded from database)
  senderIdRotator.registerUser('default', 'SMSAlert', [
    'SMSAlert',
    'SMSInfo',
    'SMSNotify',
    'SMSUpdate',
  ]);

  // 7. Initialize Aggregator Routing Engine
  log.info('Initializing aggregator routing...');
  aggregatorRouter = new AggregatorRouter(redisClient || undefined);
  aggregatorDispatcher = new AggregatorDispatcher(aggregatorRouter);

  // Register Termii client (OTP/transactional)
  const termiiClient = new TermiiClient();
  aggregatorRouter.registerClient(termiiClient);
  aggregatorDispatcher.registerClient(termiiClient);

  // Register BulkSMS client (promotional)
  const bulksmsClient = new BulkSmsClient();
  aggregatorRouter.registerClient(bulksmsClient);
  aggregatorDispatcher.registerClient(bulksmsClient);

  log.info(
    `Aggregator routing: OTP→${AGGREGATORS.ROUTE.TRANSACTIONAL_PROVIDER}, ` +
    `Promo→${AGGREGATORS.ROUTE.PROMOTIONAL_PROVIDER}, ` +
    `Fallback→${AGGREGATORS.ROUTE.FALLBACK_PROVIDER}`
  );

  // 8. Start API server
  log.info('Starting API server...');
  apiServer = new ApiServer(
    {
      sessionManager,
      circuitBreaker,
      dndRouter,
      senderIdRotator,
      dlrHandler,
      kafkaClient: kafkaClient || undefined,
      redisClient: redisClient || undefined,
      aggregatorRouter: aggregatorRouter || undefined,
      aggregatorDispatcher: aggregatorDispatcher || undefined,
    },
    undefined, // databaseClient (injected in production)
    undefined  // dlrRepository (injected in production)
  );
  await apiServer.start();

  // 9. Log system status
  log.info('========================================');
  log.info('  SMS Gateway Core — READY');
  log.info(`  Bound sessions: ${boundCount}`);
  log.info(`  Window size: ${128} credits per session`);
  log.info(`  Queue capacity: ${100_000} messages`);
  log.info(`  Redis: ${redisClient ? 'connected' : 'not available'}`);
  log.info(`  Kafka: ${kafkaClient ? 'initialized' : 'not available'}`);
  log.info(`  API: http://${API.HOST}:${API.PORT}`);
  log.info('========================================');

  // Log periodic stats
  setInterval(() => {
    const stats = windowProcessor.getStats();
    const sessionMetrics = sessionManager.getAllSessionMetrics();
    const totalSubmitted = sessionMetrics.reduce((s, m) => s + m.messagesSubmitted, 0);
    const totalDelivered = sessionMetrics.reduce((s, m) => s + m.messagesDelivered, 0);
    const totalFailed = sessionMetrics.reduce((s, m) => s + m.messagesFailed, 0);
    const dlrStats = dlrHandler.getStats();

    log.info(
      `Stats | ` +
      `Processed: ${stats.totalProcessed} | ` +
      `Submitted: ${totalSubmitted} | ` +
      `Delivered: ${totalDelivered} | ` +
      `Failed: ${totalFailed} | ` +
      `Queue: ${stats.queueDepth} | ` +
      `Pending: ${stats.totalPending} | ` +
      `Credits: ${stats.availableCredits} | ` +
      `Timeouts: ${stats.totalTimeouts} | ` +
      `DLRs: ${dlrStats.totalProcessed} (${dlrStats.currentBufferSize} buffered)`
    );
  }, 10_000); // Every 10 seconds
}

// ─── Graceful Shutdown ──────────────────────────────────────────────────────

async function shutdown(signal: string): Promise<void> {
  log.info(`Received ${signal} — initiating graceful shutdown...`);

  // Shutdown in reverse order of initialization
  const shutdownTasks: Promise<void>[] = [];

  // API server first (stop accepting new requests)
  if (apiServer) {
    shutdownTasks.push(apiServer.shutdown());
  }

  if (senderIdRotator) {
    shutdownTasks.push(
      new Promise(resolve => {
        senderIdRotator.shutdown();
        resolve();
      })
    );
  }

  if (dlrHandler) {
    shutdownTasks.push(dlrHandler.shutdown());
  }

  if (circuitBreaker) {
    shutdownTasks.push(
      new Promise(resolve => {
        circuitBreaker.shutdown();
        resolve();
      })
    );
  }

  if (windowProcessor) {
    shutdownTasks.push(windowProcessor.shutdown(10_000));
  }

  if (sessionManager) {
    shutdownTasks.push(sessionManager.shutdown());
  }

  // Redis last (other components may need it during shutdown)
  if (redisClient) {
    shutdownTasks.push(disconnectRedis(redisClient));
  }

  await Promise.allSettled(shutdownTasks);

  log.info('Graceful shutdown complete. Goodbye.');
  process.exit(0);
}

// ─── Main ───────────────────────────────────────────────────────────────────

async function main(): Promise<void> {
  try {
    await initialize();

    // Register signal handlers for graceful shutdown
    process.on('SIGTERM', () => shutdown('SIGTERM'));
    process.on('SIGINT', () => shutdown('SIGINT'));

    // Keep the process alive
    log.info('System running. Press Ctrl+C to shutdown.');

  } catch (err: any) {
    log.error(`Fatal error during initialization: ${err.message}`);
    log.error(err.stack);
    process.exit(1);
  }
}

// Run
main();