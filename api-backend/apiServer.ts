/**
 * apiServer.ts — Fastify HTTP API Server Bootstrap
 *
 * Wires together all REST API routes, middleware, and service dependencies
 * for the SMS Gateway backend. Runs as a standalone HTTP server or can be
 * embedded into the main bootstrap.
 *
 * Routes:
 *   - POST /api/v1/sms/send      — Single SMS submission
 *   - GET  /api/v1/admin/routes   — Route monitoring & control
 *   - POST /api/v1/admin/routes/override — Route override control
 *   - GET  /api/v1/admin/health   — System health endpoint
 *   - POST /api/v1/webhooks/*     — Payment gateway webhooks
 */

import Fastify, { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import fastifyJwt from '@fastify/jwt';
import fastifyCors from '@fastify/cors';
import fastifyMultipart from '@fastify/multipart';

import { Kafka } from 'kafkajs';
import Redis, { Cluster } from 'ioredis';
import { createLogger } from '../smpp-core/logger';
import { API, KAFKA } from '../smpp-core/config';

// Core SMPP dependencies
import { SmppSessionManager } from '../smpp-core/smppSessionManager';
import { CircuitBreaker } from '../smpp-core/circuitBreaker';
import { DndRouter } from '../smpp-core/dndRouter';
import { SenderIdRotator } from '../smpp-core/senderIdRotator';
import { DlrHandler, DlrRepository } from '../smpp-core/dlrHandler';
import { createRedisClient } from '../smpp-core/lib/redisClient';
import { AggregatorRouter, AggregatorDispatcher, TermiiClient, BulkSmsClient } from '../smpp-core/aggregator';

// API services
import { BillingEngine } from './services/billingEngine';
import { PaymentWebhookHandler } from './services/paymentWebhooks';
import { CampaignStreamProcessor } from './services/campaignStreamProcessor';

// Routes
import { registerSendSmsRoute } from './routes/sendSms';
import { registerAdminRoutes } from './routes/admin';
import { registerWebhookRoutes } from './routes/webhooks';

const log = createLogger('ApiServer');

// ─── Server State ────────────────────────────────────────────────────────────

export interface ApiServerDependencies {
  sessionManager: SmppSessionManager;
  circuitBreaker: CircuitBreaker;
  dndRouter: DndRouter;
  senderIdRotator: SenderIdRotator;
  dlrHandler?: DlrHandler;
  kafkaClient?: Kafka;
  redisClient?: Redis | Cluster;
  aggregatorRouter?: AggregatorRouter;
  aggregatorDispatcher?: AggregatorDispatcher;
}

// ─── Server Class ────────────────────────────────────────────────────────────

export class ApiServer {
  private app: FastifyInstance;
  private billingEngine: BillingEngine;
  private webhookHandler: PaymentWebhookHandler;
  private streamProcessor: CampaignStreamProcessor;
  private deps: ApiServerDependencies;
  private isRunning: boolean = false;
  private kafkaProducer: import('kafkajs').Producer | null = null;

  constructor(deps: ApiServerDependencies, databaseClient?: any, dlrRepository?: DlrRepository) {
    this.deps = deps;

    // Initialize Fastify
    this.app = Fastify({
      logger: false, // We use our own logger
      bodyLimit: API.MAX_PAYLOAD_BYTES,
    });

    // Initialize services
    this.billingEngine = new BillingEngine(databaseClient);
    this.webhookHandler = new PaymentWebhookHandler(this.billingEngine);
    this.streamProcessor = new CampaignStreamProcessor(this.billingEngine, undefined, deps.kafkaClient);

    // Initialize Kafka producer for sendSms route
    if (deps.kafkaClient) {
      this.kafkaProducer = deps.kafkaClient.producer({
        allowAutoTopicCreation: true,
        transactionTimeout: 30000,
      });
    }

    // Register plugins
    this.registerPlugins();

    // Register middleware
    this.registerMiddleware();

    // Register routes
    this.registerRoutes();
  }

  // ─── Plugin Registration ───────────────────────────────────────────────────

  private registerPlugins(): void {
    // CORS
    this.app.register(fastifyCors, {
      origin: '*',
      methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
      allowedHeaders: ['Content-Type', 'Authorization', 'X-API-Key'],
    });

    // JWT authentication
    this.app.register(fastifyJwt, {
      secret: API.JWT_SECRET,
      sign: { expiresIn: API.JWT_EXPIRY },
    });

    // File upload / multipart support (for CSV campaign uploads)
    this.app.register(fastifyMultipart, {
      limits: {
        fileSize: API.MAX_PAYLOAD_BYTES,
        files: 1,
      },
    });
  }

  // ─── Middleware ─────────────────────────────────────────────────────────────

  private registerMiddleware(): void {
    // Request logging
    this.app.addHook('onRequest', async (request: FastifyRequest) => {
      (request as any).startTime = Date.now();
    });

    this.app.addHook('onResponse', async (request: FastifyRequest, reply: FastifyReply) => {
      const startTime = (request as any).startTime || Date.now();
      const elapsed = Date.now() - startTime;
      log.info(
        `${reply.statusCode} ${request.method} ${request.url} — ${elapsed}ms`
      );
    });

    // Auth decorator
    this.app.decorate('authenticate', async (request: FastifyRequest, reply: FastifyReply) => {
      try {
        await request.jwtVerify();
      } catch (err) {
        reply.status(401).send({ success: false, error: 'Unauthorized' });
      }
    });
  }

  // ─── Route Registration ────────────────────────────────────────────────────

  private registerRoutes(): void {
    // Health check (public)
    this.app.get('/api/v1/health', async (_request: FastifyRequest, reply: FastifyReply) => {
      return reply.send({
        success: true,
        data: {
          service: 'SMS Gateway API',
          version: '1.0.0',
          uptime: process.uptime(),
          timestamp: Date.now(),
        },
      });
    });

    // SMS send route (authenticated)
    registerSendSmsRoute(this.app, this.streamProcessor, this.billingEngine, {
      redisClient: this.deps.redisClient,
      dndRouter: this.deps.dndRouter,
      kafkaProducer: this.kafkaProducer || undefined,
    });

    // Admin routes (authenticated)
    registerAdminRoutes(
      this.app,
      this.deps.sessionManager,
      this.deps.circuitBreaker,
      this.deps.dndRouter,
      this.deps.senderIdRotator,
      this.deps.aggregatorRouter
    );

    // Webhook routes (no auth — signature verification handles security)
    registerWebhookRoutes(this.app, this.webhookHandler, this.deps.dlrHandler);
  }

  // ─── Lifecycle ─────────────────────────────────────────────────────────────

  /**
   * Start the API server.
   */
  async start(): Promise<void> {
    if (this.isRunning) return;

    try {
      // Start the campaign stream processor
      await this.streamProcessor.start();

      // Start listening
      await this.app.listen({
        port: API.PORT,
        host: API.HOST,
      });

      this.isRunning = true;

      log.info(`API server listening on ${API.HOST}:${API.PORT}`);
      log.info(`Health check: http://localhost:${API.PORT}/api/v1/health`);
    } catch (err: any) {
      log.error(`Failed to start API server: ${err.message}`);
      throw err;
    }
  }

  /**
   * Graceful shutdown.
   */
  async shutdown(): Promise<void> {
    log.info('Shutting down API server...');

    // Stop stream processor
    await this.streamProcessor.stop(5_000);

    // Close Fastify server
    await this.app.close();

    this.isRunning = false;
    log.info('API server shutdown complete');
  }

  // ─── Accessors ─────────────────────────────────────────────────────────────

  getServer(): FastifyInstance {
    return this.app;
  }

  getBillingEngine(): BillingEngine {
    return this.billingEngine;
  }

  getStreamProcessor(): CampaignStreamProcessor {
    return this.streamProcessor;
  }
}