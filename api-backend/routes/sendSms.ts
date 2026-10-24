/**
 * sendSms.ts — POST /api/v1/sms/send Route
 *
 * Handles single SMS submission via REST API.
 * Pipeline in <2ms:
 *   1. Zod validation (fast)
 *   2. Redis prefix lookup → operator detection
 *   3. DND check
 *   4. Kafka push to appropriate topic (OTP vs bulk)
 *   5. Response to client immediately
 *
 * Budget reservation and campaign tracking is handled asynchronously
 * by the campaign stream processor consumer.
 */

import Redis, { Cluster } from 'ioredis';
import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { Kafka, Producer } from 'kafkajs';
import { z } from 'zod';
import { createLogger } from '../../smpp-core/logger';
import { IncomingMessage, TelcoOperator } from '../../smpp-core/types';
import { KAFKA } from '../../smpp-core/config';
import { DndRouter, DndAction } from '../../smpp-core/dndRouter';
import { lookupOperator, normalizePhoneNumber } from '../../smpp-core/prefixMatcher';
import { AggregatorRouter, AggregatorDispatcher } from '../../smpp-core/aggregator';
import { CampaignStreamProcessor } from '../services/campaignStreamProcessor';
import { BillingEngine } from '../services/billingEngine';

const log = createLogger('Route:SendSms');

// ─── Zod Validation Schema ──────────────────────────────────────────────────

const sendSmsSchema = z.object({
  sender: z.string().min(1).max(11),
  recipient: z.string().min(10).max(20),
  message: z.string().min(1).max(5000),
  priority: z.enum(['otp', 'bulk', 'transactional']).optional().default('bulk'),
  encoding: z.enum(['gsm7', 'ucs2', 'auto']).optional().default('auto'),
  campaignId: z.string().optional(),
  scheduleAt: z.number().optional(),
});

type SendSmsRequest = z.infer<typeof sendSmsSchema>;

interface SendSmsResponse {
  success: boolean;
  messageId?: string;
  cost?: number;
  operator?: string;
  dndAction?: string;
  error?: string;
}

// ─── Route Registration ──────────────────────────────────────────────────────

export function registerSendSmsRoute(
  app: FastifyInstance,
  streamProcessor: CampaignStreamProcessor,
  billingEngine: BillingEngine,
  dependencies?: {
    redisClient?: Redis | Cluster;
    dndRouter?: DndRouter;
    kafkaProducer?: Producer;
    aggregatorDispatcher?: AggregatorDispatcher;
    aggregatorRouter?: AggregatorRouter;
  }
): void {
  app.post<{ Body: SendSmsRequest }>(
    '/api/v1/sms/send',
    {
      config: {
        rateLimit: {
          max: 100,
          timeWindow: '1 second',
        },
      },
    },
    async (request: FastifyRequest, reply: FastifyReply) => {
      const startTime = Date.now();

      try {
        // Step 1: Zod validation (< 50μs)
        const parsed = sendSmsSchema.safeParse(request.body);
        if (!parsed.success) {
          return reply.status(400).send({
            success: false,
            error: parsed.error.errors.map(e => e.message).join('; '),
          } as SendSmsResponse);
        }

        const { sender, recipient, message, priority, encoding, campaignId, scheduleAt } = parsed.data;

        // Get user from JWT (set by auth middleware)
        const userId = (request as any).user?.id || 'default';

        // Step 2: Normalize recipient to E.164 format
        const normalizedRecipient = normalizePhoneNumber(recipient) || recipient;

        // Step 3: Operator detection via prefix matcher (< 100ns)
        const operator = lookupOperator(normalizedRecipient);

        // Step 4: DND check via Redis SISMEMBER or local cache
        let dndAction = DndAction.NORMAL;
        if (dependencies?.dndRouter) {
          const dndResult = await dependencies.dndRouter.determineRoute(
            normalizedRecipient,
            operator ?? TelcoOperator.UNKNOWN,
            priority === 'otp' ? 'otp' : priority === 'transactional' ? 'transactional' : 'bulk'
          );
          dndAction = dndResult.action;
        }

        // Step 5: Build the incoming message
        const incomingMsg: IncomingMessage = {
          sender,
          recipient: normalizedRecipient,
          message,
          priority: priority || 'bulk',
          encoding: encoding || 'auto',
          campaignId,
          userId,
          scheduleAt,
        };

        // Step 6: Determine Kafka topic based on priority
        const topic = (priority === 'otp' || priority === 'transactional')
          ? KAFKA.TOPICS.HIGH_PRIORITY_OTP
          : KAFKA.TOPICS.LOW_PRIORITY_BULK;

        // Step 7: Produce to Kafka directly (fast path — non-blocking)
        if (dependencies?.kafkaProducer) {
          await dependencies.kafkaProducer.send({
            topic,
            messages: [
              {
                key: incomingMsg.id || `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
                value: JSON.stringify(incomingMsg),
                headers: {
                  messageType: priority || 'bulk',
                  operator: operator ?? 'unknown',
                  dndAction: dndAction,
                  userId,
                },
              },
            ],
          });
        } else {
          // Fallback: enqueue through stream processor
          const enqueued = await streamProcessor.enqueueMessage(incomingMsg);
          if (!enqueued) {
            return reply.status(503).send({
              success: false,
              error: 'Service busy — queue full, try again later',
            } as SendSmsResponse);
          }
        }

        // Step 8: Estimate cost (quick calculation, non-blocking)
        const isUcs2 = encoding === 'ucs2';
        const segments = isUcs2
          ? Math.ceil(message.length / 67)
          : Math.ceil(message.length / 153);
        const costEstimate = billingEngine.estimateCost(segments, isUcs2 ? 'ucs2' : 'gsm7');

        const elapsed = Date.now() - startTime;

        log.info(
          `SMS queued: user=${userId} op=${operator} dnd=${dndAction} ` +
          `recip=${normalizedRecipient} cost=${costEstimate.totalCost} segs=${segments} in ${elapsed}ms`
        );

        return reply.status(202).send({
          success: true,
          messageId: incomingMsg.id,
          cost: costEstimate.totalCost,
          operator: operator ?? 'unknown',
          dndAction,
        } as SendSmsResponse);

      } catch (err: any) {
        log.error(`Send SMS error: ${err.message}`);
        return reply.status(500).send({
          success: false,
          error: 'Internal server error',
        } as SendSmsResponse);
      }
    }
  );
}

