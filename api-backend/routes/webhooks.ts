/**
 * webhooks.ts — Payment Gateway + Aggregator DLR Webhook Routes
 *
 * Payment webhooks:
 *   - POST /api/v1/webhooks/paystack
 *   - POST /api/v1/webhooks/flutterwave
 *   - POST /api/v1/webhooks/stripe
 *
 * Aggregator DLR webhooks:
 *   - POST /api/v1/webhooks/termii/dlr — Termii delivery receipt callback
 *   - POST /api/v1/webhooks/bulksms/dlr — BulkSMS Nigeria delivery receipt
 */

import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { createLogger } from '../../smpp-core/logger';
import { PaymentWebhookHandler } from '../services/paymentWebhooks';
import { DlrHandler } from '../../smpp-core/dlrHandler';
import { DeliveryReceipt, DlrStatus, TelcoOperator } from '../../smpp-core/types';

const log = createLogger('Route:Webhooks');

// ─── Route Registration ──────────────────────────────────────────────────────

export function registerWebhookRoutes(
  app: FastifyInstance,
  webhookHandler: PaymentWebhookHandler,
  dlrHandler?: DlrHandler
): void {
  // ═══════════════════════════════════════════════════════════════════════════
  // PAYMENT WEBHOOKS
  // ═══════════════════════════════════════════════════════════════════════════

  /**
   * POST /api/v1/webhooks/paystack — Paystack payment confirmation
   */
  app.post(
    '/api/v1/webhooks/paystack',
    async (request: FastifyRequest, reply: FastifyReply) => {
      const rawBody = typeof request.body === 'string' ? request.body : JSON.stringify(request.body);
      const headers = request.headers as Record<string, string>;

      const payload = webhookHandler.parsePaystackWebhook(rawBody, headers);
      if (!payload) {
        return reply.status(400).send({ success: false, error: 'Invalid webhook payload' });
      }

      const result = await webhookHandler.processWebhook(payload);
      log.info(`Paystack webhook: ${result.processed ? 'processed' : 'failed'} — ${result.reason || ''}`);

      return reply.status(result.processed ? 200 : 422).send(result);
    }
  );

  /**
   * POST /api/v1/webhooks/flutterwave — Flutterwave payment confirmation
   */
  app.post(
    '/api/v1/webhooks/flutterwave',
    async (request: FastifyRequest, reply: FastifyReply) => {
      const rawBody = typeof request.body === 'string' ? request.body : JSON.stringify(request.body);
      const headers = request.headers as Record<string, string>;

      const payload = webhookHandler.parseFlutterwaveWebhook(rawBody, headers);
      if (!payload) {
        return reply.status(400).send({ success: false, error: 'Invalid webhook payload' });
      }

      const result = await webhookHandler.processWebhook(payload);
      log.info(`Flutterwave webhook: ${result.processed ? 'processed' : 'failed'} — ${result.reason || ''}`);

      return reply.status(result.processed ? 200 : 422).send(result);
    }
  );

  /**
   * POST /api/v1/webhooks/stripe — Stripe payment confirmation
   */
  app.post(
    '/api/v1/webhooks/stripe',
    async (request: FastifyRequest, reply: FastifyReply) => {
      const rawBody = typeof request.body === 'string' ? request.body : JSON.stringify(request.body);
      const headers = request.headers as Record<string, string>;

      const payload = webhookHandler.parseStripeWebhook(rawBody, headers);
      if (!payload) {
        return reply.status(400).send({ success: false, error: 'Invalid webhook payload' });
      }

      const result = await webhookHandler.processWebhook(payload);
      log.info(`Stripe webhook: ${result.processed ? 'processed' : 'failed'} — ${result.reason || ''}`);

      return reply.status(result.processed ? 200 : 422).send(result);
    }
  );

  /**
   * GET /api/v1/webhooks/health — Webhook handler health check
   */
  app.get('/api/v1/webhooks/health', async (_request: FastifyRequest, reply: FastifyReply) => {
    return reply.send({
      success: true,
      data: {
        status: 'ready',
        gateways: ['paystack', 'flutterwave', 'stripe'],
        aggregatorDlr: dlrHandler ? ['termii', 'bulksms'] : [],
        timestamp: Date.now(),
      },
    });
  });

  // ═══════════════════════════════════════════════════════════════════════════
  // AGGREGATOR DLR WEBHOOKS
  // ═══════════════════════════════════════════════════════════════════════════

  if (dlrHandler) {
    /**
     * POST /api/v1/webhooks/termii/dlr — Termii delivery receipt callback
     *
     * Termii sends DLRs to this URL with the following format:
     * {
     *   "message_id": "123456",
     *   "status": "DELIVERED",
     *   "recipient": "2348031234567",
     *   "cost": 1,
     *   "sent_at": "2026-01-01 12:00:00",
     *   "done_at": "2026-01-01 12:00:05"
     * }
     */
    app.post(
      '/api/v1/webhooks/termii/dlr',
      async (request: FastifyRequest, reply: FastifyReply) => {
        try {
          const body = request.body as any;
          if (!body || !body.message_id) {
            return reply.status(400).send({ success: false, error: 'Missing message_id' });
          }

          const dlr: DeliveryReceipt = {
            messageId: body.message_id,
            recipient: body.recipient || '',
            status: mapAggregatorStatus(body.status),
            doneDate: body.done_at || '',
            submitDate: body.sent_at || '',
            errorCode: body.error_code || '000',
            rawPdu: JSON.stringify(body),
            receivedAt: Date.now(),
            operator: TelcoOperator.UNKNOWN,
            sessionId: 'termii-webhook',
          };

          // Emit the DLR for Kafka offload (skip ACK — this is webhook, not SMPP)
          dlrHandler.emit('dlr:received', dlr);
          dlrHandler.forceFlush();

          log.info(`Termii DLR received: ${dlr.messageId} → ${dlr.status}`);
          return reply.status(200).send({ success: true });
        } catch (err: any) {
          log.error(`Termii DLR error: ${err.message}`);
          return reply.status(500).send({ success: false, error: 'Internal error' });
        }
      }
    );

    /**
     * POST /api/v1/webhooks/bulksms/dlr — BulkSMS Nigeria delivery receipt
     *
     * BulkSMS sends DLRs with the format:
     * {
     *   "id": "123456",
     *   "status": "DELIVERED",
     *   "number": "2348031234567",
     *   "cost": "0.40"
     * }
     */
    app.post(
      '/api/v1/webhooks/bulksms/dlr',
      async (request: FastifyRequest, reply: FastifyReply) => {
        try {
          const body = request.body as any;
          const messageId = body?.id || body?.data?.message_id;
          if (!messageId) {
            return reply.status(400).send({ success: false, error: 'Missing message id' });
          }

          const dlr: DeliveryReceipt = {
            messageId: String(messageId),
            recipient: body.number || body.recipient || '',
            status: mapAggregatorStatus(body.status || body.data?.status),
            doneDate: body.done_at || '',
            submitDate: body.sent_at || '',
            errorCode: body.error_code || '000',
            rawPdu: JSON.stringify(body),
            receivedAt: Date.now(),
            operator: TelcoOperator.UNKNOWN,
            sessionId: 'bulksms-webhook',
          };

          // Emit the DLR for Kafka offload
          dlrHandler.emit('dlr:received', dlr);
          dlrHandler.forceFlush();

          log.info(`BulkSMS DLR received: ${dlr.messageId} → ${dlr.status}`);
          return reply.status(200).send({ success: true });
        } catch (err: any) {
          log.error(`BulkSMS DLR error: ${err.message}`);
          return reply.status(500).send({ success: false, error: 'Internal error' });
        }
      }
    );
  }
}

// ─── DLR Status Mapping for Aggregators ────────────────────────────────────

function mapAggregatorStatus(status?: string): DlrStatus {
  if (!status) return DlrStatus.UNKNOWN;

  const upper = status.toUpperCase().trim();

  switch (upper) {
    case 'DELIVERED':
    case 'DELIVERED_TO_HANDSET':
      return DlrStatus.DELIVERED;
    case 'SENT':
    case 'ACCEPTED':
    case 'PENDING':
      return DlrStatus.PENDING;
    case 'REJECTED':
    case 'FAILED':
    case 'NOT_DELIVERED':
      return DlrStatus.REJECTED;
    case 'EXPIRED':
      return DlrStatus.EXPIRED;
    default:
      return DlrStatus.UNKNOWN;
  }
}