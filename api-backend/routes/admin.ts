/**
 * admin.ts — Admin REST API Routes
 *
 * Provides administrative endpoints for:
 *   - Route overrides (drain/disable specific telco routes)
 *   - DND registry management
 *   - Sender ID management
 *   - System telemetry and monitoring
 *   - User management
 */

import { FastifyInstance, FastifyRequest, FastifyReply } from 'fastify';
import { createLogger } from '../../smpp-core/logger';
import { TelcoOperator, RouteAction, AggregatorProvider, MessageIntent } from '../../smpp-core/types';
import { DndRouter } from '../../smpp-core/dndRouter';
import { SenderIdRotator } from '../../smpp-core/senderIdRotator';
import { CircuitBreaker } from '../../smpp-core/circuitBreaker';
import { SmppSessionManager } from '../../smpp-core/smppSessionManager';
import { AggregatorRouter } from '../../smpp-core/aggregator';

const log = createLogger('Route:Admin');

// ─── Route Override Types ────────────────────────────────────────────────────

interface RouteOverrideRequest {
  operator: TelcoOperator;
  action: RouteAction;
  priority?: number;
  expiresAt?: number;
}

// ─── Route Registration ──────────────────────────────────────────────────────

export function registerAdminRoutes(
  app: FastifyInstance,
  sessionManager: SmppSessionManager,
  circuitBreaker: CircuitBreaker,
  dndRouter: DndRouter,
  senderIdRotator: SenderIdRotator,
  aggregatorRouter?: AggregatorRouter
): void {
  // ═════════════════════════════════════════════════════════════════════════
  // Route Overrides
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * GET /api/v1/admin/routes — List all route states
   */
  app.get('/api/v1/admin/routes', async (_request: FastifyRequest, reply: FastifyReply) => {
    const metrics = sessionManager.getAllSessionMetrics();
    const circuitStates = circuitBreaker.getAllStates();

    // Group by operator
    const routes: Record<string, any> = {};

    for (const m of metrics) {
      const op = m.operator;
      if (!routes[op]) {
        routes[op] = {
          operator: op,
          sessions: [],
          totalSubmitted: 0,
          totalDelivered: 0,
          totalFailed: 0,
        };
      }
      routes[op].sessions.push({
        sessionId: m.sessionId,
        state: m.state,
        isCircuitOpen: m.isCircuitOpen,
        currentWindowUsage: m.currentWindowUsage,
        avgResponseTimeMs: m.avgResponseTimeMs,
      });
      routes[op].totalSubmitted += m.messagesSubmitted;
      routes[op].totalDelivered += m.messagesDelivered;
      routes[op].totalFailed += m.messagesFailed;
    }

    // Merge circuit breaker states
    for (const cs of circuitStates) {
      for (const op of Object.keys(routes)) {
        const session = routes[op].sessions.find((s: any) => s.sessionId === cs.sessionId);
        if (session) {
          session.circuitState = cs.state;
          session.errorRate = cs.errorRate;
        }
      }
    }

    return reply.send({ success: true, data: Object.values(routes) });
  });

  /**
   * POST /api/v1/admin/routes/override — Set a route override
   */
  app.post<{ Body: RouteOverrideRequest }>(
    '/api/v1/admin/routes/override',
    async (request: FastifyRequest<{ Body: RouteOverrideRequest }>, reply: FastifyReply) => {
      const { operator, action, priority, expiresAt } = request.body;
      const adminUserId = (request as any).user?.id || 'admin';

      log.info(
        `Route override: ${operator} → ${action} (by ${adminUserId})` +
        (expiresAt ? ` expires at ${new Date(expiresAt).toISOString()}` : '')
      );

      // TODO: Persist to database
      // await db('route_overrides').insert({ operator, action, priority, overridden_by: adminUserId, expires_at: expiresAt ? new Date(expiresAt) : null })
      //   .onConflict('operator').merge();

      return reply.send({
        success: true,
        data: { operator, action, priority, overriddenBy: adminUserId, expiresAt },
      });
    }
  );

  /**
   * DELETE /api/v1/admin/routes/override/:operator — Remove a route override
   */
  app.delete(
    '/api/v1/admin/routes/override/:operator',
    async (request: FastifyRequest<{ Params: { operator: string } }>, reply: FastifyReply) => {
      const { operator } = request.params;

      log.info(`Route override removed for ${operator}`);

      // TODO: Delete from database
      // await db('route_overrides').where('operator', operator).del();

      return reply.send({ success: true, data: { operator, action: 'active' } });
    }
  );

  // ═════════════════════════════════════════════════════════════════════════
  // Circuit Breaker
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * POST /api/v1/admin/circuits/:sessionId/reset — Reset a circuit breaker
   */
  app.post(
    '/api/v1/admin/circuits/:sessionId/reset',
    async (request: FastifyRequest<{ Params: { sessionId: string } }>, reply: FastifyReply) => {
      const { sessionId } = request.params;
      circuitBreaker.resetCircuit(sessionId);
      return reply.send({ success: true, data: { sessionId, state: 'closed' } });
    }
  );

  /**
   * GET /api/v1/admin/circuits — List all circuit breaker states
   */
  app.get('/api/v1/admin/circuits', async (_request: FastifyRequest, reply: FastifyReply) => {
    const states = circuitBreaker.getAllStates();
    return reply.send({ success: true, data: states });
  });

  // ═════════════════════════════════════════════════════════════════════════
  // DND Registry
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * POST /api/v1/admin/dnd/add — Add a number to DND registry
   */
  app.post<{ Body: { msisdn: string } }>(
    '/api/v1/admin/dnd/add',
    async (request: FastifyRequest<{ Body: { msisdn: string } }>, reply: FastifyReply) => {
      const { msisdn } = request.body;
      await dndRouter.addDndNumber(msisdn);
      return reply.send({ success: true, data: { msisdn, action: 'added' } });
    }
  );

  /**
   * POST /api/v1/admin/dnd/remove — Remove a number from DND registry
   */
  app.post<{ Body: { msisdn: string } }>(
    '/api/v1/admin/dnd/remove',
    async (request: FastifyRequest<{ Body: { msisdn: string } }>, reply: FastifyReply) => {
      const { msisdn } = request.body;
      await dndRouter.removeDndNumber(msisdn);
      return reply.send({ success: true, data: { msisdn, action: 'removed' } });
    }
  );

  // ═════════════════════════════════════════════════════════════════════════
  // Sender ID Management
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * GET /api/v1/admin/sender-ids — List all Sender ID states
   */
  app.get('/api/v1/admin/sender-ids', async (_request: FastifyRequest, reply: FastifyReply) => {
    const states = senderIdRotator.getAllStates();
    return reply.send({ success: true, data: states });
  });

  /**
   * POST /api/v1/admin/sender-ids/lock — Lock a user's Sender ID
   */
  app.post<{ Body: { userId: string } }>(
    '/api/v1/admin/sender-ids/lock',
    async (request: FastifyRequest<{ Body: { userId: string } }>, reply: FastifyReply) => {
      const { userId } = request.body;
      senderIdRotator.lockSenderId(userId);
      return reply.send({ success: true, data: { userId, locked: true } });
    }
  );

  /**
   * POST /api/v1/admin/sender-ids/unlock — Unlock a user's Sender ID
   */
  app.post<{ Body: { userId: string } }>(
    '/api/v1/admin/sender-ids/unlock',
    async (request: FastifyRequest<{ Body: { userId: string } }>, reply: FastifyReply) => {
      const { userId } = request.body;
      senderIdRotator.unlockSenderId(userId);
      return reply.send({ success: true, data: { userId, locked: false } });
    }
  );

  /**
   * POST /api/v1/admin/sender-ids/set — Manually set a user's Sender ID
   */
  app.post<{ Body: { userId: string; senderId: string } }>(
    '/api/v1/admin/sender-ids/set',
    async (request: FastifyRequest<{ Body: { userId: string; senderId: string } }>, reply: FastifyReply) => {
      const { userId, senderId } = request.body;
      senderIdRotator.setSenderId(userId, senderId);
      return reply.send({ success: true, data: { userId, senderId } });
    }
  );

  // ═════════════════════════════════════════════════════════════════════════
  // System Health
  // ═════════════════════════════════════════════════════════════════════════

  /**
   * GET /api/v1/admin/health — System health overview
   */
  app.get('/api/v1/admin/health', async (_request: FastifyRequest, reply: FastifyReply) => {
    const metrics = sessionManager.getAllSessionMetrics();
    const circuitStates = circuitBreaker.getAllStates();

    const totalSubmitted = metrics.reduce((s, m) => s + m.messagesSubmitted, 0);
    const totalDelivered = metrics.reduce((s, m) => s + m.messagesDelivered, 0);
    const totalFailed = metrics.reduce((s, m) => s + m.messagesFailed, 0);
    const openCircuits = circuitStates.filter(c => c.state === 'open').length;
    const boundSessions = metrics.filter(m => m.state === 'bound_trx').length;

    return reply.send({
      success: true,
      data: {
        status: 'healthy',
        uptime: process.uptime(),
        sessions: {
          total: metrics.length,
          bound: boundSessions,
          openCircuits,
        },
        throughput: {
          submitted: totalSubmitted,
          delivered: totalDelivered,
          failed: totalFailed,
          deliveryRate: totalSubmitted > 0 ? (totalDelivered / totalSubmitted * 100).toFixed(2) + '%' : '0%',
        },
        timestamp: Date.now(),
      },
    });
  });
}