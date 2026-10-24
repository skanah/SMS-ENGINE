/**
 * standaloneServer.ts — Lightweight API server for frontend testing
 *
 * Standalone HTTP server with:
 *   - POST /api/v1/sms/send — Simulate SMS sending
 *   - GET  /api/v1/health   — Health check
 *   - GET  /api/v1/admin/health — Dashboard health endpoint
 *   - GET  /api/v1/admin/routes — Dashboard routes endpoint
 *
 * Run: npx ts-node api-backend/standaloneServer.ts
 */

import * as http from 'http';

const PORT = parseInt(process.env.API_PORT || '8081', 10);
const HOST = '0.0.0.0';

function jsonResponse(res: http.ServerResponse, statusCode: number, data: any): void {
  res.writeHead(statusCode, {
    'Content-Type': 'application/json',
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-API-Key',
  });
  res.end(JSON.stringify(data));
}

const server = http.createServer((req: http.IncomingMessage, res: http.ServerResponse) => {
  const { method, url } = req;

  // Handle CORS preflight
  if (method === 'OPTIONS') {
    res.writeHead(204, {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type, Authorization, X-API-Key',
      'Access-Control-Max-Age': '86400',
    });
    res.end();
    return;
  }

  // ─── GET /api/v1/health ─────────────────────────────────────────────────
  if (method === 'GET' && url === '/api/v1/health') {
    return jsonResponse(res, 200, {
      success: true,
      data: {
        service: 'SMS Gateway API',
        version: '1.0.0',
        uptime: process.uptime(),
        timestamp: Date.now(),
      },
    });
  }

  // ─── GET /api/v1/admin/health ───────────────────────────────────────────
  if (method === 'GET' && url === '/api/v1/admin/health') {
    return jsonResponse(res, 200, {
      success: true,
      data: {
        status: 'running',
        uptime: process.uptime(),
        sessions: { total: 30, bound: 0, openCircuits: 0 },
        throughput: {
          submitted: 15420,
          delivered: 15186,
          failed: 234,
          deliveryRate: '98.5%',
        },
        timestamp: Date.now(),
      },
    });
  }

  // ─── GET /api/v1/admin/routes ───────────────────────────────────────────
  if (method === 'GET' && url === '/api/v1/admin/routes') {
    return jsonResponse(res, 200, {
      success: true,
      data: [
        {
          operator: 'mtn',
          sessions: [
            { sessionId: 'mtn-session-1', state: 'disconnected', circuitState: 'half_open', avgResponseTimeMs: 120, currentWindowUsage: 0, errorRate: 0.05 },
            { sessionId: 'mtn-session-2', state: 'disconnected', circuitState: 'open', avgResponseTimeMs: 0, currentWindowUsage: 0, errorRate: 0.15 },
          ],
          totalSubmitted: 8920, totalDelivered: 8780, totalFailed: 140,
        },
        {
          operator: 'airtel',
          sessions: [
            { sessionId: 'airtel-session-1', state: 'disconnected', circuitState: 'closed', avgResponseTimeMs: 95, currentWindowUsage: 0, errorRate: 0.02 },
            { sessionId: 'airtel-session-2', state: 'disconnected', circuitState: 'closed', avgResponseTimeMs: 88, currentWindowUsage: 0, errorRate: 0.01 },
          ],
          totalSubmitted: 4500, totalDelivered: 4436, totalFailed: 64,
        },
        {
          operator: 'glo',
          sessions: [
            { sessionId: 'glo-session-1', state: 'disconnected', circuitState: 'closed', avgResponseTimeMs: 145, currentWindowUsage: 0, errorRate: 0.03 },
          ],
          totalSubmitted: 1800, totalDelivered: 1770, totalFailed: 30,
        },
        {
          operator: '9mobile',
          sessions: [
            { sessionId: '9mobile-session-1', state: 'disconnected', circuitState: 'closed', avgResponseTimeMs: 200, currentWindowUsage: 0, errorRate: 0.01 },
          ],
          totalSubmitted: 200, totalDelivered: 200, totalFailed: 0,
        },
      ],
    });
  }

  // ─── POST /api/v1/sms/send ──────────────────────────────────────────────
  if (method === 'POST' && url === '/api/v1/sms/send') {
    let body = '';
    req.on('data', (chunk: Buffer) => { body += chunk.toString(); });
    req.on('end', () => {
      try {
        const { sender, recipient, message, priority, encoding } = JSON.parse(body);

        if (!sender || !recipient || !message) {
          return jsonResponse(res, 400, {
            success: false,
            error: 'Missing required fields: sender, recipient, message',
          });
        }

        let operator = 'unknown';
        if (recipient.startsWith('234803') || recipient.startsWith('234806')) operator = 'mtn';
        else if (recipient.startsWith('234802') || recipient.startsWith('234808')) operator = 'airtel';
        else if (recipient.startsWith('234805') || recipient.startsWith('234807')) operator = 'glo';
        else if (recipient.startsWith('234809') || recipient.startsWith('234818')) operator = '9mobile';

        const isUcs2 = encoding === 'ucs2';
        const segments = isUcs2 ? Math.ceil(message.length / 67) : Math.ceil(message.length / 153);
        const costPerSegment = isUcs2 ? 150 : 100;
        const totalCost = costPerSegment * segments + 50;
        const id = `MSG${Date.now().toString(36).toUpperCase()}${Math.random().toString(36).slice(2, 6).toUpperCase()}`;

        return jsonResponse(res, 202, {
          success: true,
          messageId: id,
          cost: totalCost,
          operator,
          dndAction: 'normal',
        });
      } catch (err: any) {
        return jsonResponse(res, 400, { success: false, error: 'Invalid JSON body' });
      }
    });
    return;
  }

  // ─── 404 ────────────────────────────────────────────────────────────────
  jsonResponse(res, 404, { success: false, error: 'Not found' });
});

server.listen(PORT, HOST, () => {
  console.log(`\n  ✅ SMS Gateway Test API running on http://localhost:${PORT}`);
  console.log(`  📨 Send SMS:  POST http://localhost:${PORT}/api/v1/sms/send`);
  console.log(`  🏥 Health:    GET  http://localhost:${PORT}/api/v1/health`);
  console.log(`  📊 Dashboard: GET  http://localhost:${PORT}/api/v1/admin/health\n`);
});
