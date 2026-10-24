/**
 * DashboardOverview.tsx — Real-Time SMS Gateway Dashboard
 *
 * Displays live metrics from the SMPP core engine:
 *   - Total throughput (submitted, delivered, failed)
 *   - Per-operator session health
 *   - Circuit breaker states
 *   - Kafka queue depths
 *   - DLR processing rate
 *
 * Polls the /api/v1/admin/health and /api/v1/admin/routes endpoints
 * every 5 seconds for real-time updates.
 */

import React, { useState, useEffect, useCallback } from 'react';

// ─── Types ───────────────────────────────────────────────────────────────────

interface HealthData {
  status: string;
  uptime: number;
  sessions: {
    total: number;
    bound: number;
    openCircuits: number;
  };
  throughput: {
    submitted: number;
    delivered: number;
    failed: number;
    deliveryRate: string;
  };
  timestamp: number;
}

interface SessionInfo {
  sessionId: string;
  state: string;
  isCircuitOpen: boolean;
  currentWindowUsage: number;
  avgResponseTimeMs: number;
  circuitState?: string;
  errorRate?: number;
}

interface RouteData {
  operator: string;
  sessions: SessionInfo[];
  totalSubmitted: number;
  totalDelivered: number;
  totalFailed: number;
}

interface DashboardData {
  health: HealthData | null;
  routes: RouteData[];
}

// ─── Color Helpers ───────────────────────────────────────────────────────────

const statusColor = (state: string): string => {
  switch (state) {
    case 'bound_trx': return '#22c55e'; // green
    case 'connecting': return '#f59e0b'; // amber
    case 'disconnected': return '#ef4444'; // red
    case 'error': return '#dc2626'; // dark red
    default: return '#6b7280'; // gray
  }
};

const circuitColor = (state?: string): string => {
  switch (state) {
    case 'closed': return '#22c55e';
    case 'half_open': return '#f59e0b';
    case 'open': return '#ef4444';
    default: return '#6b7280';
  }
};

const formatUptime = (seconds: number): string => {
  const h = Math.floor(seconds / 3600);
  const m = Math.floor((seconds % 3600) / 60);
  const s = Math.floor(seconds % 60);
  return `${h}h ${m}m ${s}s`;
};

// ─── Metric Card ─────────────────────────────────────────────────────────────

const MetricCard: React.FC<{
  label: string;
  value: string | number;
  color?: string;
  subtitle?: string;
}> = ({ label, value, color, subtitle }) => (
  <div style={{
    background: '#1f2937',
    borderRadius: '8px',
    padding: '16px',
    minWidth: '160px',
    border: `1px solid ${color || '#374151'}`,
  }}>
    <div style={{ color: '#9ca3af', fontSize: '12px', textTransform: 'uppercase', letterSpacing: '0.05em' }}>
      {label}
    </div>
    <div style={{ color: color || '#ffffff', fontSize: '28px', fontWeight: 700, marginTop: '4px' }}>
      {value}
    </div>
    {subtitle && (
      <div style={{ color: '#6b7280', fontSize: '11px', marginTop: '4px' }}>
        {subtitle}
      </div>
    )}
  </div>
);

// ─── Session Row ─────────────────────────────────────────────────────────────

const SessionRow: React.FC<{ session: SessionInfo }> = ({ session }) => (
  <tr style={{ borderBottom: '1px solid #374151' }}>
    <td style={{ padding: '8px 12px', fontSize: '13px' }}>{session.sessionId}</td>
    <td style={{ padding: '8px 12px' }}>
      <span style={{
        display: 'inline-block',
        width: '10px',
        height: '10px',
        borderRadius: '50%',
        background: statusColor(session.state),
        marginRight: '6px',
        verticalAlign: 'middle',
      }} />
      <span style={{ fontSize: '13px', verticalAlign: 'middle' }}>{session.state}</span>
    </td>
    <td style={{ padding: '8px 12px' }}>
      <span style={{
        display: 'inline-block',
        width: '10px',
        height: '10px',
        borderRadius: '50%',
        background: circuitColor(session.circuitState),
        marginRight: '6px',
        verticalAlign: 'middle',
      }} />
      <span style={{ fontSize: '13px', verticalAlign: 'middle' }}>{session.circuitState || '—'}</span>
    </td>
    <td style={{ padding: '8px 12px', fontSize: '13px' }}>
      {session.currentWindowUsage > 0
        ? `${(session.errorRate ?? 0 * 100).toFixed(1)}%`
        : '—'}
    </td>
    <td style={{ padding: '8px 12px', fontSize: '13px' }}>{session.avgResponseTimeMs.toFixed(0)}ms</td>
    <td style={{ padding: '8px 12px', fontSize: '13px' }}>{session.currentWindowUsage}</td>
  </tr>
);

// ─── Operator Panel ──────────────────────────────────────────────────────────

const OperatorPanel: React.FC<{ route: RouteData }> = ({ route }) => {
  const totalSuccess = route.totalDelivered;
  const totalRate = route.totalSubmitted > 0
    ? ((totalSuccess / route.totalSubmitted) * 100).toFixed(1)
    : '0.0';

  return (
    <div style={{
      background: '#111827',
      borderRadius: '8px',
      border: '1px solid #374151',
      marginBottom: '16px',
      overflow: 'hidden',
    }}>
      {/* Header */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        alignItems: 'center',
        padding: '12px 16px',
        background: '#1f2937',
        borderBottom: '1px solid #374151',
      }}>
        <h3 style={{ margin: 0, color: '#ffffff', fontSize: '16px', fontWeight: 600 }}>
          {route.operator.toUpperCase()}
        </h3>
        <div style={{ display: 'flex', gap: '24px', fontSize: '13px' }}>
          <span style={{ color: '#22c55e' }}>✓ {route.totalDelivered.toLocaleString()}</span>
          <span style={{ color: '#ef4444' }}>✗ {route.totalFailed.toLocaleString()}</span>
          <span style={{ color: '#f59e0b' }}>📨 {route.totalSubmitted.toLocaleString()}</span>
          <span style={{ color: '#60a5fa' }}>📊 {totalRate}%</span>
        </div>
      </div>

      {/* Sessions Table */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
          <thead>
            <tr style={{ color: '#9ca3af', fontSize: '11px', textTransform: 'uppercase' }}>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>Session</th>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>State</th>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>Circuit</th>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>Error Rate</th>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>Avg Response</th>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>Window Usage</th>
            </tr>
          </thead>
          <tbody>
            {route.sessions.map(s => (
              <SessionRow key={s.sessionId} session={s} />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// ─── Main Dashboard Component ────────────────────────────────────────────────

export const DashboardOverview: React.FC = () => {
  const [data, setData] = useState<DashboardData>({ health: null, routes: [] });
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);

  const fetchData = useCallback(async () => {
    try {
      const baseUrl = process.env.REACT_APP_API_URL || 'http://localhost:8080';

      const [healthResp, routesResp] = await Promise.all([
        fetch(`${baseUrl}/api/v1/admin/health`),
        fetch(`${baseUrl}/api/v1/admin/routes`),
      ]);

      if (!healthResp.ok || !routesResp.ok) {
        throw new Error('Failed to fetch dashboard data');
      }

      const healthJson = await healthResp.json();
      const routesJson = await routesResp.json();

      setData({
        health: healthJson.data,
        routes: routesJson.data || [],
      });
      setError(null);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  // Poll every 5 seconds
  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000);
    return () => clearInterval(interval);
  }, [fetchData]);

  // ─── Render ───────────────────────────────────────────────────────────────

  if (loading) {
    return (
      <div style={{ padding: '24px', color: '#9ca3af', textAlign: 'center' }}>
        Loading dashboard...
      </div>
    );
  }

  const { health, routes } = data;

  return (
    <div style={{
      background: '#0f172a',
      minHeight: '100vh',
      padding: '24px',
      fontFamily: "'Inter', -apple-system, sans-serif",
      color: '#ffffff',
    }}>
      {/* Header */}
      <div style={{ marginBottom: '24px' }}>
        <h1 style={{ fontSize: '24px', fontWeight: 700, margin: 0 }}>SMS Gateway Dashboard</h1>
        <p style={{ color: '#6b7280', fontSize: '14px', margin: '4px 0 0 0' }}>
          Nigerian Telecom SMS Engine — Real-Time Monitoring
        </p>
      </div>

      {/* Error Banner */}
      {error && (
        <div style={{
          background: '#7f1d1d',
          border: '1px solid #dc2626',
          borderRadius: '8px',
          padding: '12px 16px',
          marginBottom: '16px',
          color: '#fca5a5',
          fontSize: '14px',
        }}>
          ⚠ Connection error: {error}
        </div>
      )}

      {/* Metric Cards */}
      {health && (
        <div style={{
          display: 'flex',
          gap: '16px',
          flexWrap: 'wrap',
          marginBottom: '24px',
        }}>
          <MetricCard
            label="Sessions"
            value={`${health.sessions.bound}/${health.sessions.total}`}
            color="#60a5fa"
            subtitle={`${health.sessions.openCircuits} circuits open`}
          />
          <MetricCard
            label="Submitted"
            value={health.throughput.submitted.toLocaleString()}
            color="#f59e0b"
          />
          <MetricCard
            label="Delivered"
            value={health.throughput.delivered.toLocaleString()}
            color="#22c55e"
            subtitle={`Rate: ${health.throughput.deliveryRate}`}
          />
          <MetricCard
            label="Failed"
            value={health.throughput.failed.toLocaleString()}
            color="#ef4444"
          />
          <MetricCard
            label="Uptime"
            value={formatUptime(health.uptime)}
            color="#a78bfa"
          />
        </div>
      )}

      {/* Operator Panels */}
      <div>
        <h2 style={{ fontSize: '18px', fontWeight: 600, marginBottom: '12px', color: '#d1d5db' }}>
          Routes by Operator
        </h2>
        {routes.length === 0 ? (
          <div style={{
            background: '#1f2937',
            borderRadius: '8px',
            padding: '32px',
            textAlign: 'center',
            color: '#6b7280',
            border: '1px dashed #374151',
          }}>
            No route data available. Is the SMPP engine running?
          </div>
        ) : (
          routes.map(route => (
            <OperatorPanel key={route.operator} route={route} />
          ))
        )}
      </div>
    </div>
  );
};

export default DashboardOverview;