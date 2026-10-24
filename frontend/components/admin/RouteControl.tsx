/**
 * RouteControl.tsx — Admin Route Override Control Panel
 *
 * Allows administrators to view and control SMPP session routing:
 *   - View per-operator session health and circuit breaker states
 *   - Set route overrides (active / drain / disabled)
 *   - Reset circuit breakers
 *   - Monitor real-time error rates
 *
 * Communicates with the /api/v1/admin/* endpoints.
 */

import React, { useState, useEffect, useCallback } from 'react';

// ─── Types ───────────────────────────────────────────────────────────────────

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

type RouteAction = 'active' | 'drain' | 'disabled';

interface OverrideState {
  operator: string;
  action: RouteAction;
  loading: boolean;
}

// ─── Color Helpers ───────────────────────────────────────────────────────────

const actionColors: Record<RouteAction, string> = {
  active: '#22c55e',
  drain: '#f59e0b',
  disabled: '#ef4444',
};

const statusColor = (state: string): string => {
  switch (state) {
    case 'bound_trx': return '#22c55e';
    case 'connecting': return '#f59e0b';
    case 'disconnected': return '#ef4444';
    case 'error': return '#dc2626';
    default: return '#6b7280';
  }
};

// ─── Route Action Badge ──────────────────────────────────────────────────────

const ActionBadge: React.FC<{ action: RouteAction }> = ({ action }) => (
  <span style={{
    display: 'inline-block',
    padding: '2px 8px',
    borderRadius: '4px',
    fontSize: '11px',
    fontWeight: 600,
    textTransform: 'uppercase',
    background: actionColors[action] + '22',
    color: actionColors[action],
    border: `1px solid ${actionColors[action]}`,
  }}>
    {action}
  </span>
);

// ─── Operator Route Card ─────────────────────────────────────────────────────

const OperatorRouteCard: React.FC<{
  route: RouteData;
  currentOverride: OverrideState | null;
  onSetOverride: (operator: string, action: RouteAction) => void;
  onResetCircuit: (sessionId: string) => void;
}> = ({ route, currentOverride, onSetOverride, onResetCircuit }) => {
  const totalRate = route.totalSubmitted > 0
    ? ((route.totalDelivered / route.totalSubmitted) * 100).toFixed(1)
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
        flexWrap: 'wrap',
        gap: '8px',
      }}>
        <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
          <h3 style={{ margin: 0, color: '#ffffff', fontSize: '16px', fontWeight: 600 }}>
            {route.operator.toUpperCase()}
          </h3>
          {currentOverride && <ActionBadge action={currentOverride.action} />}
        </div>

        <div style={{ display: 'flex', gap: '8px', alignItems: 'center' }}>
          {/* Override controls */}
          {(['active', 'drain', 'disabled'] as RouteAction[]).map(action => (
            <button
              key={action}
              onClick={() => onSetOverride(route.operator, action)}
              disabled={currentOverride?.loading}
              style={{
                padding: '4px 12px',
                borderRadius: '4px',
                border: `1px solid ${actionColors[action]}`,
                background: currentOverride?.action === action
                  ? actionColors[action]
                  : 'transparent',
                color: currentOverride?.action === action ? '#ffffff' : actionColors[action],
                cursor: 'pointer',
                fontSize: '11px',
                fontWeight: 600,
                textTransform: 'uppercase',
                opacity: currentOverride?.loading ? 0.5 : 1,
              }}
            >
              {action}
            </button>
          ))}
        </div>
      </div>

      {/* Stats Row */}
      <div style={{
        display: 'flex',
        gap: '16px',
        padding: '8px 16px',
        borderBottom: '1px solid #374151',
        fontSize: '12px',
        color: '#9ca3af',
      }}>
        <span>Submitted: <strong style={{ color: '#f59e0b' }}>{route.totalSubmitted.toLocaleString()}</strong></span>
        <span>Delivered: <strong style={{ color: '#22c55e' }}>{route.totalDelivered.toLocaleString()}</strong></span>
        <span>Failed: <strong style={{ color: '#ef4444' }}>{route.totalFailed.toLocaleString()}</strong></span>
        <span>Rate: <strong style={{ color: '#60a5fa' }}>{totalRate}%</strong></span>
      </div>

      {/* Sessions */}
      <div style={{ overflowX: 'auto' }}>
        <table style={{ width: '100%', borderCollapse: 'collapse', fontSize: '13px' }}>
          <thead>
            <tr style={{ color: '#9ca3af', fontSize: '11px', textTransform: 'uppercase' }}>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>Session</th>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>State</th>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>Circuit</th>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>Error Rate</th>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>Response</th>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>Window</th>
              <th style={{ padding: '8px 12px', textAlign: 'left' }}>Actions</th>
            </tr>
          </thead>
          <tbody>
            {route.sessions.map(session => (
              <tr key={session.sessionId} style={{ borderBottom: '1px solid #374151' }}>
                <td style={{ padding: '8px 12px', fontSize: '12px' }}>{session.sessionId}</td>
                <td style={{ padding: '8px 12px' }}>
                  <span style={{
                    display: 'inline-block',
                    width: '8px',
                    height: '8px',
                    borderRadius: '50%',
                    background: statusColor(session.state),
                    marginRight: '6px',
                  }} />
                  {session.state}
                </td>
                <td style={{ padding: '8px 12px' }}>
                  <span style={{
                    display: 'inline-block',
                    width: '8px',
                    height: '8px',
                    borderRadius: '50%',
                    background: session.circuitState === 'open' ? '#ef4444'
                      : session.circuitState === 'half_open' ? '#f59e0b' : '#22c55e',
                    marginRight: '6px',
                  }} />
                  {session.circuitState || 'closed'}
                </td>
                <td style={{ padding: '8px 12px' }}>
                  {session.errorRate !== undefined
                    ? `${(session.errorRate * 100).toFixed(1)}%`
                    : '—'}
                </td>
                <td style={{ padding: '8px 12px' }}>{session.avgResponseTimeMs.toFixed(0)}ms</td>
                <td style={{ padding: '8px 12px' }}>{session.currentWindowUsage}</td>
                <td style={{ padding: '8px 12px' }}>
                  <button
                    onClick={() => onResetCircuit(session.sessionId)}
                    style={{
                      padding: '2px 8px',
                      borderRadius: '4px',
                      border: '1px solid #a78bfa',
                      background: 'transparent',
                      color: '#a78bfa',
                      cursor: 'pointer',
                      fontSize: '11px',
                    }}
                  >
                    Reset Circuit
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
};

// ─── Main Route Control Component ────────────────────────────────────────────

export const RouteControl: React.FC = () => {
  const [routes, setRoutes] = useState<RouteData[]>([]);
  const [overrides, setOverrides] = useState<Map<string, OverrideState>>(new Map());
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [notification, setNotification] = useState<string | null>(null);

  const baseUrl = (typeof process !== 'undefined' && process.env?.REACT_APP_API_URL)
    || 'http://localhost:8080';

  // ─── Fetch Routes ─────────────────────────────────────────────────────────

  const fetchRoutes = useCallback(async () => {
    try {
      const resp = await fetch(`${baseUrl}/api/v1/admin/routes`);
      if (!resp.ok) throw new Error('Failed to fetch routes');
      const json = await resp.json();
      setRoutes(json.data || []);
      setError(null);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, [baseUrl]);

  useEffect(() => {
    fetchRoutes();
    const interval = setInterval(fetchRoutes, 5000);
    return () => clearInterval(interval);
  }, [fetchRoutes]);

  // ─── Set Override ─────────────────────────────────────────────────────────

  const handleSetOverride = async (operator: string, action: RouteAction) => {
    setOverrides(prev => {
      const next = new Map(prev);
      next.set(operator, { operator, action, loading: true });
      return next;
    });

    try {
      const resp = await fetch(`${baseUrl}/api/v1/admin/routes/override`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ operator, action }),
      });

      if (!resp.ok) throw new Error('Failed to set override');

      setOverrides(prev => {
        const next = new Map(prev);
        next.set(operator, { operator, action, loading: false });
        return next;
      });

      showNotification(`Route ${operator} set to ${action}`);
    } catch (err: any) {
      setOverrides(prev => {
        const next = new Map(prev);
        next.delete(operator);
        return next;
      });
      showNotification(`Failed: ${err.message}`, true);
    }
  };

  // ─── Reset Circuit ────────────────────────────────────────────────────────

  const handleResetCircuit = async (sessionId: string) => {
    try {
      const resp = await fetch(`${baseUrl}/api/v1/admin/circuits/${sessionId}/reset`, {
        method: 'POST',
      });

      if (!resp.ok) throw new Error('Failed to reset circuit');

      showNotification(`Circuit ${sessionId} reset`);
      fetchRoutes(); // Refresh immediately
    } catch (err: any) {
      showNotification(`Failed: ${err.message}`, true);
    }
  };

  // ─── Notification ─────────────────────────────────────────────────────────

  const showNotification = (message: string, isError: boolean = false) => {
    setNotification(message);
    setTimeout(() => setNotification(null), 3000);
  };

  // ─── Render ───────────────────────────────────────────────────────────────

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
        <h1 style={{ fontSize: '24px', fontWeight: 700, margin: 0 }}>Route Control</h1>
        <p style={{ color: '#6b7280', fontSize: '14px', margin: '4px 0 0 0' }}>
          Manage SMPP session routing and circuit breakers
        </p>
      </div>

      {/* Notification */}
      {notification && (
        <div style={{
          position: 'fixed',
          top: '16px',
          right: '16px',
          padding: '12px 20px',
          borderRadius: '8px',
          background: notification.startsWith('Failed') ? '#7f1d1d' : '#065f46',
          border: `1px solid ${notification.startsWith('Failed') ? '#dc2626' : '#22c55e'}`,
          color: notification.startsWith('Failed') ? '#fca5a5' : '#a7f3d0',
          fontSize: '14px',
          zIndex: 1000,
          boxShadow: '0 4px 12px rgba(0,0,0,0.3)',
        }}>
          {notification}
        </div>
      )}

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

      {/* Loading */}
      {loading ? (
        <div style={{ textAlign: 'center', color: '#6b7280', padding: '48px' }}>
          Loading route data...
        </div>
      ) : routes.length === 0 ? (
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
          <OperatorRouteCard
            key={route.operator}
            route={route}
            currentOverride={overrides.get(route.operator) || null}
            onSetOverride={handleSetOverride}
            onResetCircuit={handleResetCircuit}
          />
        ))
      )}
    </div>
  );
};

export default RouteControl;