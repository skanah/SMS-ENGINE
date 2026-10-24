/**
 * CampaignsList.tsx — Campaign List & Management Component
 *
 * Displays a list of SMS campaigns with real-time progress tracking.
 * Features:
 *   - Campaign status badges (draft, scheduled, sending, completed, paused, cancelled)
 *   - Real-time delivery progress bars
 *   - Per-campaign metrics (sent, delivered, failed)
 *   - Schedule time display
 *   - Create new campaign button
 */

import React, { useState, useEffect, useCallback } from 'react';

// ─── Types ───────────────────────────────────────────────────────────────────

interface Campaign {
  id: string;
  name: string;
  type: 'otp' | 'bulk' | 'transactional';
  status: 'draft' | 'scheduled' | 'sending' | 'completed' | 'paused' | 'cancelled';
  totalRecipients: number;
  sentCount: number;
  deliveredCount: number;
  failedCount: number;
  scheduledAt: string | null;
  createdAt: string;
}

// ─── Status Badge Colors ────────────────────────────────────────────────────

const statusColors: Record<string, string> = {
  draft: '#6b7280',
  scheduled: '#f59e0b',
  sending: '#3b82f6',
  completed: '#22c55e',
  paused: '#a78bfa',
  cancelled: '#ef4444',
};

// ─── Progress Bar ────────────────────────────────────────────────────────────

const ProgressBar: React.FC<{ sent: number; delivered: number; failed: number; total: number }> = ({
  sent, delivered, failed, total,
}) => {
  if (total === 0) return null;

  const deliveredPct = (delivered / total) * 100;
  const failedPct = (failed / total) * 100;
  const sentPct = ((sent - delivered - failed) / total) * 100;

  return (
    <div style={{
      width: '100%',
      height: '6px',
      borderRadius: '3px',
      background: '#374151',
      overflow: 'hidden',
      display: 'flex',
    }}>
      {deliveredPct > 0 && (
        <div style={{ width: `${deliveredPct}%`, background: '#22c55e', transition: 'width 0.5s' }} />
      )}
      {sentPct > 0 && (
        <div style={{ width: `${sentPct}%`, background: '#3b82f6', transition: 'width 0.5s' }} />
      )}
      {failedPct > 0 && (
        <div style={{ width: `${failedPct}%`, background: '#ef4444', transition: 'width 0.5s' }} />
      )}
    </div>
  );
};

// ─── Campaign Card ───────────────────────────────────────────────────────────

const CampaignCard: React.FC<{ campaign: Campaign }> = ({ campaign }) => {
  const progress = campaign.totalRecipients > 0
    ? ((campaign.deliveredCount / campaign.totalRecipients) * 100).toFixed(1)
    : '0.0';

  return (
    <div style={{
      background: '#111827',
      borderRadius: '8px',
      border: '1px solid #374151',
      padding: '16px',
      marginBottom: '12px',
    }}>
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'flex-start', marginBottom: '12px' }}>
        <div>
          <h3 style={{ margin: 0, fontSize: '15px', fontWeight: 600, color: '#ffffff' }}>
            {campaign.name}
          </h3>
          <div style={{ color: '#6b7280', fontSize: '12px', marginTop: '2px' }}>
            {campaign.type.toUpperCase()} · {new Date(campaign.createdAt).toLocaleDateString()}
          </div>
        </div>
        <span style={{
          display: 'inline-block',
          padding: '2px 8px',
          borderRadius: '4px',
          fontSize: '11px',
          fontWeight: 600,
          textTransform: 'uppercase',
          background: statusColors[campaign.status] + '22',
          color: statusColors[campaign.status],
          border: `1px solid ${statusColors[campaign.status]}`,
        }}>
          {campaign.status}
        </span>
      </div>

      {/* Progress Bar */}
      <ProgressBar
        sent={campaign.sentCount}
        delivered={campaign.deliveredCount}
        failed={campaign.failedCount}
        total={campaign.totalRecipients}
      />

      {/* Metrics */}
      <div style={{
        display: 'flex',
        justifyContent: 'space-between',
        marginTop: '8px',
        fontSize: '12px',
        color: '#9ca3af',
      }}>
        <span>Total: <strong style={{ color: '#ffffff' }}>{campaign.totalRecipients.toLocaleString()}</strong></span>
        <span>Sent: <strong style={{ color: '#3b82f6' }}>{campaign.sentCount.toLocaleString()}</strong></span>
        <span>Delivered: <strong style={{ color: '#22c55e' }}>{campaign.deliveredCount.toLocaleString()}</strong></span>
        <span>Failed: <strong style={{ color: '#ef4444' }}>{campaign.failedCount.toLocaleString()}</strong></span>
        <span>Progress: <strong style={{ color: '#f59e0b' }}>{progress}%</strong></span>
      </div>

      {campaign.scheduledAt && (
        <div style={{ color: '#6b7280', fontSize: '11px', marginTop: '8px' }}>
          Scheduled: {new Date(campaign.scheduledAt).toLocaleString()}
        </div>
      )}
    </div>
  );
};

// ─── Campaigns List ──────────────────────────────────────────────────────────

export const CampaignsList: React.FC = () => {
  const [campaigns, setCampaigns] = useState<Campaign[]>([]);
  const [loading, setLoading] = useState<boolean>(true);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState<string>('all');

  const baseUrl = (typeof process !== 'undefined' && process.env?.REACT_APP_API_URL)
    || 'http://localhost:8080';

  const fetchCampaigns = useCallback(async () => {
    try {
      // TODO: Implement campaigns API endpoint
      // For now, use mock data
      const mockCampaigns: Campaign[] = [
        {
          id: '1',
          name: 'Welcome Campaign Q2',
          type: 'bulk',
          status: 'sending',
          totalRecipients: 50000,
          sentCount: 34200,
          deliveredCount: 29800,
          failedCount: 1200,
          scheduledAt: null,
          createdAt: '2026-06-15T08:00:00Z',
        },
        {
          id: '2',
          name: 'OTP Verification Test',
          type: 'otp',
          status: 'completed',
          totalRecipients: 10000,
          sentCount: 10000,
          deliveredCount: 9850,
          failedCount: 150,
          scheduledAt: null,
          createdAt: '2026-06-14T10:30:00Z',
        },
        {
          id: '3',
          name: 'Promo June 2026',
          type: 'bulk',
          status: 'draft',
          totalRecipients: 100000,
          sentCount: 0,
          deliveredCount: 0,
          failedCount: 0,
          scheduledAt: '2026-07-01T09:00:00Z',
          createdAt: '2026-06-20T14:00:00Z',
        },
      ];

      setCampaigns(mockCampaigns);
      setError(null);
    } catch (err: any) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchCampaigns();
    const interval = setInterval(fetchCampaigns, 10000);
    return () => clearInterval(interval);
  }, [fetchCampaigns]);

  // ─── Filter ───────────────────────────────────────────────────────────────

  const filteredCampaigns = filter === 'all'
    ? campaigns
    : campaigns.filter(c => c.status === filter);

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
      <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '24px' }}>
        <div>
          <h1 style={{ fontSize: '24px', fontWeight: 700, margin: 0 }}>Campaigns</h1>
          <p style={{ color: '#6b7280', fontSize: '14px', margin: '4px 0 0 0' }}>
            Manage your SMS campaigns
          </p>
        </div>
        <button style={{
          padding: '10px 20px',
          borderRadius: '8px',
          border: 'none',
          background: '#3b82f6',
          color: '#ffffff',
          fontSize: '14px',
          fontWeight: 600,
          cursor: 'pointer',
        }}>
          + New Campaign
        </button>
      </div>

      {/* Error */}
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
          ⚠ {error}
        </div>
      )}

      {/* Filter Tabs */}
      <div style={{ display: 'flex', gap: '8px', marginBottom: '16px', flexWrap: 'wrap' }}>
        {['all', 'draft', 'scheduled', 'sending', 'completed', 'paused', 'cancelled'].map(status => (
          <button
            key={status}
            onClick={() => setFilter(status)}
            style={{
              padding: '6px 14px',
              borderRadius: '6px',
              border: `1px solid ${filter === status ? statusColors[status] || '#3b82f6' : '#374151'}`,
              background: filter === status ? (statusColors[status] || '#3b82f6') + '22' : 'transparent',
              color: filter === status ? (statusColors[status] || '#3b82f6') : '#9ca3af',
              cursor: 'pointer',
              fontSize: '12px',
              fontWeight: 500,
              textTransform: 'capitalize',
            }}
          >
            {status}
          </button>
        ))}
      </div>

      {/* Loading */}
      {loading ? (
        <div style={{ textAlign: 'center', color: '#6b7280', padding: '48px' }}>
          Loading campaigns...
        </div>
      ) : filteredCampaigns.length === 0 ? (
        <div style={{
          background: '#1f2937',
          borderRadius: '8px',
          padding: '32px',
          textAlign: 'center',
          color: '#6b7280',
          border: '1px dashed #374151',
        }}>
          No campaigns found. Create your first campaign to get started.
        </div>
      ) : (
        filteredCampaigns.map(campaign => (
          <CampaignCard key={campaign.id} campaign={campaign} />
        ))
      )}
    </div>
  );
};

export default CampaignsList;