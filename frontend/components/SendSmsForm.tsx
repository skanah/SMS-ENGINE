/**
 * SendSmsForm.tsx — Single SMS Send Form Component
 *
 * Provides a form UI for sending individual SMS messages via the API.
 * Features:
 *   - Sender ID selection
 *   - Recipient MSISDN input
 *   - Message body with character/segment counter
 *   - Priority selection (OTP / Transactional / Bulk)
 *   - Cost estimate preview
 *   - Credit balance display
 */

import React, { useState, useCallback } from 'react';

// ─── Types ───────────────────────────────────────────────────────────────────

interface SendSmsFormData {
  sender: string;
  recipient: string;
  message: string;
  priority: 'otp' | 'transactional' | 'bulk';
  encoding: 'gsm7' | 'ucs2' | 'auto';
}

interface SendSmsResult {
  success: boolean;
  messageId?: string;
  cost?: number;
  error?: string;
}

// ─── Constants ───────────────────────────────────────────────────────────────

const SEGMENT_LIMITS = {
  gsm7: { single: 160, concat: 153 },
  ucs2: { single: 70, concat: 67 },
};

// ─── Send SMS Form ───────────────────────────────────────────────────────────

export const SendSmsForm: React.FC = () => {
  const [formData, setFormData] = useState<SendSmsFormData>({
    sender: '',
    recipient: '',
    message: '',
    priority: 'bulk',
    encoding: 'auto',
  });

  const [sending, setSending] = useState<boolean>(false);
  const [result, setResult] = useState<SendSmsResult | null>(null);
  const [balance, setBalance] = useState<number | null>(null);

  const baseUrl = (typeof process !== 'undefined' && process.env?.REACT_APP_API_URL)
    || 'http://localhost:8080';

  // ─── Segment Calculation ─────────────────────────────────────────────────

  const isUcs2 = formData.encoding === 'ucs2';
  const limits = isUcs2 ? SEGMENT_LIMITS.ucs2 : SEGMENT_LIMITS.gsm7;
  const charLimit = formData.message.length > limits.single ? limits.concat : limits.single;
  const segments = Math.ceil(formData.message.length / charLimit) || 1;

  // ─── Cost Estimate ───────────────────────────────────────────────────────

  const costPerSegment = isUcs2 ? 150 : 100;
  const estimatedCost = (costPerSegment * segments) + 50; // + overhead

  // ─── Handlers ────────────────────────────────────────────────────────────

  const handleChange = (field: keyof SendSmsFormData, value: string) => {
    setFormData(prev => ({ ...prev, [field]: value }));
    setResult(null);
  };

  const handleSubmit = useCallback(async (e: React.FormEvent) => {
    e.preventDefault();
    setSending(true);
    setResult(null);

    try {
      const resp = await fetch(`${baseUrl}/api/v1/sms/send`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(formData),
      });

      const data = await resp.json();
      setResult(data);

      if (data.balanceAfter !== undefined) {
        setBalance(data.balanceAfter);
      }
    } catch (err: any) {
      setResult({ success: false, error: err.message });
    } finally {
      setSending(false);
    }
  }, [baseUrl, formData]);

  // ─── Styles ──────────────────────────────────────────────────────────────

  const inputStyle: React.CSSProperties = {
    width: '100%',
    padding: '10px 12px',
    borderRadius: '6px',
    border: '1px solid #374151',
    background: '#1f2937',
    color: '#ffffff',
    fontSize: '14px',
    outline: 'none',
    boxSizing: 'border-box',
  };

  const labelStyle: React.CSSProperties = {
    display: 'block',
    color: '#9ca3af',
    fontSize: '12px',
    fontWeight: 600,
    textTransform: 'uppercase',
    marginBottom: '6px',
    letterSpacing: '0.05em',
  };

  // ─── Render ──────────────────────────────────────────────────────────────

  return (
    <div style={{
      background: '#0f172a',
      minHeight: '100vh',
      padding: '24px',
      fontFamily: "'Inter', -apple-system, sans-serif",
      color: '#ffffff',
    }}>
      <div style={{ maxWidth: '600px', margin: '0 auto' }}>
        {/* Header */}
        <div style={{ marginBottom: '24px' }}>
          <h1 style={{ fontSize: '24px', fontWeight: 700, margin: 0 }}>Send SMS</h1>
          <p style={{ color: '#6b7280', fontSize: '14px', margin: '4px 0 0 0' }}>
            Compose and send a single SMS message
          </p>
        </div>

        {/* Balance Display */}
        {balance !== null && (
          <div style={{
            background: '#065f46',
            border: '1px solid #22c55e',
            borderRadius: '8px',
            padding: '12px 16px',
            marginBottom: '16px',
            color: '#a7f3d0',
            fontSize: '14px',
          }}>
            Balance: {balance.toLocaleString()} credits
          </div>
        )}

        {/* Result */}
        {result && (
          <div style={{
            background: result.success ? '#065f46' : '#7f1d1d',
            border: `1px solid ${result.success ? '#22c55e' : '#dc2626'}`,
            borderRadius: '8px',
            padding: '12px 16px',
            marginBottom: '16px',
            color: result.success ? '#a7f3d0' : '#fca5a5',
            fontSize: '14px',
          }}>
            {result.success
              ? `✓ Message queued! ID: ${result.messageId} (Cost: ${result.cost})`
              : `✗ Failed: ${result.error}`}
          </div>
        )}

        {/* Form */}
        <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '16px' }}>
          {/* Sender ID */}
          <div>
            <label style={labelStyle}>Sender ID</label>
            <input
              type="text"
              value={formData.sender}
              onChange={e => handleChange('sender', e.target.value)}
              placeholder="e.g., SMSAlert"
              maxLength={11}
              required
              style={inputStyle}
            />
            <div style={{ color: '#6b7280', fontSize: '11px', marginTop: '4px' }}>
              Max 11 alphanumeric characters
            </div>
          </div>

          {/* Recipient */}
          <div>
            <label style={labelStyle}>Recipient MSISDN</label>
            <input
              type="text"
              value={formData.recipient}
              onChange={e => handleChange('recipient', e.target.value)}
              placeholder="e.g., 2348031234567"
              required
              style={inputStyle}
            />
            <div style={{ color: '#6b7280', fontSize: '11px', marginTop: '4px' }}>
              E.164 format without leading +
            </div>
          </div>

          {/* Message */}
          <div>
            <label style={labelStyle}>Message</label>
            <textarea
              value={formData.message}
              onChange={e => handleChange('message', e.target.value)}
              placeholder="Type your message here..."
              maxLength={5000}
              required
              rows={5}
              style={{ ...inputStyle, resize: 'vertical', fontFamily: 'inherit' }}
            />
            <div style={{
              display: 'flex',
              justifyContent: 'space-between',
              color: '#6b7280',
              fontSize: '11px',
              marginTop: '4px',
            }}>
              <span>{formData.message.length} chars</span>
              <span>{segments} segment{segments > 1 ? 's' : ''} · ~{estimatedCost} credits</span>
            </div>
          </div>

          {/* Priority & Encoding Row */}
          <div style={{ display: 'flex', gap: '16px' }}>
            <div style={{ flex: 1 }}>
              <label style={labelStyle}>Priority</label>
              <select
                value={formData.priority}
                onChange={e => handleChange('priority', e.target.value)}
                style={inputStyle}
              >
                <option value="bulk">Bulk</option>
                <option value="transactional">Transactional</option>
                <option value="otp">OTP</option>
              </select>
            </div>

            <div style={{ flex: 1 }}>
              <label style={labelStyle}>Encoding</label>
              <select
                value={formData.encoding}
                onChange={e => handleChange('encoding', e.target.value)}
                style={inputStyle}
              >
                <option value="auto">Auto-detect</option>
                <option value="gsm7">GSM 7-bit</option>
                <option value="ucs2">UCS-2 (Unicode)</option>
              </select>
            </div>
          </div>

          {/* Submit */}
          <button
            type="submit"
            disabled={sending}
            style={{
              padding: '12px 24px',
              borderRadius: '8px',
              border: 'none',
              background: sending ? '#374151' : '#3b82f6',
              color: '#ffffff',
              fontSize: '16px',
              fontWeight: 600,
              cursor: sending ? 'not-allowed' : 'pointer',
              transition: 'background 0.2s',
            }}
          >
            {sending ? 'Sending...' : `Send SMS (${estimatedCost} credits)`}
          </button>
        </form>
      </div>
    </div>
  );
};

export default SendSmsForm;