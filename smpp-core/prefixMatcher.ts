/**
 * prefixMatcher.ts — High-Performance Nigerian Telco Prefix Lookup
 *
 * Uses a Trie (prefix tree) data structure to resolve phone number prefixes
 * to their corresponding Nigerian network operator in O(L) time where L is
 * the prefix length (typically 6-7 digits).
 *
 * Performance characteristics:
 *   - Build time: O(N * L) for N prefixes of average length L
 *   - Lookup time: O(L) — sub-microsecond per lookup
 *   - Memory: ~2KB per 100 prefixes (negligible)
 *
 * The Trie is built once at startup from the PREFIX_TABLE in config.ts.
 * It is then frozen and shared across all worker threads.
 */

import { TelcoOperator } from './types';
import { PREFIX_TABLE } from './config';

// ─── Trie Node ──────────────────────────────────────────────────────────────

interface TrieNode {
  /** Child nodes keyed by digit character (0-9) */
  children: Map<string, TrieNode>;
  /** If this node represents the end of a known prefix, the operator */
  operator: TelcoOperator | null;
}

// ─── Trie Builder ───────────────────────────────────────────────────────────

function buildTrie(): TrieNode {
  const root: TrieNode = {
    children: new Map(),
    operator: null,
  };

  for (const { prefix, operator } of PREFIX_TABLE) {
    let current = root;

    for (const char of prefix) {
      let child = current.children.get(char);
      if (!child) {
        child = { children: new Map(), operator: null };
        current.children.set(char, child);
      }
      current = child;
    }

    // Mark the terminal node with the operator.
    // If multiple prefixes share a node (e.g., 234701 for both Airtel and Glo),
    // the last one in the table wins. The table is ordered by primary operator.
    current.operator = operator;
  }

  return root;
}

// Build the Trie once at module load time
const ROOT: TrieNode = buildTrie();

// ─── Phone Number Normalization ─────────────────────────────────────────────

/**
 * Normalizes a phone number to E.164 format (without leading +).
 *
 * Handles:
 *   - +2348031234567 → 2348031234567
 *   - 2348031234567  → 2348031234567
 *   - 08031234567    → 2348031234567
 *   - 8031234567     → 2348031234567
 *   - 01123456789    → (non-Nigerian, returned as-is after stripping leading 0)
 *
 * Returns null if the number is invalid (empty, non-digit after normalization).
 */
export function normalizePhoneNumber(raw: string): string | null {
  if (!raw || raw.length < 5) return null;

  // Strip all non-digit characters (spaces, dashes, parentheses, etc.)
  let digits = raw.replace(/\D/g, '');

  if (digits.length === 0) return null;

  // Handle Nigerian number formats
  if (digits.startsWith('234') && digits.length >= 10 && digits.length <= 14) {
    // Already in E.164 format without +
    return digits;
  }

  if (digits.startsWith('0') && digits.length === 11) {
    // Local format: 08031234567 → 2348031234567
    return '234' + digits.slice(1);
  }

  if (digits.length === 10 && digits.startsWith('8')) {
    // Short format: 8031234567 → 2348031234567
    return '234' + digits;
  }

  if (digits.startsWith('+')) {
    // Strip leading + (shouldn't happen since we stripped non-digits, but safety)
    digits = digits.slice(1);
    if (digits.startsWith('234') && digits.length >= 10) {
      return digits;
    }
  }

  // Non-Nigerian or unrecognized format — return normalized digits
  return digits;
}

// ─── Operator Lookup ────────────────────────────────────────────────────────

/**
 * Resolves a normalized phone number to its Nigerian network operator.
 *
 * @param normalizedNumber - E.164 formatted number (e.g., "2348031234567")
 * @returns The TelcoOperator, or TelcoOperator.UNKNOWN if no prefix matches
 *
 * Algorithm:
 *   1. Walk the Trie character by character from the start of the number.
 *   2. Track the deepest node that has an operator assigned (longest prefix match).
 *   3. Return the operator at the deepest matching node.
 *
 * This ensures that if both "234803" and "2348031" are registered, the longer
 * match (more specific) takes priority.
 */
export function lookupOperator(normalizedNumber: string): TelcoOperator {
  if (!normalizedNumber || normalizedNumber.length < 5) {
    return TelcoOperator.UNKNOWN;
  }

  let current = ROOT;
  let lastKnownOperator: TelcoOperator = TelcoOperator.UNKNOWN;

  for (const char of normalizedNumber) {
    const child = current.children.get(char);
    if (!child) {
      // No more matching prefix characters — return the deepest match found
      break;
    }
    current = child;

    if (current.operator !== null) {
      lastKnownOperator = current.operator;
    }
  }

  return lastKnownOperator;
}

// ─── Bulk Validation ────────────────────────────────────────────────────────

/**
 * Validates and classifies a batch of phone numbers in a single pass.
 * Useful for campaign CSV processing where we need to validate thousands
 * of numbers quickly.
 *
 * @param numbers - Array of raw phone number strings
 * @returns Array of { original, normalized, operator, isValid } results
 */
export interface NumberValidationResult {
  original: string;
  normalized: string | null;
  operator: TelcoOperator;
  isValid: boolean;
}

export function validateNumberBatch(numbers: string[]): NumberValidationResult[] {
  const results: NumberValidationResult[] = [];

  for (const raw of numbers) {
    const normalized = normalizePhoneNumber(raw);

    if (!normalized) {
      results.push({
        original: raw,
        normalized: null,
        operator: TelcoOperator.UNKNOWN,
        isValid: false,
      });
      continue;
    }

    const operator = lookupOperator(normalized);
    const isValid = operator !== TelcoOperator.UNKNOWN;

    results.push({
      original: raw,
      normalized,
      operator,
      isValid,
    });
  }

  return results;
}

// ─── Utility: Get all prefixes for a given operator ─────────────────────────

/**
 * Returns all known prefixes for a specific operator.
 * Useful for Redis cache warming and admin diagnostics.
 */
export function getPrefixesForOperator(operator: TelcoOperator): string[] {
  return PREFIX_TABLE
    .filter(entry => entry.operator === operator)
    .map(entry => entry.prefix);
}

// ─── Utility: Check if a number is a valid Nigerian mobile number ───────────

/**
 * Validates that a normalized number is a valid Nigerian mobile number.
 * Nigerian mobile numbers are 13 digits (234 + 3-digit prefix + 7-digit subscriber).
 */
export function isValidNigerianMobile(normalizedNumber: string): boolean {
  if (!normalizedNumber || normalizedNumber.length !== 13) return false;
  if (!normalizedNumber.startsWith('234')) return false;
  return lookupOperator(normalizedNumber) !== TelcoOperator.UNKNOWN;
}