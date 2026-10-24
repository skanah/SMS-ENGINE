/**
 * gsmProcessor.ts — GSM 7-Bit / UCS-2 Message Concatenation Engine
 *
 * Handles text encoding, segmentation, and UDH (User Data Header) injection
 * for the Nigerian SMS Gateway. Supports:
 *
 *   - GSM 7-bit default alphabet: 160 chars single, 153 chars per segment (multi-part)
 *   - UCS-2 (Unicode): 70 chars single, 67 chars per segment (multi-part)
 *   - Automatic detection of regional African characters/accents
 *   - 6-byte UDH generation for concatenated SMS tracking
 *   - Bit-packing for GSM 7-bit (8-to-7 bit unpacking)
 *
 * Performance: O(n) single-pass encoding, no unnecessary Buffer allocations.
 */

import { GSM_ENCODING } from './config';
import { MessageSegment } from './types';
import { encodeGsm7Packed, encodeToSeptets } from './lib/gsm7BitPacking';

// ─── GSM 7-Bit Default Alphabet Character Map ──────────────────────────────
//
// The GSM 7-bit default alphabet (3GPP TS 23.038) maps characters to 7-bit values.
// Extended characters (^, {, }, \, [, ~, |, €) use the escape code 0x1B.

const GSM7_CHAR_MAP: Record<string, number> = {
  '@': 0x00, '£': 0x01, '$': 0x02, '¥': 0x03, 'è': 0x04, 'é': 0x05, 'ù': 0x06, 'ì': 0x07,
  'ò': 0x08, 'Ç': 0x09, '\n': 0x0A, 'Ø': 0x0B, 'ø': 0x0C, '\r': 0x0D, 'Å': 0x0E, 'å': 0x0F,
  'Δ': 0x10, '_': 0x11, 'Φ': 0x12, 'Γ': 0x13, 'Λ': 0x14, 'Ω': 0x15, 'Π': 0x16, 'Ψ': 0x17,
  'Σ': 0x18, 'Θ': 0x19, 'Ξ': 0x1A, '\x1B': 0x1B, 'Æ': 0x1C, 'æ': 0x1D, 'ß': 0x1E, 'É': 0x1F,
  ' ': 0x20, '!': 0x21, '"': 0x22, '#': 0x23, '¤': 0x24, '%': 0x25, '&': 0x26, '\'': 0x27,
  '(': 0x28, ')': 0x29, '*': 0x2A, '+': 0x2B, ',': 0x2C, '-': 0x2D, '.': 0x2E, '/': 0x2F,
  '0': 0x30, '1': 0x31, '2': 0x32, '3': 0x33, '4': 0x34, '5': 0x35, '6': 0x36, '7': 0x37,
  '8': 0x38, '9': 0x39, ':': 0x3A, ';': 0x3B, '<': 0x3C, '=': 0x3D, '>': 0x3E, '?': 0x3F,
  '¡': 0x40, 'A': 0x41, 'B': 0x42, 'C': 0x43, 'D': 0x44, 'E': 0x45, 'F': 0x46, 'G': 0x47,
  'H': 0x48, 'I': 0x49, 'J': 0x4A, 'K': 0x4B, 'L': 0x4C, 'M': 0x4D, 'N': 0x4E, 'O': 0x4F,
  'P': 0x50, 'Q': 0x51, 'R': 0x52, 'S': 0x53, 'T': 0x54, 'U': 0x55, 'V': 0x56, 'W': 0x57,
  'X': 0x58, 'Y': 0x59, 'Z': 0x5A, 'Ä': 0x5B, 'Ö': 0x5C, 'Ñ': 0x5D, 'Ü': 0x5E, '§': 0x5F,
  '¿': 0x60, 'a': 0x61, 'b': 0x62, 'c': 0x63, 'd': 0x64, 'e': 0x65, 'f': 0x66, 'g': 0x67,
  'h': 0x68, 'i': 0x69, 'j': 0x6A, 'k': 0x6B, 'l': 0x6C, 'm': 0x6D, 'n': 0x6E, 'o': 0x6F,
  'p': 0x70, 'q': 0x71, 'r': 0x72, 's': 0x73, 't': 0x74, 'u': 0x75, 'v': 0x76, 'w': 0x77,
  'x': 0x78, 'y': 0x79, 'z': 0x7A, 'ä': 0x7B, 'ö': 0x7C, 'ñ': 0x7D, 'ü': 0x7E, 'à': 0x7F,
};

// GSM 7-bit extension table (characters prefixed with escape 0x1B)
const GSM7_EXT_MAP: Record<string, number> = {
  '\n': 0x0A,  // Form feed
  '^': 0x14,
  '{': 0x28,
  '}': 0x29,
  '\\': 0x2F,
  '[': 0x3C,
  '~': 0x3D,
  ']': 0x3E,
  '|': 0x40,
  '€': 0x65,
};

// ─── Character Detection ────────────────────────────────────────────────────

/**
 * Detects if a string contains characters outside the GSM 7-bit default alphabet.
 * If so, the message must be encoded as UCS-2 (data_coding = 0x08).
 *
 * This is critical for Nigerian use cases where names may contain accented
 * characters (e.g., "Olúwafẹ́mi", "Chimamanda") or special punctuation.
 */
export function requiresUcs2(text: string): boolean {
  for (const char of text) {
    if (char in GSM7_CHAR_MAP) continue;
    if (char in GSM7_EXT_MAP) continue;
    // Check if it's a basic ASCII printable that GSM7 covers
    const code = char.charCodeAt(0);
    if (code >= 32 && code <= 126) continue; // ASCII printable
    return true; // Non-GSM7 character found
  }
  return false;
}

// ─── GSM 7-Bit Encoding ────────────────────────────────────────────────────

/**
 * Encodes a string into GSM 7-bit default alphabet bytes with correct bit-packing.
 * Delegates to the corrected algorithm in lib/gsm7BitPacking.ts.
 *
 * @param text - The text to encode
 * @returns Buffer containing the packed GSM 7-bit data
 */
export function encodeGsm7(text: string): Buffer {
  return encodeGsm7Packed(text);
}

// ─── UCS-2 Encoding ────────────────────────────────────────────────────────

/**
 * Encodes a string into UCS-2 (UTF-16) for data_coding = 0x08.
 * Each character becomes 2 bytes (BMP only; supplementary planes not supported
 * by SMS).
 *
 * @param text - The text to encode
 * @returns Buffer containing UCS-2 encoded data
 */
export function encodeUcs2(text: string): Buffer {
  // Encode as UTF-16LE (UCS-2 is a subset of UTF-16 for BMP characters)
  const buffer = Buffer.alloc(text.length * 2);
  for (let i = 0; i < text.length; i++) {
    const code = text.charCodeAt(i);
    buffer.writeUInt16LE(code, i * 2);
  }
  return buffer;
}

// ─── UDH Generation ────────────────────────────────────────────────────────

/**
 * Generates a 6-byte UDH (User Data Header) for concatenated SMS.
 *
 * Format:
 *   Byte 0: UDH Length (0x05) — 5 bytes follow
 *   Byte 1: IE Identifier (0x00) — Concatenated SMS, 8-bit reference
 *   Byte 2: IE Data Length (0x03) — 3 bytes follow
 *   Byte 3: Reference Number (0x00-0xFF) — same for all parts
 *   Byte 4: Total Parts (1-255)
 *   Byte 5: Part Number (1-255)
 *
 * @param referenceNumber - Unique reference for this concatenated message
 * @param totalParts - Total number of segments
 * @param partNumber - Current segment number (1-based)
 * @returns 6-byte Buffer containing the UDH
 */
export function generateUdh(
  referenceNumber: number,
  totalParts: number,
  partNumber: number
): Buffer {
  const udh = Buffer.alloc(6);
  udh[0] = 0x05;                                     // UDH Length
  udh[1] = GSM_ENCODING.UDH_IEI_CONCAT_8BIT;         // IEI: 8-bit concat
  udh[2] = 0x03;                                     // IE Data Length
  udh[3] = referenceNumber & 0xFF;                   // Reference number
  udh[4] = totalParts & 0xFF;                        // Total parts
  udh[5] = partNumber & 0xFF;                        // Part number
  return udh;
}

// ─── Message Segmentation ──────────────────────────────────────────────────

/**
 * Splits a message into segments and generates the appropriate encoding.
 *
 * @param text - The message text
 * @param forceUcs2 - If true, force UCS-2 encoding (otherwise auto-detect)
 * @returns Object with segments, dataCoding, and totalSegments
 */
export function processMessage(
  text: string,
  forceUcs2?: boolean
): {
  segments: MessageSegment[];
  dataCoding: number;
  totalSegments: number;
} {
  const useUcs2 = forceUcs2 ?? requiresUcs2(text);
  const dataCoding = useUcs2
    ? GSM_ENCODING.DATA_CODING_UCS2
    : GSM_ENCODING.DATA_CODING_GSM7;

  const maxPerSegment = useUcs2
    ? GSM_ENCODING.UCS2_SINGLE_MAX
    : GSM_ENCODING.GSM7_SINGLE_MAX;

  const maxPerSegmentConcat = useUcs2
    ? GSM_ENCODING.UCS2_CONCAT_MAX
    : GSM_ENCODING.GSM7_CONCAT_MAX;

  // Determine if concatenation is needed
  const charLimit = text.length > maxPerSegment ? maxPerSegmentConcat : maxPerSegment;
  const totalSegments = Math.ceil(text.length / charLimit);

  const segments: MessageSegment[] = [];
  const referenceNumber = Math.floor(Math.random() * 256); // 0-255

  for (let i = 0; i < totalSegments; i++) {
    const start = i * charLimit;
    const end = Math.min(start + charLimit, text.length);
    const chunk = text.slice(start, end);

    // Encode the chunk
    const encoded = useUcs2 ? encodeUcs2(chunk) : encodeGsm7(chunk);

    // Generate UDH if concatenated
    const udh = totalSegments > 1
      ? generateUdh(referenceNumber, totalSegments, i + 1)
      : null;

    segments.push({
      sequenceNumber: i + 1,
      totalParts: totalSegments,
      udh,
      payload: encoded,
      dataCoding,
      esmClass: udh ? GSM_ENCODING.ESM_CLASS_UDH : GSM_ENCODING.ESM_CLASS_NORMAL,
    });
  }

  return { segments, dataCoding, totalSegments };
}

// ─── Utility: Count segments without encoding ──────────────────────────────

/**
 * Quickly counts how many SMS segments a message will require.
 * Useful for pricing previews in the campaign builder UI.
 *
 * @param text - The message text
 * @returns Object with segment count, encoding type, and per-segment char limits
 */
export function countSegments(text: string): {
  totalSegments: number;
  encoding: 'gsm7' | 'ucs2';
  charsPerSegment: number;
  totalChars: number;
} {
  const useUcs2 = requiresUcs2(text);
  const maxPerSegment = useUcs2
    ? GSM_ENCODING.UCS2_SINGLE_MAX
    : GSM_ENCODING.GSM7_SINGLE_MAX;
  const maxPerSegmentConcat = useUcs2
    ? GSM_ENCODING.UCS2_CONCAT_MAX
    : GSM_ENCODING.GSM7_CONCAT_MAX;

  const charLimit = text.length > maxPerSegment ? maxPerSegmentConcat : maxPerSegment;
  const totalSegments = Math.ceil(text.length / charLimit);

  return {
    totalSegments,
    encoding: useUcs2 ? 'ucs2' : 'gsm7',
    charsPerSegment: charLimit,
    totalChars: text.length,
  };
}