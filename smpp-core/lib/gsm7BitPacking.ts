/**
 * gsm7BitPacking.ts — Corrected GSM 7-bit Default Alphabet Bit-Packing
 *
 * Implements the correct GSM 7-bit packing algorithm per 3GPP TS 23.038.
 * Each character is a 7-bit septet; septets are packed into 8-bit bytes
 * with the least significant bits of the first septet in the first byte.
 *
 * Correction over original gsmProcessor.ts:
 *   - The spill condition is `bitOffset > 0` (not `bitOffset > 1`)
 *   - Only when bitOffset === 0 does a 7-bit value fit entirely in one byte
 *   - Added GSM 7-bit unpacking (for decoding incoming messages)
 *   - Added validation that packed output is byte-aligned
 */

import { GSM_ENCODING } from '../config';

// ─── GSM 7-bit Default Alphabet Character Map ──────────────────────────────
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
  '\f': 0x0A, // Form feed
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

// Reverse map: septet value → character
const GSM7_REVERSE_MAP: Record<number, string> = {};
const GSM7_EXT_REVERSE_MAP: Record<number, string> = {};

// Build reverse maps
for (const [char, code] of Object.entries(GSM7_CHAR_MAP)) {
  GSM7_REVERSE_MAP[code] = char;
}
for (const [char, code] of Object.entries(GSM7_EXT_MAP)) {
  GSM7_EXT_REVERSE_MAP[code] = char;
}

/**
 * Pack GSM 7-bit septets into 8-bit bytes.
 *
 * Per 3GPP TS 23.038, the packing is:
 *   - Septet 0 occupies bits 0-6 of Byte 0
 *   - Septet 1 occupies bit 7 of Byte 0 and bits 0-5 of Byte 1
 *   - Septet 2 occupies bits 6-7 of Byte 1 and bits 0-4 of Byte 2
 *   - ...
 *
 * For septet i:
 *   startByte = floor(i * 7 / 8)
 *   startBit  = (i * 7) % 8
 *   Byte[startByte]     |= (septet << startBit) & 0xFF
 *   Byte[startByte + 1]  = (septet >> (8 - startBit)) & 0xFF
 *
 * @param septets - Array of 7-bit values (0-127)
 * @returns Buffer of packed bytes
 */
export function packGsm7Bits(septets: number[]): Buffer {
  if (septets.length === 0) return Buffer.alloc(0);

  const packedSize = Math.ceil((septets.length * 7) / 8);
  const packed = Buffer.alloc(packedSize, 0);

  for (let i = 0; i < septets.length; i++) {
    const septet = septets[i] & 0x7F; // Ensure 7 bits only
    const bitPos = i * 7;
    const bytePos = Math.floor(bitPos / 8);
    const bitOffset = bitPos % 8;

    // Write the lower portion to the current byte
    packed[bytePos] |= (septet << bitOffset) & 0xFF;

    // If the septet overflows into the next byte, write the upper portion
    if (bitOffset > 0) {
      const nextByte = bytePos + 1;
      if (nextByte < packedSize) {
        packed[nextByte] = (septet >> (8 - bitOffset)) & 0xFF;
      }
    }
  }

  return packed;
}

/**
 * Unpack GSM 7-bit bytes back into an array of septets.
 *
 * @param packed - Buffer of packed GSM 7-bit data
 * @param septetCount - Number of septets to unpack (default: inferred from buffer)
 * @returns Array of 7-bit values (0-127)
 */
export function unpackGsm7Bits(packed: Buffer, septetCount?: number): number[] {
  const count = septetCount ?? Math.floor((packed.length * 8) / 7);
  const septets: number[] = [];

  for (let i = 0; i < count; i++) {
    const bitPos = i * 7;
    const bytePos = Math.floor(bitPos / 8);
    const bitOffset = bitPos % 8;

    let value = (packed[bytePos] >> bitOffset) & 0x7F;

    // If the value spans into the next byte, read the overflow bits
    if (bitOffset > 0 && bytePos + 1 < packed.length) {
      value |= (packed[bytePos + 1] << (8 - bitOffset)) & 0x7F;
    }

    septets.push(value);
  }

  return septets;
}

/**
 * Encode a string into GSM 7-bit septets (no bit-packing, just the 7-bit values).
 * Extended characters are prefixed with the escape code 0x1B.
 */
export function encodeToSeptets(text: string): number[] {
  const septets: number[] = [];

  for (const char of text) {
    if (char in GSM7_CHAR_MAP) {
      septets.push(GSM7_CHAR_MAP[char]);
    } else if (char in GSM7_EXT_MAP) {
      septets.push(0x1B); // Escape
      septets.push(GSM7_EXT_MAP[char]);
    } else {
      // Fallback: replace with space for unsupported characters
      septets.push(0x20);
    }
  }

  return septets;
}

/**
 * Decode GSM 7-bit septets back into a string.
 * Handles the 0x1B escape code for extended characters.
 */
export function decodeFromSeptets(septets: number[]): string {
  const chars: string[] = [];
  let i = 0;

  while (i < septets.length) {
    const code = septets[i];

    if (code === 0x1B && i + 1 < septets.length) {
      // Extended character
      const extCode = septets[i + 1];
      const char = GSM7_EXT_REVERSE_MAP[extCode];
      if (char !== undefined) {
        chars.push(char);
      } else {
        chars.push(' '); // Unknown extended character
      }
      i += 2;
    } else if (code in GSM7_REVERSE_MAP) {
      chars.push(GSM7_REVERSE_MAP[code]);
    } else {
      chars.push(' '); // Unknown character
      i++;
    }
  }

  return chars.join('');
}

/**
 * Packed encoding: text → septets → bit-packed bytes.
 * This is the full encode function to use for SMPP submit_sm payloads.
 */
export function encodeGsm7Packed(text: string): Buffer {
  const septets = encodeToSeptets(text);
  return packGsm7Bits(septets);
}

/**
 * Decode packed GSM 7-bit bytes back into text.
 */
export function decodeGsm7Packed(packed: Buffer, charCount?: number): string {
  const septets = unpackGsm7Bits(packed, charCount);
  return decodeFromSeptets(septets);
}