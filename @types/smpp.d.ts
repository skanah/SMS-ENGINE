/**
 * Minimal TypeScript declarations for the `smpp` module (v0.6.0-rc.4).
 *
 * This covers the SMPP 3.4 PDU types, Session class, and connection function
 * used by the SMS Gateway Core's smppSessionManager.
 */

declare module 'smpp' {
  import { EventEmitter } from 'events';
  import { Socket } from 'net';

  // ─── PDU Types ──────────────────────────────────────────────────────────

  interface Pdu {
    command: string;
    command_status: number;
    sequence_number: number;
    message_id?: string;
    system_id?: string;
    source_addr?: string;
    destination_addr?: string;
    short_message?: Buffer | string;
    data_coding?: number;
    esm_class?: number;
    registered_delivery?: number;
    priority_flag?: number;
    validity_period?: string;
    schedule_delivery_time?: string;
    error_code?: string;
    stat?: string;
    done_date?: string;
    submit_date?: string;
    [key: string]: any;
  }

  type PduCallback = (pdu: Pdu) => void;

  // ─── Session Class ─────────────────────────────────────────────────────

  interface SessionOptions {
    url: string;
    auto_enquire_link?: boolean;
    debug?: boolean;
  }

  interface BindTransceiverParams {
    system_id: string;
    password: string;
    system_type?: string;
    interface_version?: number;
    addr_ton?: number;
    addr_npi?: number;
    address_range?: string;
  }

  interface BindTransmitterParams {
    system_id: string;
    password: string;
    system_type?: string;
    interface_version?: number;
    addr_ton?: number;
    addr_npi?: number;
    address_range?: string;
  }

  interface BindReceiverParams {
    system_id: string;
    password: string;
    system_type?: string;
    interface_version?: number;
    addr_ton?: number;
    addr_npi?: number;
    address_range?: string;
  }

  interface SubmitSmParams {
    source_addr: string;
    destination_addr: string;
    short_message: Buffer | string;
    data_coding?: number;
    esm_class?: number;
    registered_delivery?: number;
    priority_flag?: number;
    validity_period?: string;
    schedule_delivery_time?: string;
    sequence_number?: number;
    replace_if_present_flag?: number;
    service_type?: string;
    source_addr_ton?: number;
    source_addr_npi?: number;
    dest_addr_ton?: number;
    dest_addr_npi?: number;
    [key: string]: any;
  }

  interface DeliverSmParams {
    [key: string]: any;
  }

  interface EnquireLinkParams {
    sequence_number?: number;
  }

  interface GenericNackParams {
    command_status: number;
    sequence_number?: number;
  }

  interface UnbindParams {
    sequence_number?: number;
  }

  class Session extends EventEmitter {
    constructor(socket: Socket);
    
    // Bind methods
    bind_transceiver(params: BindTransceiverParams, callback: PduCallback): void;
    bind_transmitter(params: BindTransmitterParams, callback: PduCallback): void;
    bind_receiver(params: BindReceiverParams, callback: PduCallback): void;
    
    // Submit
    submit_sm(params: SubmitSmParams, callback: PduCallback): void;
    
    // Deliver
    deliver_sm_resp(params: any): void;
    
    // Enquire Link
    enquire_link(params: EnquireLinkParams, callback: PduCallback): void;
    
    // Unbind
    unbind(callback: PduCallback): void;
    unbind(params: UnbindParams, callback: PduCallback): void;
    
    // Generic
    send(pdu: any): void;
    close(): void;
    destroy(): void;
    
    // Events
    on(event: 'connect', listener: () => void): this;
    on(event: 'close', listener: () => void): this;
    on(event: 'error', listener: (err: Error) => void): this;
    on(event: 'timeout', listener: () => void): this;
    on(event: 'pdu', listener: (pdu: Pdu) => void): this;
    on(event: string, listener: (...args: any[]) => void): this;
    
    // PDU response helpers
    pdu: any;
  }

  // ─── Connection Factory ────────────────────────────────────────────────

  export function connect(options: SessionOptions): Session;
  export function connect(port: number, host: string): Session;

  // ─── PDU Factory ──────────────────────────────────────────────────────

  export function PDU(command: string, params?: any): any;
  export function parsePDU(data: Buffer): Pdu;
}