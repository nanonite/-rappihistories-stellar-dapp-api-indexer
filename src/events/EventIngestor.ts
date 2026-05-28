import type { DecodedIndexerEvent, StellarRpcEvent } from "./EventDecoder.js";
import { decodeIndexerEvent } from "./EventDecoder.js";
import { EventStore } from "./EventStore.js";

export interface StellarEventRpcClient {
  getEvents(request: GetEventsRequest): Promise<GetEventsResponse>;
}

export interface GetEventsRequest {
  startLedger: number;
  filters: readonly [
    {
      type: "contract";
      contractIds: readonly string[];
    },
  ];
}

export interface GetEventsResponse {
  events: readonly StellarRpcEvent[];
}

export interface EventIngestorConfig {
  contractIds: readonly string[];
  pollIntervalMs: number;
}

export interface JsonRpcStellarEventClientConfig {
  rpcUrl: string;
}

interface JsonRpcSuccess {
  result?: unknown;
}

interface JsonRpcFailure {
  error?: unknown;
}

export class JsonRpcStellarEventClient implements StellarEventRpcClient {
  readonly #rpcUrl: string;

  constructor(config: JsonRpcStellarEventClientConfig) {
    this.#rpcUrl = config.rpcUrl;
  }

  async getEvents(request: GetEventsRequest): Promise<GetEventsResponse> {
    const response = await fetch(this.#rpcUrl, {
      method: "POST",
      headers: {
        "content-type": "application/json",
      },
      body: JSON.stringify({
        jsonrpc: "2.0",
        id: "api-indexer-get-events",
        method: "getEvents",
        params: {
          ...request,
          xdrFormat: "json",
        },
      }),
    });

    if (!response.ok) {
      throw new Error(`Stellar RPC getEvents failed with HTTP ${response.status}`);
    }

    const payload: unknown = await response.json();

    if (hasJsonRpcError(payload)) {
      throw new Error(`Stellar RPC getEvents failed: ${JSON.stringify(payload.error)}`);
    }

    if (!isRecord(payload)) {
      throw new Error("Stellar RPC getEvents returned a non-object payload");
    }

    return normalizeGetEventsResponse((payload as JsonRpcSuccess).result);
  }
}

export class EventIngestor {
  readonly #client: StellarEventRpcClient;
  readonly #store: EventStore;
  readonly #config: EventIngestorConfig;
  #timer: ReturnType<typeof setTimeout> | null = null;
  #stopped = true;
  #polling = false;

  constructor(
    client: StellarEventRpcClient,
    store: EventStore,
    config: EventIngestorConfig,
  ) {
    if (config.contractIds.length !== 5) {
      throw new Error("EventIngestor requires exactly 5 contract IDs");
    }

    if (!Number.isInteger(config.pollIntervalMs) || config.pollIntervalMs <= 0) {
      throw new Error("EventIngestor pollIntervalMs must be a positive integer");
    }

    this.#client = client;
    this.#store = store;
    this.#config = config;
  }

  start(): void {
    if (!this.#stopped) {
      return;
    }

    this.#stopped = false;
    this.scheduleNextPoll(0);
  }

  stop(): void {
    this.#stopped = true;

    if (this.#timer) {
      clearTimeout(this.#timer);
      this.#timer = null;
    }
  }

  async pollOnce(): Promise<readonly DecodedIndexerEvent[]> {
    if (this.#polling) {
      return [];
    }

    this.#polling = true;

    try {
      const lastLedger = await this.#store.getLastLedger();
      const response = await this.getEventsFromNextLedger(lastLedger + 1);
      const lastObservedLedger = readLastObservedLedger(response.events);
      const decodedEvents = response.events
        .map((event) => decodeIndexerEvent(event))
        .filter((event): event is DecodedIndexerEvent => event !== null);

      await this.#store.ingestBatch(decodedEvents, lastObservedLedger);
      return decodedEvents;
    } finally {
      this.#polling = false;
    }
  }

  private scheduleNextPoll(delayMs: number): void {
    this.#timer = setTimeout(() => {
      this.pollOnce()
        .catch((error: unknown) => {
          console.error(error);
        })
        .finally(() => {
          if (!this.#stopped) {
            this.scheduleNextPoll(this.#config.pollIntervalMs);
          }
        });
    }, delayMs);
  }

  private async getEventsFromNextLedger(
    startLedger: number,
  ): Promise<GetEventsResponse> {
    try {
      return await this.#client.getEvents({
        startLedger,
        filters: [
          {
            type: "contract",
            contractIds: this.#config.contractIds,
          },
        ],
      });
    } catch (error) {
      const ledgerRange = readLedgerRangeFromRangeError(error);

      if (
        ledgerRange === null ||
        (startLedger >= ledgerRange.minimumLedger &&
          startLedger <= ledgerRange.maximumLedger)
      ) {
        throw error;
      }

      await this.#store.ingestBatch([], ledgerRange.minimumLedger - 1);
      console.warn(
        `Adjusted indexer start ledger from ${startLedger} to local RPC range ${ledgerRange.minimumLedger}-${ledgerRange.maximumLedger}`,
      );

      return this.#client.getEvents({
        startLedger: ledgerRange.minimumLedger,
        filters: [
          {
            type: "contract",
            contractIds: this.#config.contractIds,
          },
        ],
      });
    }
  }
}

function normalizeGetEventsResponse(result: unknown): GetEventsResponse {
  if (!isRecord(result)) {
    return { events: [] };
  }

  const events = result.events;

  if (!Array.isArray(events)) {
    return { events: [] };
  }

  return {
    events: events.filter(isRecord).map((event) => event as StellarRpcEvent),
  };
}

function readLastObservedLedger(events: readonly StellarRpcEvent[]): number | null {
  const ledgers = events
    .map((event) => Number(event.ledgerSequence ?? event.ledger))
    .filter((ledger) => Number.isInteger(ledger) && ledger >= 0);

  if (ledgers.length === 0) {
    return null;
  }

  return Math.max(...ledgers);
}

function hasJsonRpcError(payload: unknown): payload is JsonRpcFailure {
  return isRecord(payload) && payload.error !== undefined;
}

interface LedgerRange {
  minimumLedger: number;
  maximumLedger: number;
}

function readLedgerRangeFromRangeError(error: unknown): LedgerRange | null {
  if (!(error instanceof Error)) {
    return null;
  }

  const match = /ledger range:\s*(\d+)\s*-\s*(\d+)/i.exec(error.message);
  if (!match) {
    return null;
  }

  const minimumLedger = Number(match[1]);
  const maximumLedger = Number(match[2]);

  if (
    !Number.isInteger(minimumLedger) ||
    !Number.isInteger(maximumLedger) ||
    minimumLedger <= 0 ||
    maximumLedger < minimumLedger
  ) {
    return null;
  }

  return { minimumLedger, maximumLedger };
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}
