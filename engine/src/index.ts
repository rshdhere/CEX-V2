import "dotenv/config";
import { createClient } from "redis";
import { env } from "./utils/env.js";
import {
  ORDERBOOKS,
  ORDERS,
  type CreateOrderInput,
  type OrderBook,
  type RestingOrder,
} from "./store/exchange-store.js";

export type EngineCommandType =
  | "create_order"
  | "get_depth"
  | "get_user_balance"
  | "get_order"
  | "cancel_order";

export interface EngineRequest {
  correlationId: string;
  responseQueue: string;
  type: EngineCommandType;
  payload: Record<string, unknown>;
}

export interface EngineResponse {
  correlationId: string;
  ok: boolean;
  data?: unknown;
  error?: string;
}

const brokerClient = createClient({ url: env.redisUrl }).on(
  "error",
  (error) => {
    console.error("Redis broker client error", error);
  },
);

const responseClient = createClient({ url: env.redisUrl }).on(
  "error",
  (error) => {
    console.error("Redis response client error", error);
  },
);

await Promise.all([brokerClient.connect(), responseClient.connect()]);

async function sendResponse(
  responseQueue: string,
  response: EngineResponse,
): Promise<void> {
  await responseClient.lPush(responseQueue, JSON.stringify(response));
}

function getOrCreateOrderBook(symbol: string): OrderBook {
  let orderbook = ORDERBOOKS.get(symbol);

  if (!orderbook) {
    orderbook = {
      asks: new Map(),
      bids: new Map(),
    };
  }

  ORDERBOOKS.set(symbol, orderbook);
  return orderbook;
}

function handleEngineRequest(message: EngineRequest): unknown {
  /**
   * TODO(student):
   * 1. Check _message.type.
   * 2. Read _message.payload.
   * 3. Call your order book / balance / order logic.
   * 4. Return the data that should go back to the backend.
   *
   * Required message types:
   * - create_order
   * - get_depth
   * - get_user_balance
   * - get_order
   * - cancel_order
   */

  if (message.type === "create_order") {
    const { userId, type, side, symbol, price, qty } =
      message.payload as unknown as CreateOrderInput;

    if (qty <= 0) {
      throw new Error("invalid quantity");
    }

    if (type === "limit" && price === null) {
      throw new Error("limit order price required");
    }

    const order: RestingOrder = {
      userId,
      orderId: crypto.randomUUID(),
      symbol,
      qty,
      side,
      type: "limit",
      price: price!,
      filledQty: 0,
      status: "open",
      createdAt: Date.now(),
    };

    const book = getOrCreateOrderBook(symbol);

    const opposingBook = side === "buy" ? book.asks : book.bids;

    const sortedPrices = [...opposingBook.keys()].sort((a, b) =>
      side === "buy" ? a - b : b - a,
    );

    for (const bookPrice of sortedPrices) {
      if (order.filledQty >= order.qty) {
        break;
      }

      // LIMIT ORDER PRICE CHECKS
      if (type === "limit") {
        const incompatible =
          side === "buy" ? bookPrice > price! : bookPrice < price!;

        if (incompatible) {
          break;
        }
      }

      const level = opposingBook.get(bookPrice);

      if (!level) {
        continue;
      }

      for (const resting of level) {
        if (order.filledQty >= order.qty) {
          break;
        }

        const incomingRemaining = order.qty - order.filledQty;

        const restingRemaining = resting.qty - resting.filledQty;

        const tradedQty = Math.min(incomingRemaining, restingRemaining);

        resting.filledQty += tradedQty;
        order.filledQty += tradedQty;

        if (resting.filledQty === resting.qty) {
          resting.status = "filled";
        } else {
          resting.status = "partially_filled";
        }
      }

      const remainingOrders = level.filter((o) => o.filledQty < o.qty);

      if (remainingOrders.length === 0) {
        opposingBook.delete(bookPrice);
      } else {
        opposingBook.set(bookPrice, remainingOrders);
      }
    }

    // FINAL STATUS
    if (order.filledQty === 0) {
      order.status = "open";
    } else if (order.filledQty < order.qty) {
      order.status = "partially_filled";
    } else {
      order.status = "filled";
    }

    // REST REMAINING LIMIT QTY
    if (type === "limit" && order.filledQty < order.qty) {
      const ownBook = side === "buy" ? book.bids : book.asks;

      const ownLevel = ownBook.get(price!) ?? [];

      ownLevel.push(order);

      ownBook.set(price!, ownLevel);
    }

    ORDERS.set(order.orderId, {
      ...order,
      fills: [],
    });

    return order;
  }
  if (message.type === "cancel_order") {
    return;
  }

  if (message.type === "get_order") {
    return;
  }

  if (message.type === "get_depth") {
    return;
  }

  if (message.type === "get_user_balance") {
    return;
  }
}

console.log(`Engine listening on Redis queue: ${env.incomingQueue}`);

for (;;) {
  const item = await brokerClient.brPop(env.incomingQueue, 0);
  if (!item) continue;

  let message: EngineRequest;

  try {
    message = JSON.parse(item.element) as EngineRequest;
  } catch {
    console.error("Skipping invalid broker message");
    continue;
  }

  try {
    const data = handleEngineRequest(message);
    await sendResponse(message.responseQueue, {
      correlationId: message.correlationId,
      ok: true,
      data,
    });
  } catch (error) {
    await sendResponse(message.responseQueue, {
      correlationId: message.correlationId,
      ok: false,
      error: error instanceof Error ? error.message : "engine_error",
    });
  }
}
