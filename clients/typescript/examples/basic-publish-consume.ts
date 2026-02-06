/**
 * Basic publish/consume example for qwer-q TypeScript client.
 *
 * Prerequisites:
 *   - qwer-q broker running on localhost:9876
 *
 * Usage:
 *   npx tsx examples/basic-publish-consume.ts
 */

import { QwerQConnection, TypedClient } from "../src/index.js";

interface OrderEvent {
  orderId: string;
  amount: number;
  currency: string;
}

async function main() {
  // --- Publisher ---
  const pub = new QwerQConnection({ host: "127.0.0.1", port: 9876 });
  await pub.connect();
  console.log("Publisher connected");

  // Publish a raw buffer message
  const result = await pub.publish(
    "orders",
    Buffer.from(JSON.stringify({ orderId: "ORD-001", amount: 99.99, currency: "USD" })),
  );
  console.log("Published:", result.messageId);

  // Or use the typed client for convenience
  const typed = new TypedClient(pub);
  const result2 = await typed.publish<OrderEvent>("orders", {
    orderId: "ORD-002",
    amount: 149.00,
    currency: "EUR",
  });
  console.log("Published (typed):", result2.messageId);

  // --- Consumer ---
  const sub = new QwerQConnection({
    host: "127.0.0.1",
    port: 9876,
    reconnect: true,
  });
  await sub.connect();
  console.log("Consumer connected");

  // Subscribe with raw handler
  await sub.subscribe("orders", (msg) => {
    console.log("Received:", msg.messageId, msg.payload.toString());
    sub.ack(msg.messageId);
  }, { prefetch: 10 });

  // Or with typed handler
  const typedSub = new TypedClient(sub);
  await typedSub.subscribe<OrderEvent>("orders", (msg) => {
    console.log("Received (typed):", msg.data.orderId, msg.data.amount);
    typedSub.connection.ack(msg.messageId);
  });

  // Keep running for 10 seconds
  await new Promise((resolve) => setTimeout(resolve, 10_000));

  await pub.close();
  await sub.close();
  console.log("Done");
}

main().catch(console.error);
