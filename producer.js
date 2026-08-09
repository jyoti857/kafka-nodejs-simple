const { Partitioners } = require("kafkajs");
const kafka = require("./kafkaClient");

const producer = kafka.producer({
  idempotent: false, // prevents duplicate writes on retry
  maxInFlightRequests: 1,
  createPartitioner: Partitioners.LegacyPartitioner,
});

// async function run() {
async function produceMessage(msg) {
  await producer.connect(msg);
  const { key, userId, amount = 0, orderId = 1 } = msg;
  const spl = userId.split("_");
  const userIdC = spl[0] + (+spl[1] ?? 0 + 1);
  const orderIdC = spl[0] + (+spl[1] ?? 0 + 1);

  try {
    await producer.send({
      topic: "orders-jy",
      acks: -1, // all - wait for full ISR ack
      messages: [
        {
          key,
          value: JSON.stringify({ orderId: orderIdC, userId: userIdC, amount }),
          // headers: { source: "checkout-service" },
        },
      ],
    });
    console.log("producer has sent ");
  } catch (err) {
    console.log("error producing message:", err);
  }

  console.log("message sent");
  await producer.disconnect();
}

// run().catch(console.error);

module.exports = produceMessage;
