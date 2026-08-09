// consumer.js

const kafka = require("./kafkaClient");

const consumer = kafka.consumer({ groupId: "order-group=processing" });

async function consumeMessage() {
  await consumer.connect();
  await consumer.subscribe({ topic: "orders-jy", fromBeginning: false });

  await consumer.run({
    // automatically is true by default (commits after each batch)
    eachMessage: async ({ topic, partition, message }) => {
      const value = message.value.toString();
      console.log("consumer - ", {
        topic,
        partition,
        offset: message.offset,
        key: message?.key?.toString(),
        value,
      });
    },
  });
}

// run().catch(console.error);

module.exports = consumeMessage;
