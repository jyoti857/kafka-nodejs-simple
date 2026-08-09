const { logLevel, Kafka } = require("kafkajs");

const kafka = new Kafka({
  clientId: "my-app-jy",
  brokers: ["localhost:9092"],
  logLevel: logLevel.WARN,
  retry: {
    initialRetryTime: 300,
    retries: 2,
  },
});

module.exports = kafka;
