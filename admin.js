const kafka = require("./kafkaClient");

const admin = kafka.admin();
async function setupTopics() {
  await admin.connect();

  try {
    await admin.createTopics({
      topics: [
        {
          topic: "orders-jy",
          numPartitions: 3,
          replicationFactor: 1,
          configEntries: [
            { name: "retention.ms", value: "93120" }, // any value in ms
          ],
        },
      ],
    });

    const topics = await admin.listTopics();
    console.log("topics ", topics);
    await admin.disconnect();
  } catch (err) {
    console.log("error creating topic: ", err);
  } finally {
    console.log("mdlasmd");
  }
}

// setupTopics().catch(console.error);
module.exports = { setupTopics, admin };
