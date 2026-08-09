const express = require("express");
const produceMessage = require("./producer");
const consumeMessage = require("./consumer");
const { setupTopics, admin } = require("./admin");

const app = express();
app.use(express.json());

app.post("/send", async (req, res) => {
  try {
    const message = req.body;
    await produceMessage(message);
    res.json({ success: true, message: "message sent successfully" });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});
app.get("/send", async (req, res) => {
  try {
    const topics = await admin.listTopics();
    console.log(topics);
    res.json({ success: true, topics });
  } catch (err) {
    res.status(500).json({ success: false, error: err.message });
  }
});

app.get("/health", (req, res) => {
  res.json({ status: "ok", timestamp: new Date().toISOString() });
});

// initialize the app
async function init() {
  try {
    // create topic if it doesnt exist
    await setupTopics();

    await consumeMessage();

    const PORT = process.env.PORT || 3000;
    app.listen(PORT, () => {
      console.log(`Server running on http://localhost:${PORT}`);
    });
  } catch (err) {
    console.log("error while initializing the app", err);
  }
}

// Graceful shutdown
process.on("SIGTERM", async () => {
  console.log("Shutting down gracefully...");
  await producer.disconnect();
  await consumer.disconnect();
  process.exit(0);
});

init();
