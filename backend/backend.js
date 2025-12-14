const express = require("express");
const fetch = require("node-fetch");
const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const app = express();
const PORT = 7901;

/* ================= Kafka ================= */

const kafka = new Kafka({
  clientId: "backend-service",
  brokers: [process.env.KAFKA_BROKER]
});

const producer = kafka.producer();
const admin = kafka.admin();

const TOPIC_NAME = "todos";

async function initKafka() {
  console.log("Initializing Kafka...");
  await admin.connect();

  const topics = await admin.listTopics();
  if (!topics.includes(TOPIC_NAME)) {
    console.log(`Creating topic: ${TOPIC_NAME}`);
    await admin.createTopics({
      topics: [
        {
          topic: TOPIC_NAME,
          numPartitions: 1,
          replicationFactor: 1
        }
      ]
    });
  }

  await admin.disconnect();
  await producer.connect();
  console.log("Kafka producer ready");
}

initKafka().catch(err => {
  console.error("Kafka init failed:", err);
  process.exit(1);
});

/* ================= Database (READ ONLY) ================= */

const pool = new Pool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT || 5432
});

/* ================= Routes ================= */

/**
 * WRITE PATH
 * Fetch from public API and publish to Kafka
 */
app.get("/status/:id", async (req, res) => {
  try {
    const response = await fetch(
      `https://jsonplaceholder.typicode.com/todos/${req.params.id}`
    );
    const todo = await response.json();

    await producer.send({
      topic: TOPIC_NAME,
      messages: [
        {
          key: String(todo.id),
          value: JSON.stringify(todo)
        }
      ]
    });

    res.json({
      status: "published",
      todo
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/**
 * READ PATH
 * Fetch all todos from DB for UI
 */
app.get("/todos", async (req, res) => {
  try {
    const result = await pool.query(
      "SELECT id, user_id, title, completed FROM todos ORDER BY id"
    );
    res.json(result.rows);
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

/* ================= Server ================= */

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Backend running on port ${PORT}`);
});
