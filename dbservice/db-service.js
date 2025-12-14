const { Kafka } = require("kafkajs");
const { Pool } = require("pg");

const kafka = new Kafka({
  clientId: "db-service",
  brokers: [process.env.KAFKA_BROKER]
});

const consumer = kafka.consumer({ groupId: "todo-consumers" });

const pool = new Pool({
  host: process.env.DB_HOST,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  port: process.env.DB_PORT || 5432
});

const TOPIC_NAME = "todos";

async function startConsumer() {
  while (true) {
    try {
      console.log("Connecting to Kafka...");
      await consumer.connect();
      await consumer.subscribe({ topic: TOPIC_NAME, fromBeginning: true });

      console.log("Kafka consumer ready");

      await consumer.run({
        eachMessage: async ({ message }) => {
          const todo = JSON.parse(message.value.toString());

          await pool.query(
            `INSERT INTO todos (id, user_id, title, completed)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (id) DO NOTHING`,
            [todo.id, todo.userId, todo.title, todo.completed]
          );

          console.log(`Stored todo id=${todo.id}`);
        }
      });

      break; // run exits only on fatal error
    } catch (err) {
      console.error("Kafka not ready, retrying in 5s...", err.message);
      await new Promise(r => setTimeout(r, 5000));
    }
  }
}

startConsumer();
