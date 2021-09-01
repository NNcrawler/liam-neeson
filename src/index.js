require('dotenv').config();

const { Kafka, logLevel } = require('kafkajs');

const host = process.env.BOOTSTRAP_SERVERS || 'localhost:9092';

const kafka = new Kafka({
  logLevel: logLevel[process.env.LOG_LEVEL],
  brokers: host.split(','),
});

const topic = process.env.TOPIC;
const consumer = kafka.consumer({ groupId: process.env.GROUP_ID });

const deserialize = require('./proto-deserializer');
const store = require('./dead-letter-sink');

const run = async () => {
  await consumer.connect();
  await consumer.subscribe({
    topic,
    fromBeginning: process.env.FROM_BEGINNING.toLowerCase() === 'true',
  });
  await consumer.run({
    eachMessage: async ({ topic, partition, message }) => {
      const prefix = `${topic}[parition:${partition} | offset:${message.offset}] / ${message.timestamp}`;
      console.log(`- ${prefix}`);
      await deserialize(
        message.value,
        store(
          topic,
          partition,
          message.offset,
          process.env.BROKEN_MESSAGES_DIRECTORY
        )
      );
    },
  });
};

run().catch((e) => console.error(`[example/consumer] ${e.message}`, e));

const errorTypes = ['unhandledRejection', 'uncaughtException'];
const signalTraps = ['SIGTERM', 'SIGINT', 'SIGUSR2'];

errorTypes.map((type) => {
  process.on(type, async (e) => {
    try {
      console.log(`process.on ${type}`);
      console.error(e);
      await consumer.disconnect();
      process.exit(0);
    } catch (_) {
      process.exit(1);
    }
  });
});

signalTraps.map((type) => {
  process.once(type, async () => {
    try {
      await consumer.disconnect();
    } finally {
      process.kill(process.pid, type);
    }
  });
});
