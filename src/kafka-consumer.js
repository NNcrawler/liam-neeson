const { Kafka, logLevel } = require('kafkajs');
const Stream = require('stream');

const readableStream = new Stream.Readable();
readableStream._read = function () {};

function createReadableStream(options) {
  const host = options.BOOTSTRAP_SERVERS || 'localhost:9092';

  const kafka = new Kafka({
    logLevel: logLevel[options.LOG_LEVEL],
    brokers: host.split(','),
  });

  const topic = options.TOPIC;
  const consumer = kafka.consumer({ groupId: options.GROUP_ID });

  const run = async () => {
    await consumer.connect();
    await consumer.subscribe({
      topic,
      fromBeginning: options.FROM_BEGINNING.toLowerCase() === 'true',
    });
    await consumer.run({
      eachMessage: async ({ topic, partition, message }) => {
        const prefix = `${topic}[parition:${partition} | offset:${message.offset}] / ${message.timestamp}`;
        console.log(`- ${prefix}`);
        readableStream.push(
          JSON.stringify({
            value: message.value,
            offset: message.offset,
            partition,
            topic,
          })
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

  return readableStream;
}

module.exports = {
  createReadableStream,
};
