const consumer = require('./src/kafka-consumer');
const deserializer = require('./src/deserializer');
const fileSink = require('./src/file-sink');
const Stream = require('stream');
require('dotenv').config();

(async () => {
  const kafkaConsumer = consumer.createReadableStream(process.env);

  const inspectData = new Stream.Writable();
  inspectData.setDefaultEncoding('utf-8');
  inspectData._write = function (ch, en, next) {
    console.log(JSON.stringify(JSON.parse(ch.toString()), null, 2));
    next();
  };
  const forwardBroken = await deserializer.createTransformStream(
    process.env.STENCIL_URL,
    process.env.PROTO_SCHEMA,
    process.env.ENABLE_DATA_INSPECTION.toLowerCase() === 'true'
      ? inspectData
      : null
  );
  const sink = fileSink.createWriteableStream(
    process.env.BROKEN_MESSAGES_DIRECTORY
  );

  Stream.pipeline(kafkaConsumer, forwardBroken, sink, (err) => {
    if (err) {
      console.error('Pipeline failed.', err);
    } else {
      console.log('Pipeline succeeded.');
    }
  });
})();
