const { Stencil } = require('@odpf/stencil');
const Stream = require('stream');

async function createTransformStream(url, protoSchema, successStream) {
  const client = await Stencil.getInstance(url, {
    shouldRefresh: false,
  });

  const type = client.getType(protoSchema);

  const transformStream = new Stream.Transform();

  let readStream;
  if (successStream != null) {
    readStream = new Stream.Readable();
    readStream._read = function () {};
    readStream.pipe(successStream);
  }

  transformStream._transform = function (ch, enc, next) {
    try {
      const data = JSON.parse(ch.toString());
      const message = type.decode(Buffer.from(data.value));
      if (successStream != null) {
        readStream.push(Buffer.from(JSON.stringify(message.toJSON())), 'utf-8');
      }
    } catch (err) {
      this.push(ch);
      console.log(`Unable to deserialize: ${err}`);
    }
    next();
  };

  return transformStream;
}

module.exports = { createTransformStream };
