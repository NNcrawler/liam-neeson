const { promises } = require('fs');
const Stream = require('stream');

function createWriteableStream(directory) {
  const writeableStream = Stream.Writable();
  writeableStream._write = (chunk, encoding, next) => {
    const data = JSON.parse(chunk.toString());
    const fileName = `${directory}/${data.topic}-${data.partition}-${data.offset}`;
    console.log(`storing broken message in: ${fileName}`);
    promises.writeFile(fileName, Buffer.from(data.value)).then(() => next());
  };
  return writeableStream;
}

module.exports = { createWriteableStream };
