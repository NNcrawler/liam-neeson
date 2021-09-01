const { promises } = require('fs');

function Store(topic, partition, offset, directory) {
  return function (bytes) {
    const fileName = `${directory}/${topic}-${partition}-${offset}`;
    console.log(`storing broken message in: ${fileName}`);
    promises.writeFile(fileName, bytes);
  };
}

module.exports = Store;
