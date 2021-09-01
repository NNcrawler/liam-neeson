const { Stencil } = require('@odpf/stencil');

const url = process.env.STENCIL_URL;
let client;

async function Deserialize(bytes, dlProcessor) {
  client = client
    ? client
    : await Stencil.getInstance(url, {
        shouldRefresh: false,
      });

  const type = client.getType(process.env.PROTO_SCHEMA);
  try {
    type.decode(bytes);
  } catch (err) {
    console.log(`Unable to deserialize: ${err}`);
    dlProcessor(bytes);
  }
}

module.exports = Deserialize;
