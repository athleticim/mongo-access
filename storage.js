const {MongoClient} = require('mongodb');
const {getMongoUri}=require('@athleticim/vault-manager');

const clientConnections = {};

async function connect(hostName) {
  const {mongoUri, dbName} = {
    mongoUri:await getMongoUri('writeMongo' , hostName),
    dbName:hostName,
  };
  try {
    const clientConn = clientConnections?.[mongoUri];
    if (clientHasConnection(clientConn)) {
      return clientConn.db(dbName);
    }
    const client = new MongoClient(mongoUri);
    await client.connect();
    mapClientToHost(mongoUri, client);
    return client.db(dbName);
  } catch (err) {
    throw new Error(`no database connection: ${err.message}`);
  }
}

const collection = (collectionName, connectionToDb)=> {
  return connectionToDb.collection(collectionName);
};



function isGoodToStore({collectionName, operationMsg}) {
  if (!collectionName) {
    return Promise.reject(Error(
        `no collectionName given for ${operationMsg}`));
  }
  return Promise.resolve();
}

function isEmptyQuery(queryObject) {
  if (Object.entries(queryObject).length === 0 &&
  queryObject.constructor === Object) {
    return Promise.reject(Error(
        `required atleast one parameter`));
  }
  return Promise.resolve();
}

async function indexOn(collectionName, query, hostName) {
  const connectionToDb = await connect(hostName);
  const storage = collection(collectionName, connectionToDb);
  return storage.createIndex(query);
}

async function verifyAndGetTenantCollection({collectionName, hostName, operationMsg}) {
  const connectionToDb = await connect(hostName);
  await isGoodToStore({collectionName, hostName, operationMsg});
  return await collection(collectionName, connectionToDb);
}

function mapClientToHost(mongoUri, client) {
  clientConnections[mongoUri] = client;
}

function clientHasConnection(clientConnection) {
  return Boolean(clientConnection?.topology?.isConnected());
}

module.exports = {
  collection,
  connect,
  isGoodToStore,
  isEmptyQuery,
  indexOn,
  verifyAndGetTenantCollection,
};