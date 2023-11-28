const {collection, connect, isGoodToStore,
  isEmptyQuery, verifyAndGetTenantCollection} = require('./storage');

function recallOne({collectionName, identifier, hostName}) {
  return recallAll({collectionName, query: {identifier}, hostName})
      .then(singleOrUndefined);
}

function deleteOne({collectionName, identifier, hostName}) {
  return deleteAll({collectionName, query: {identifier}, hostName});
}

async function deleteAll({collectionName, query, hostName}) {
  const store = await verifyAndGetTenantCollection({
    collectionName, operationMsg: `query ${JSON.stringify(query)}`, hostName,
  });
  return await store.deleteMany(query);
}

async function recallAll({collectionName, query, projection, hostName}) {
  const storage = await verifyAndGetTenantCollection({
    collectionName, hostName, operationMsg: `query ${JSON.stringify(query)}`,
  });
  return await storage.find(query, projection).toArray();
}

function recallAllSorted({collectionName, query, sortCondition, hostName}) {
  return connect(hostName).then((connectionToDb)=>{
    return isGoodToStore({
      collectionName, hostName, operationMsg: `query ${JSON.stringify(query)}`}).then(()=> {
      const dbStorage = collection(collectionName, connectionToDb);
      return dbStorage.find(query).sort(sortCondition).toArray();
    });
  });
}

function recallWithAlias({query, collectionName, hostName}) {
  return connect(hostName).then((connectionToDb)=>{
    const operationMsg = `query parameter ${JSON.stringify(query)}`;
    return isGoodToStore({
      collectionName, operationMsg, hostName,
    }).then(()=> {
      return isEmptyQuery(query).then(() => {
        const storage = collection(collectionName, connectionToDb);
        return storage.aggregate([{$project: query}]).toArray();
      });
    });
  });
}

function singleOrUndefined(resultSet) {
  if (resultSet.length == 0) {
    return Promise.resolve(undefined);
  } else if (resultSet.length == 1) {
    return Promise.resolve(resultSet[0]);
  } else {
    return Promise.reject(
        Error(`single or none expected, but ${resultSet.length} found.
        the first one is: ${JSON.stringify(resultSet[0])}`));
  }
}

function recallWithAggregate({aggregateQuery, collectionName, hostName}) {
  return connect(hostName)
      .then((connectionToDb)=>{
        return isGoodToStore({
          collectionName,
          operationMsg: `query parameter ${JSON.stringify(aggregateQuery)}`,
          hostName,
        }).then(()=> {
          return isEmptyQuery(aggregateQuery).then(() => {
            const storageCollection = collection(collectionName, connectionToDb);
            return storageCollection.aggregate(aggregateQuery).toArray();
          });
        });
      });
}

module.exports = {connect, recallOne, recallAll, recallWithAlias,
  singleOrUndefined, deleteAll, deleteOne, recallWithAggregate,
  recallAllSorted};