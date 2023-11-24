const {connect, verifyAndGetTenantCollection} = require('./storage');

async function upsert({collectionName, parameters, hostName}) {
  const storage = await verifyAndGetTenantCollection({
    collectionName, hostName, operationMsg: `upsert ${JSON.stringify(parameters)}`,
  });
  const queryCond = {identifier: parameters.identifier};
  // mongodb expects to split the Operators to modify document
  const {nonDollarOpsParams, dollarOpsParams} = splitParameters(parameters);
  return await storage.findOneAndUpdate(queryCond,
      {$set: {...nonDollarOpsParams}, ...dollarOpsParams}, {upsert: true});
}

async function update({collectionName, parameters, hostName}) {
  const operationMsg = `update data ${JSON.stringify(parameters)}`;
  const tenantCollection = await verifyAndGetTenantCollection({
    collectionName, hostName, operationMsg,
  });
  // mongodb expects to split the Operators to modify document
  const {nonDollarOpsParams, dollarOpsParams} = splitParameters(parameters);
  const updateResponse = tenantCollection.findOneAndUpdate({
    identifier: parameters.identifier}, {$set: {...nonDollarOpsParams}, ...dollarOpsParams});
  return updateResponse;
}

async function findAndUpdate({collectionName, parameters, query, hostName}) {
  const storage = await verifyAndGetTenantCollection({
    collectionName, operationMsg: `updating ${JSON.stringify(parameters)}`,
    hostName,
  });
  // mongodb expects to split the Operators to modify document
  const {nonDollarOpsParams, dollarOpsParams} = splitParameters(parameters);
  const updatedResult = await storage.updateOne(query,
      {$set: {...nonDollarOpsParams}, ...dollarOpsParams}, {upsert: false});
  return updatedResult;
}

async function findAndUpdateWithOptions({collectionName,
  parameters, query, options, hostName}) {
  const dbStorage = await verifyAndGetTenantCollection({
    collectionName, hostName, operationMsg: `updating with options
      ${JSON.stringify(parameters)}`,
  });
  // mongodb expects to split the Operators to modify document
  const {nonDollarOpsParams, dollarOpsParams} = splitParameters(parameters);
  const result = await dbStorage.updateOne(query,
      {$set: {...nonDollarOpsParams}, ...dollarOpsParams}, {upsert: false, ...options});
  return result;
}

async function insert({collectionName, parameters, hostName}) {
  const storage = await verifyAndGetTenantCollection({
    collectionName, hostName, operationMsg: `insert ${JSON.stringify(parameters)}`,
  });
  return await storage.insertMany([{...parameters}], {lean: true, checkKeys: false});
}

function splitParameters(parameters) {
  const nonDollarOpsParams = {};
  const dollarOpsParams = {};

  for (const key in parameters) {
    if (key.startsWith('$')) {
      dollarOpsParams[key] = parameters[key];
    } else {
      nonDollarOpsParams[key] = parameters[key];
    }
  }

  return {nonDollarOpsParams, dollarOpsParams};
}

module.exports = {connect, upsert, insert,
  findAndUpdate, findAndUpdateWithOptions, update};