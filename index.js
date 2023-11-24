const {connect, indexOn}=require('./storage')
const {
    upsert, insert, findAndUpdate,
    findAndUpdateWithOptions, update
}=require('./')
const {recallOne , recallAll, recallWithAggregate, deletOne}=require('./reader');

module.exports={
    connect, upsert, recallAll, recallOne, indexOn, recallWithAggregate,
    insert, findAndUpdate, findAndUpdateWithOptions, update, deletOne
}

