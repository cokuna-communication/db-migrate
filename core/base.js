/**
 * @type {sqlite3.Database} driver
 * @type {callback} log - to output some debug info, could be console.log Function
 */
const Base = function (driver, log) {
    this.db = driver
    this.log = typeof log === "function" ? log : function(...args){}
}

/**
 * returns db version
 * @returns {?Strign}
 */
Base.prototype.getVersion = async function () {
    const result = await this.get('select sqlite_version() as version')
    return result ? result[0].version : null
}
/**
 * @param {string} stmt
 * @returns {Promise.<boolean>}
 */
Base.prototype.exec = function(stmt){
    this.log( stmt )
    return new Promise((resolve,reject) => this.db.run(stmt, (err) => err ? reject(err) : resolve(true)))
}

/**
 * @param {string} stmt
 * @returns {Promise.<any[]>}
 */
Base.prototype.get = function(stmt){
    this.log( stmt )
    return new Promise((resolve,reject) => this.db.all(stmt, (err, rows) => err ? reject(err) : resolve(rows)))
}

/**
 *
 */
Base.updateMethodList = []
Base.exportMethodList = []

/**
 * @param {object[]} dump
 * @returns {Promise}
 */
Base.prototype.update = async function(dump){
    for(let i = 0, cn = Base.updateMethodList.length; i < cn; i++ ) {
        await Base.updateMethodList[i].call(this,dump)
    }
}

/**
 * @returns {Promise.<object[]>}
 */
Base.prototype.export = async function(){
    const dump = []
    for(let i = 0, cn = Base.exportMethodList.length; i < cn; i++ ) {
        const _d = await Base.exportMethodList[i].call(this,dump)
        if (_d) dump.push(..._d)
    }
    return dump
}

module.exports = Base