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
 *
 */
Base.prototype.exec = function(stmt){
    this.log( stmt )
    return new Promise((resolve,reject) => this.db.run(stmt, (err) => err ? reject(err) : resolve(true)))
}

/**
 *
 */
Base.prototype.get = function(stmt){
    this.log( stmt )
    return new Promise((resolve,reject) => this.db.all(stmt, (err, rows) => err ? reject(err) : resolve(rows)))
}


module.exports = Base