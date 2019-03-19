const Base = require('./base.js')

/**
 *
 * @param {string} name
 * @returns {object}
 */
Base.prototype.getTriggerInfo = async function(name) {
    const getTrigger = (name) => new Promise((resolve,reject) => this.db.get(`SELECT * FROM sqlite_master WHERE name="${name}";`, (err, row) => {
        if( err ) reject(err)
        else if (!row) reject(`trigger ${name} not exists`)
        else resolve(row)
    }))

    let type, target, action, code
    try {
        const data = await getTrigger(name)
        type = data.type

        const parse = /^CREATE TRIGGER\s+(.+)\s((?:AFTER|BEFORE) .+) ON (.+)\s+BEGIN\s+(.+)\s+END$/
        const matched = data.sql.match(parse)
        if( !matched ) throw Error('Unknown SQL Statements')

        action = matched[2] //AFTER|BEFOR + INSERT|UPDATE|DELETE
        target = matched[3]
        code   = matched[4]

    } catch( e ) {
        //console.log( e )
        return null
    }

    return {
        type: type,
        name: name,
        target : target,
        action : action,
        code   : code
    }
}

/**
 * @param {string} name
 * @param {string} action
 * @param {string} target
 * @param {string} code
 */
const createTriggerStatement = function(name, action, target, code){
    return  `CREATE TRIGGER ${name} ${action} ON ${target}
        BEGIN
            ${code}
        END`
}

/**
 * @param {string} name
 * @param {string} action
 * @param {string} target
 * @param {string} code
 */
Base.prototype.createTrigger = async function(name, action, target, code){
    const stmt = createTriggerStatement(name, action, target, code)
    return await this.exec(stmt)
}

/**
 * @param {string} name
 * @param {string} action
 * @param {string} target
 * @param {string} code
 */
Base.prototype.updateTrigger = async function(name, action, target, code) {
    const stmt = createTriggerStatement(name, action, target, code)
    await this.dropTrigger(name)
    return await this.exec(stmt)
}

/**
 * @param {string} name
 */
Base.prototype.dropTrigger = async function(name) {
    const stmt = `DROP TRIGGER IF EXISTS ${name}`;
    return await this.exec(stmt)
}

/**
 *
 */
Base.prototype.exportTriggers = async function() {
    let dump = []
    let tables = await this.get("SELECT name FROM sqlite_master WHERE type='trigger'")
    for(let i = 0, cn = tables.length; i < cn; i++){
        dump.push(await this.getTriggerInfo(tables[i].name) )
    }
    return dump
}

module.exports = Base