const Column = require('../lib/column.js')
const Base = require('./base.js')


/**
 * returns db version
 * @param {string} tableName
 * @returns {strign}
 */
Base.prototype.getTableInfo = async function(tableName) {
    const getTableType = (tableName) => new Promise((resolve,reject) => this.db.get(`SELECT * FROM sqlite_master WHERE name="${tableName}";`, (err, row) => {
        if( err ) reject(err)
        else if (!row) reject(`table ${tableName} not exists`)
        else resolve(row.type)
    }))

    let type
    try {
        type = await getTableType(tableName)

    } catch( e ) {
        return null
    }

    return {
        type: type,
        name: tableName,
        indexes: await this.getIndexes(tableName),
        columns: await this.getColumns(tableName)
    }
}

Base.prototype.getIndexes = async function(tableName) {
    let indexes = [], result = await this.get(`PRAGMA index_list("${tableName}");`)
    for (let i = 0, cn = result.length; i < cn; i++) {
        //result[i] : {name:string, unique: 0|1, origin: u|c|pk, partial: num}
        let indexName = result[i].name
        let columns = await this.get(`PRAGMA index_info("${indexName}");`)

        indexes.push({
            name: indexName,
            unique: result[i].unique || false,
            columns: columns.map(col => col.name)
        })
    }
    return indexes
}

Base.prototype.getColumns = async function(tbl) {
    return await new Promise((resolve,reject) => this.db.all(`PRAGMA table_info(${tbl})`, (err, rows) => {
        if( err ) reject(err)
        else {
             resolve(rows.map(v => new Column(v)))
        }
    }))
}

Base.prototype.exportTables = async function() {
    let dump = []
    let tables = await this.get("SELECT name FROM sqlite_master WHERE type='table'")
    for(let i = 0, cn = tables.length; i < cn; i++){
        dump.push(await this.getTableInfo(tables[i].name) )
    }
    return dump
}


Base.prototype.updateTables = async function(tables){
    const log = this.log || function(){}
    const cmpColumns = function(a, b){
        for(let i = 0, cn = a.length; i < cn; i++) {
            const a1 = a[i]
            const b2 = b.find(c => c.name === a1.name)
            if( !b2 ||
                b2.name !== a1.name ||
                b2.type !== a1.type ||
                b2.notnull !== a1.notnull ||
                b2.dflt_value !== a1.dflt_value ||
                b2.pk !== a1.pk
            ) {
                log('two columns are not equal', a1, b2)
                return false
            }
        }
        return true
    }

    const indexes_diff = function(a, b){
        let diff = []
        for(let i = 0, cn = a.length; i < cn; i++) {
            if( ignoreIndex(a[i].name) ) continue

            const a1 = a[i]
            const b2 = b.find(c => c.name === a1.name)
            if( !b2 ||
                b2.name !== a1.name ||
                !!b2.unique !== !!a1.unique ||
                b2.columns.sort().join(',') !== a1.columns.sort().join(',')
            ) {
                log('indexes are not equal', a1, b2)
                diff.push(a1)
            } //else log( 'b2',b2 )
        }

        return diff
    }

    const ignoreIndex = (name) => name.indexOf('sqlite_autoindex_') === 0
    const updateTable = async (data) => {
        const tbl = await this.getTableInfo(data.name)
        if( ! tbl ) {
            await this.createTable(data.name, data.columns)
            await this.addIndexes(data.name, data.indexes || [])

        } else if (!cmpColumns(data.columns, tbl.columns)) {
            let columns = [].concat( data.columns || [], tbl.columns.filter(col => !data.columns.find(c => c.name === col.name)))
            let mapping = columns.map(col => tbl.columns.find(c => c.name === col.name) ? col.name : getColumnDefaultValue(col))

            await this.updateTable(data.name, columns, mapping)
            await this.addIndexes(data.name, data.indexes || [])

        } else {
            const diff = indexes_diff(data.indexes || [], tbl.indexes)
            for(let i = 0; i < diff.length; i++ ) await this.dropIndex(diff[i].name)
            await this.addIndexes(data.name, diff)
        }
        return true
    }

    for(let i = 0, cn = tables.length; i < cn; i++ ) {
        const tbl = tables[i]
        if( 'table' === tbl.type ) {
            log( '--- update('+tbl.name+')' )
            await updateTable(tbl)
        }
    }
    return true
}

Base.prototype.addColumn = async function(table, column) {
    let spec = new Column(column)
    return await this.exec(`ALTER TABLE \`${table}\` ADD COLUMN ${createColumnDef(spec.name, spec, {})}`)
}

Base.prototype.renameColumn = async function(table, oldName, newName) {
    let column
    const columns = await this.getColumns(table)
    if( (column = columns.find(col => col.name === oldName)) ) {
        const mapping = columns.map(col => col.name)
        column.name = newName
        await this.updateTable(table, columns, mapping)
        return true
    } else return false
}

Base.prototype.dropColumn = async function(table, columnName) {
    const columns = await this.getColumns(table)
    if( columns.find(col => col.name === columnName) ) {
        const mapping = columns.filter(col => col.name !== columnName)
        await this.updateTable(table, mapping)
    }
    return true
}

Base.prototype.addIndex = async function(tableName, indexName, columns, unique) {
    if (!(columns instanceof Array)) columns = [columns]
    return await this.exec(`CREATE ${unique ? 'UNIQUE' : ''} INDEX "${indexName}" ON "${tableName}" (${columns.join(', ')})`)
}

Base.prototype.dropIndex = async function(tableName, indexName) {
    //tableName not used in sqlite
    return await this.exec(`DROP INDEX IF EXISTS "${indexName}"`)
}

Base.prototype.addIndexes = async function(tableName, indexes) {
    if (!(indexes instanceof Array)) return false
    let affected = 0
    for( let i = 0, cn = indexes ? indexes.length : 0; i < cn; i++ ){
        const ix = indexes[i]
        if( !ix.columns || ix.columns.length === 0 ) continue
        //@todo: by std indexes set name to null ()
        if( ix.name.indexOf('sqlite_autoindex_') === 0) continue
        await this.addIndex(tableName, ix.name, ix.columns, ix.unique) ? affected++ : affected
        //console.log(ix)
    }
    return affected
}

Base.prototype.updateTable = async function(table, columns, mapping) {
    await this.exec('PRAGMA foreign_keys=off;')
    await this.exec('BEGIN TRANSACTION;')
    await this.exec(`ALTER TABLE \`${table}\` RENAME TO \`${'tmp_'+table}\``)

    //@notice: indexes will be removed!
    await this.createTable(table, columns)

    const insertinto = [], selectfrom = []
    for(let i = columns.length-1; i>=0; i--){
        if( !mapping ) {
            insertinto.push('`'+columns[i].name+'`')
            selectfrom.push('`'+columns[i].name+'`')
        } else if( null !== mapping[i] ) {
            insertinto.push('`'+columns[i].name+'`')
            selectfrom.push(mapping[i])
        }
    }
    await this.exec(`INSERT INTO \`${table}\` (${insertinto.join(', ')}) SELECT ${selectfrom.join(', ')} FROM ${'tmp_'+table}`)

    await this.exec(`DROP TABLE \`${'tmp_'+table}\``)
    await this.exec('COMMIT;')
    await this.exec('PRAGMA foreign_keys=on;')
    return true
}

Base.prototype.dropTable = async function(tableName){
    return await this.exec(`DROP TABLE \`${tableName}\`;`)
}

Base.prototype.truncate = async function(tableName) {
    return await this.exec(`DELETE FROM \`${tableName}\`;`)
}

// SELECT "is-autoincrement" FROM sqlite_master WHERE tbl_name="temp" AND sql LIKE "%AUTOINCREMENT%";
Base.prototype.createTable = async function(table, columns, options){
    if (typeof columns === "undefined") throw new Error('columns are not defined')
    return await this.exec(createTableStatement(table, columns))
}

/**
 * @param {string} tableName
 * @param {Object[]} columns
 * @param {boolean} [ifNotExists=false]
 */
const createTableStatement = function(tableName, columns, ifNotExists) {
    let pkColumns = [], columnDefOptions = { emitPrimaryKey: false }, pkStmt = ''

    for (let c in columns) {
        columns[c] = new Column(columns[c])
        if (columns[c].primaryKey || columns[c].pk) pkColumns.push(columns[c])
    }

    if (pkColumns.length > 1) {
        pkStmt = handleMultiPrimaryKeys(pkColumns)

    } else if (pkColumns.length === 1) {
        pkColumns[0] = pkColumns[0].name
        columnDefOptions.emitPrimaryKey = true
    }

    let columnDefs = []
    for (let c in columns) {
        columnDefs.push(createColumnDef(
            columns[c].name,
            columns[c],
            columnDefOptions,
            tableName
        ))
    }

  return `CREATE TABLE ${ifNotExists?'IF NOT EXISTS':''}${tableName} (${columnDefs.join(', ')}${pkStmt})`
}

/**
 * @param {Column[]} primaryKeyColumns
 * @returns {String}
 */
const handleMultiPrimaryKeys = function(primaryKeyColumns) {
    return primaryKeyColumns.length > 0  ? ', PRIMARY KEY ('+primaryKeyColumns.map(value => value.name).join(', ')+')' : ''
};
/**
 * @param {string} name
 * @param {Column} spec
 * @param {object} options
 * @returns {string}
 */
const createColumnDef = function (name, spec, options) {
    return ['"' + name + '"', spec.type, createColumnConstraint(spec, options)].join(' ')
};
/**
 * @param {Column} spec
 * @param {Object} options
 */
const createColumnConstraint = function (spec, options) {
    let constraint = []

    if (spec.pk && options.emitPrimaryKey) {
        constraint.push('PRIMARY KEY')
    }

    if (spec.notnull) constraint.push('NOT NULL')
    if (spec.unique) constraint.push('UNIQUE')
    if ( spec.dflt_value ) {
        constraint.push('DEFAULT')
        constraint.push(getColumnDefaultValue(spec))
    }
    return constraint.join(' ')
}
/**
 *
 * @param {Column} column
 */
const getColumnDefaultValue = function(spec){
    if( typeof spec.dflt_value === 'undefined' || spec.dflt_value === null ){
        return null
    } else /* if (typeof spec.dflt_value === 'string') {
        return '"' + spec.dflt_value + '"'
    } else if (typeof spec.dflt_value.spec === 'string') {
        return spec.dflt_value.spec
    } else */
    return spec.dflt_value

}

Base.updateMethodList.push(Base.prototype.updateTables)
Base.exportMethodList.push(Base.prototype.exportTables)

module.exports = Base