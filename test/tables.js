/**
 *
 */
const assert = require('assert')
//const sqlite3 = require('@journeyapps/sqlcipher').verbose()
const sqlite3 = require('sqlite3').verbose()
const DBMigrate = require('../core/tables.js')

describe('DataBase Migration', function() {
    const testTableColumns = async function(upd, table, expected){
        const columns = await upd.getColumns(table)
        cmpTableColumns(table, expected, columns)
    }
    const cmpTableColumns = function(tableName, expected, actual){
        assert.equal(actual ? actual.length : 0, expected.length, 'Column count of table '+tableName+' not equals')
        for(let i = 0, cn = expected.length; i < cn; i++) {
            for(let c in expected[i]) {
                //console.log(i, c, expected[i][c], actual[i][c] )
                assert.ok(typeof actual[i][c] !== 'undefined', `Field ${tableName}.${expected[i].name}.${c} not exists in column #${i}`)
                assert.equal(actual[i][c], expected[i][c], `Column property ${tableName}.${expected[i].name}.${c} not equal #${i}`)
            }
        }

        for(let i = 0, cn = actual.length; i < cn; i++) {
            assert.ok(expected.find(col => col.name === actual[i].name), `Table ${tableName} has needless column ${expected[i].name}`)
        }
    }

    /**
     * test getting table information
     */
    describe('table info',function(){
        const db = new sqlite3.Database(':memory:')
        const upd = new DBMigrate( db )//, console.log
        const addTempTable = (tbl) => new Promise((resolve,reject) => {
            const createStmt = `CREATE TABLE ${tbl} (
                id INT(11) PRIMARY KEY,
                name VARCHAR (10),
                email VARCHAR (20) NOT NULL,
                date DATETIME NOT NULL DEFAULT datetime,
                UNIQUE (\`email\`)
            );`, addindex = 'CREATE INDEX "namedate" ON `'+tbl+'` (`name`,`date`)'

            db.serialize(function(){
                db.exec(createStmt, err => err ? reject(err) : true)
                db.exec(addindex, err => err ? reject(err) : resolve(true))
            })
        })

        before(async function(){
            await addTempTable('temp')
        })

        it('indexes', async function() {
            let indexes

            indexes = await upd.getIndexes('notexists')
            assert.equal(indexes.length, 0, 'Indexes should not exist')


            indexes = await upd.getIndexes('temp')
            assert.equal(indexes.length, 3, 'Index count not equals')
            assert.equal(indexes[0].name, 'namedate', '')
            assert.equal(indexes[0].unique, 0)
            assert.ok(indexes[0].columns.indexOf('name') !== -1, indexes[0].columns.indexOf('date') !== -1)

            assert.equal(indexes[1].unique, 1)
            assert.ok(indexes[1].columns.indexOf('email') !== -1)

            assert.equal(indexes[2].unique, 1)
            assert.ok(indexes[2].columns.indexOf('id') !== -1)
        })

        it('columns', async function() {
            let columns

            columns = await upd.getColumns('notexists')
            assert.equal(columns.length, 0, 'Columns should not exist')

            const expected = [
                { cid: 0, name: 'id', type: 'INT(11)', notnull: 0, dflt_value: null, pk: 1 },
                { cid: 1, name: 'name', type: 'VARCHAR (10)', notnull: 0, dflt_value: null, pk: 0 },
                { cid: 2, name: 'email', type: 'VARCHAR (20)', notnull: 1, dflt_value: null, pk: 0 },
                { cid: 3, name: 'date', type: 'DATETIME', notnull: 1, dflt_value: 'datetime', pk: 0 }
            ]
            await testTableColumns(upd, 'temp', expected)
        })

        it('common', async function() {
            let info
            info = await upd.getTableInfo('notexists')
            assert.ok(!info)

            info = await upd.getTableInfo('temp')
            assert.equal(info.name, 'temp')
            assert.equal(info.type, 'table')

            assert.ok(typeof info.indexes !== 'undefined')
            assert.ok(typeof info.columns !== 'undefined')

            assert.ok(info.indexes instanceof Array)
            assert.ok(info.columns instanceof Array)
        })

        it('exportTables', async function(){
            const actual = await upd.exportTables()

            assert.equal(actual.length, 1, 'DB shoud contain only one table')
            assert.equal(actual[0].type, 'table')
            assert.equal(actual[0].name, 'temp')

            const expectedColumns = [
                { cid: 0, name: 'id', type: 'INT(11)', notnull: 0, dflt_value: null, pk: 1 },
                { cid: 1, name: 'name', type: 'VARCHAR (10)', notnull: 0, dflt_value: null, pk: 0 },
                { cid: 2, name: 'email', type: 'VARCHAR (20)', notnull: 1, dflt_value: null, pk: 0 },
                { cid: 3, name: 'date', type: 'DATETIME', notnull: 1, dflt_value: 'datetime', pk: 0 }
            ]
            cmpTableColumns('temp', expectedColumns, actual[0].columns)

            assert.equal(3, actual[0].indexes.length, 'Count of indexes not equals')
            const ix = actual[0].indexes[0]
            assert.equal('namedate', ix.name, 'Index name not equals')
            assert.equal(false, ix.unique, 'Index is unique')
            assert.equal('name,date', ix.columns.join(','))
        })
    });


    /**
     * test direct table modifying function
     */
    describe('udpate table', async function() {
        const db = new sqlite3.Database(':memory:')
        const upd = new DBMigrate( db )//, console.log)

        it('create table', async function() {

            await upd.createTable('tbl_create', [
                { cid: 0, name: 'id', type: 'INTERGER', notnull: 1, dflt_value: null, pk: 1 },
                { cid: 1, name: 'name', type: 'VARCHAR(10)', notnull: 1, dflt_value: '"user name"', pk: 0 },
                { cid: 2, name: 'date', type: 'DATETIME', notnull: 1, unique: true, dflt_value: 'datetime', pk: 0 },
                { cid: 3, name: 'email', type: 'VARCHAR(100)', notnull: 0, dflt_value: null, pk: 0 }
            ])

            const expected = [
                { cid: 0, name: 'id', type: 'INTERGER', notnull: 1, dflt_value: null, pk: 1 },
                { cid: 1, name: 'name', type: 'VARCHAR(10)', notnull: 1, dflt_value: '"user name"', pk: 0 },
                { cid: 2, name: 'date', type: 'DATETIME', notnull: 1, dflt_value: 'datetime', pk: 0 },
                { cid: 3, name: 'email', type: 'VARCHAR(100)', notnull: 0, dflt_value: null, pk: 0 }
            ]

            await testTableColumns(upd, 'tbl_create', expected)
        })

        it('add column', async function() {
            await upd.exec("INSERT INTO `tbl_create` (id,name,email) VALUES (1,'N1','@mail1');")
            await upd.addColumn('tbl_create', { cid: null, name: 'phone', type: 'VARCHAR(50)', notnull: 0, dflt_value: null, pk: 0 })

            const expected = [
                { cid: 0, name: 'id', type: 'INTERGER', notnull: 1, dflt_value: null, pk: 1 },
                { cid: 1, name: 'name', type: 'VARCHAR(10)', notnull: 1, dflt_value: '"user name"', pk: 0 },
                { cid: 2, name: 'date', type: 'DATETIME', notnull: 1, dflt_value: 'datetime', pk: 0 },
                { cid: 3, name: 'email', type: 'VARCHAR(100)', notnull: 0, dflt_value: null, pk: 0 },
                { cid: 4, name: 'phone', type: 'VARCHAR(50)', notnull: 0, dflt_value: null, pk: 0 }
            ]

            await testTableColumns(upd, 'tbl_create', expected)
            let rows = await upd.get('SELECT * FROM tbl_create WHERE 1')

            assert.equal(rows.length, 1, 'Table was damaged')
            assert.equal(rows[0].id, 1, 'Table data was damaged')
            assert.equal(rows[0].name, 'N1', 'Table data was damaged')
            assert.equal(rows[0].email, '@mail1', 'Table data was damaged')
            assert.equal(rows[0].phone, null, 'Table data was damaged')
        })

        it('rename column', async function() {
            await upd.exec("INSERT INTO `tbl_create` (id,name,email,phone) VALUES (2,'N2','@mail2','002');")
            await upd.renameColumn('tbl_create', 'phone', 'mobile')

            const expected = [
                { cid: 0, name: 'id', type: 'INTERGER', notnull: 1, dflt_value: null, pk: 1 },
                { cid: 1, name: 'name', type: 'VARCHAR(10)', notnull: 1, dflt_value: '"user name"', pk: 0 },
                { cid: 2, name: 'date', type: 'DATETIME', notnull: 1, dflt_value: 'datetime', pk: 0 },
                { cid: 3, name: 'email', type: 'VARCHAR(100)', notnull: 0, dflt_value: null, pk: 0 },
                { cid: 4, name: 'mobile', type: 'VARCHAR(50)', notnull: 0, dflt_value: null, pk: 0 }
            ]

            await testTableColumns(upd, 'tbl_create', expected)

            let rows = await upd.get('SELECT * FROM tbl_create WHERE 1')
            assert.equal(rows.length, 2, 'Table was damaged')
            assert.equal(rows[1].id, 2, 'Table data was damaged')
            assert.equal(rows[1].name, 'N2', 'Table data was damaged')
            assert.equal(rows[1].email, '@mail2', 'Table data was damaged')
            assert.equal(rows[1].mobile, '002', 'Table data was damaged')
            assert.ok(typeof rows[1].phone === "undefined", 'Field phone is still there')
        })

        it('add index', async function() {
            await upd.addIndex('tbl_create', 'email', 'email', true)
            await upd.addIndex('tbl_create', 'namedate', ['name', 'date'])

            const indexes = await upd.getIndexes('tbl_create')
            let ix

            assert.equal(indexes.length, 3)

            assert.ok(!!(ix = indexes.find(x => x.name === 'namedate')), 'namedate Index was not created')
            assert.equal(ix.unique, 0, 'namedate should not be unique')
            assert.equal(ix.columns.length, 2, 'namedate column count not equals')
            assert.ok(ix.columns.indexOf('name') !== -1, ix.columns.indexOf('date') !== -1, 'namedate does not contain name and date')

            assert.ok(!!(ix = indexes.find(x => x.name === 'email')), 'email Index was not created')
            assert.equal(ix.unique, 1, 'email should be unique')
            assert.equal(ix.columns.length, 1, 'email column count not equals')
            assert.ok(ix.columns.indexOf('email') !== -1)

            //last index for Primary Key
        })

        it('remove index', async function() {
            await upd.dropIndex('tbl_create', 'email')
            const indexes = await upd.getIndexes('tbl_create')
            let ix
            assert.equal(indexes.length, 2)

            assert.ok(!!(ix = indexes.find(x => x.name === 'namedate')), 'namedate Index was not created')
            assert.equal(ix.unique, 0, 'namedate should not be unique')
            assert.equal(ix.columns.length, 2, 'namedate column count not equals')
            assert.ok('name,date', ix.columns.join(','), 'namedate does not contain name and date')

            assert.ok(!(ix = indexes.find(x => x.name === 'email')), 'email Index was not removed')
            //last index for Primary Key
        })

        it('add multiple indexes', async function(){
            await upd.addIndexes('tbl_create', [
                { name: 'mobile', columns: ['mobile'], unique: false },
                { name: 'datename', columns: ['date', 'name'], unique: false }
            ])

            const indexes = await upd.getIndexes('tbl_create')
            let ix

            assert.equal(indexes.length, 4)

            assert.ok(!!(ix = indexes.find(x => x.name === 'mobile')), 'mobile Index was not created')
            assert.equal(ix.unique, 0, 'mobile should not be unique')
            assert.equal(ix.columns.length, 1, 'mobile column count not equals')
            assert.ok('mobile', 'mobile does not mobile column')

            assert.ok(!!(ix = indexes.find(x => x.name === 'datename')), 'email Index was not created')
            assert.equal(ix.unique, 0, 'datename should be unique')
            assert.equal(ix.columns.length, 2, 'datename column count not equals')
            assert.ok('data,name', ix.columns.join(','), 'datename does not data und name columns')
        })


        it('drop column', async function() {
           await upd.dropColumn('tbl_create', 'mobile')

            const expected = [
                { cid: 0, name: 'id', type: 'INTERGER', notnull: 1, dflt_value: null, pk: 1 },
                { cid: 1, name: 'name', type: 'VARCHAR(10)', notnull: 1, dflt_value: '"user name"', pk: 0 },
                { cid: 2, name: 'date', type: 'DATETIME', notnull: 1, dflt_value: 'datetime', pk: 0 },
                { cid: 3, name: 'email', type: 'VARCHAR(100)', notnull: 0, dflt_value: null, pk: 0 }
            ]

            await testTableColumns(upd, 'tbl_create', expected)

            let rows = await upd.get('SELECT * FROM tbl_create WHERE 1')
            assert.equal(rows.length, 2, 'Table was damaged')
            assert.equal(rows[1].id, 2, 'Table data was damaged')
            assert.equal(rows[1].name, 'N2', 'Table data was damaged')
            assert.equal(rows[1].email, '@mail2', 'Table data was damaged')
            assert.ok(typeof rows[1].mobile === "undefined", 'Field phone is still there')
        })

        it('truncate', async function() {
            await upd.truncate('tbl_create')

            let rows = await upd.get('SELECT * FROM tbl_create WHERE 1')
            assert.equal(rows.length, 0, 'Table rows was not deleted')
        })

        it('drop table', async function() {
            await upd.dropTable('tbl_create')

            const columns = await upd.getColumns('tbl_create')
            assert.equal(columns.length, 0, 'Table still exists')
        })
    })

    /**
     * test updating the whole database at once
     */
    describe("update database",function(){
        const db = new sqlite3.Database(':memory:')
        const upd = new DBMigrate( db )
        const initial = [
            {
                type: 'table',
                name: 'tbl1',
                columns : [
                    { cid: 0, name: 'id', type: 'INTERGER', notnull: 1, dflt_value: null, pk: 1 },
                    { cid: 1, name: 'name', type: 'VARCHAR(10)', notnull: 1, dflt_value: '"user name"', pk: 0 }
                ],
                indexes : [ { name: 'name', unique: false, columns: ['name'] } ],
                values  : [
                    { id: 1, name: 'u1'}
                ]
            }, {
                type: 'table',
                name: 'tbl2',
                columns : [
                    { cid: 0, name: 'id', type: 'INTERGER', notnull: 1, dflt_value: null, pk: 1 },
                    { cid: 1, name: 'name', type: 'VARCHAR(10)', notnull: 1, dflt_value: '"user name"', pk: 0 },
                    { cid: 2, name: 'date', type: 'DATETIME', notnull: 1, dflt_value: 'datatime', pk: 0 }
                ],
                values : [
                    { id: 1, name: 'uu1', date: '2019-01-01 00:00:00'},
                    { id: 2, name: 'uu2', date: '2019-02-02 01:01:01'}
                ]
            }
        ]

        const insertValues = (table, values) => upd.exec('INSERT INTO `'+table+'` (`'+Object.keys(values[0]).join('`, `')+'`) VALUES '+ values.map(v => '("'+ Object.values(v).join('", "') +'")').join(', '))
        const diff = (a,b,cb) => a.filter(a0 => b.find(b0 => cb(a0) !== cb(b0)))

        const compare = function(expected, actual) {
            //console.log( 'comparing', expected.map(v=>`[${v.type}]${v.name}`).join(', '), ' <> ', actual.map(v=>`[${v.type}]${v.name}`).join(', '))
            for(let i = 0, icn = expected.length; i < icn; i++) {
                const tableName = expected[i].name
                const act = actual.find(c => c.name === tableName)
                assert.ok( !!act, `Table "${tableName}" is missing`)
                // console.log( expected[i].columns, act.columns )
                cmpTableColumns(tableName, expected[i].columns, act.columns)

                // compare indexes
                for(let j = 0, jcn = expected[i].indexes ? expected[i].indexes.length : 0; j < jcn; j++){
                    const ix1 = expected[i].indexes[j], ix2 = act.indexes.find(ix => ix.name === ix1.name)
                    assert.ok( !!ix2, `Index "${tableName}.${ix1.name}" is missing`)

                    assert.equal( ix1.unique, ix2.unique, `Unique property of index "${tableName}.${ix1.name}" not match`)
                    assert.ok( !diff(ix1.columns, ix2.columns, a=>a.name).join(', '), `Columns in table index "${tableName}.${ix1.name}" are missing`)
                    assert.ok( !diff(ix2.columns, ix1.columns, a=>a.name).join(', '), `Needless columns in table index "${tableName}.${ix1.name}"`)
                }
                for(let j = 0, jcn = act.indexes.length; j < jcn; j++){
                    if( act.indexes[j].name.indexOf('sqlite_autoindex_') === 0) continue
                    const ix2 = expected[i].indexes.find(ix => ix.name === act.indexes[j].name)
                    assert.ok( !!ix2, `Table "${tableName}" has needless index "${ix2.name}"`)
                }
            }
        }
        const compareValues = async function(expected) {
            for(let i = 0, icn = expected.length; i < icn; i++) {
                const tableName = expected[i].name
                const values = await upd.get('SELECT * FROM `'+tableName+'` WHERE 1')
                assert.equal( expected[i].values ? expected[i].values.length : 0, values.length, `Rows of table "${tableName}" not equals`)

                for(let j = 0, jcn = values.length; j < jcn; j++){
                    const val = expected[i].values.find(v => v.id === values[i].id)
                    assert.ok(!!val, `Row value ID:${values[i].id}" is missing by table ${tableName}`)

                    for(let c in values[i]) {
                        assert.ok(typeof val[c] !== 'undefined', `Column ${tableName}.${c} of row ${values[i].id} not exists #${i}`)
                        assert.equal(val[c], values[i][c], `Column ${tableName}.${c} not equal in row id ${values[i].id}  #${i}`)
                    }
                }
            }
        }

        it('initial', async function(){
            await upd.update(initial)
            compare(initial, await upd.exportTables())

            for(let i = 0; i < initial.length; i++ )
                if( initial[i].values && initial[i].values.length > 0 )
                    await insertValues(initial[i].name, initial[i].values)

            await compareValues(initial)
        })

        it('add new table', async function(){
            initial.push({
                type: 'table',
                name: 'tbl3',
                columns : [
                    { cid: 0, name: 'id', type: 'INTERGER', notnull: 1, dflt_value: null, pk: 1 },
                    { cid: 1, name: 'name', type: 'VARCHAR(10)', notnull: 1, dflt_value: '"user name"', pk: 0 },
                    { cid: 2, name: 'email', type: 'VARCHAR(20)', notnull: 1, dflt_value: null, pk: 0 },
                    { cid: 3, name: 'date', type: 'DATETIME', notnull: 1, dflt_value: 'datatime', pk: 0 }
                ],
                indexes : [
                    { name: 'email', unique: true, columns: ['email'] },
                    { name: 'emaildate', unique: false, columns: ['email','date'] }
                ],
                values : [
                    { id: 1, name: 'uuu1', email: "@uuu1", date: '2019-01-10 05:06:40'},
                    { id: 2, name: 'uuu2', email: "@uuu2", date: '2019-02-02 01:01:01'},
                    { id: 3, name: 'uuu3', email: "@uuu3", date: '2019-02-02 01:01:01'}
                ]
            })
            await upd.update(initial)
            await insertValues(initial[2].name, initial[2].values)

            compare(initial, await upd.exportTables())
            await compareValues(initial)
        })

        it('trying to drop table',async function(){
            await upd.update(initial.slice(0,2))
            compare(initial, await upd.exportTables()) //!! no changes awaited, tables drop is not permitted
            await compareValues(initial)
        })

        it('add column', async function(){
            initial[2].columns.push({ cid: 4, name: 'enabled', type: 'TINYINT(1)', notnull: 1, dflt_value: '0', pk: 0 })
            await upd.update(initial)

            for(let i = 0; i < initial[2].values.length; i++)
                initial[2].values[i]['enabled'] = 0

            compare(initial, await upd.exportTables())
            await compareValues(initial)
            upd.log = function(){}
        })

        it('change column',async function(){
            initial[2].columns[2].type = 'VARCHAR(30)'
            await upd.update(initial)

            compare(initial, await upd.exportTables())
            await compareValues(initial)
        })

        it('drop column', async function(){
            initial[1].columns[1].type = 'VARCHAR(20)'
            const clone = JSON.parse(JSON.stringify(initial))
            clone[1].columns = clone[1].columns.slice(0,2)
            await upd.update(clone)

            compare(initial, await upd.exportTables()) //!! no changes awaited, column drop by this function not permitted
            await compareValues(initial)
        })

        it('add index', async function(){
            initial[2].indexes.push({ name: 'namedate', unique: true, columns: ['name','date'] })
            await upd.update(initial)

            compare(initial, await upd.exportTables())
            await compareValues(initial)
        })

        it('trying to drop index', async function(){
            const clone = JSON.parse(JSON.stringify(initial))
            clone[2].indexes = clone[2].indexes.slice(0,2)
            await upd.update(clone)

            compare(initial, await upd.exportTables()) //!! no changes awaited, index drop by this function not permitted
            await compareValues(initial)
        })
    })
})