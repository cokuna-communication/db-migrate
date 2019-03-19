/**
 *
 */
const assert = require('assert')
//const sqlite3 = require('@journeyapps/sqlcipher').verbose()
const sqlite3 = require('sqlite3').verbose()
const DBMigrate = require('../migrate.js')

describe('DataBase Migration', function() {
    const db = new sqlite3.Database(':memory:')
    const upd = new DBMigrate( db )

    const initial = [
        {
            type: 'table',
            name: 'tbl1',
            columns : [
                { cid: 0, name: 'id', type: 'INTERGER', notnull: 1, dflt_value: null, pk: 0 },
                { cid: 1, name: 'date', type: 'datetime', notnull: 1, dflt_value: 'timestamp', pk: 0 }
            ]
        },  {
            type: "trigger",
            name : 'temp_trigger',
            target : 'tbl1',
            action : 'BEFORE UPDATE',
            code   : 'INSERT INTO update_log (id) VALUES (new.id);'
        }
    ]

    it( 'get db version', async function(){
        assert.ok(/^[0-9]+\.[0-9]+\.[0-9]+$/.test(await upd.getVersion()))
    })

    it( 'update', async function(){
        await upd.update( initial )
        const dump = await new Promise((resolve,reject) => {
            db.all('SELECT * FROM sqlite_master WHERE type="table" or type="trigger" ORDER BY type ASC, name ASC', (err,rows) => err ? reject(err): resolve(rows))
        })

        assert.equal(dump.length, initial.length, 'Count of imported data not as excepcted')
        for( let i = 0; i < initial.length; i++ ) {
            assert.equal(dump[i].type, initial[i].type)
            assert.equal(dump[i].name, initial[i].name)
        }
    })

    it( 'export', async function(){
        const dump = await upd.export( initial )
        assert.equal(dump.length, initial.length, 'Count of exported data not as excepcted')

        for( let i = 0; i < initial.length; i++ ) {
            assert.equal(dump[i].type, initial[i].type)
            assert.equal(dump[i].name, initial[i].name)
        }
    })

})