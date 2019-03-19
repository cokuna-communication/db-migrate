/**
 *
 */
const assert = require('assert')
//const sqlite3 = require('@journeyapps/sqlcipher').verbose()
const sqlite3 = require('sqlite3').verbose()
const DBMigrate = require('../migrate.js')

describe('DataBase Migration Triggers', function() {
    const db = new sqlite3.Database(':memory:')
    const upd = new DBMigrate( db  )/*, console.log*/

    const initial = {
        type: "trigger",
        name : 'temp_trigger',
        target : 'temp_table',
        action : 'BEFORE UPDATE',
        code   : 'INSERT INTO update_log (id) VALUES (new.id);'
    }
    const secondary = {
        type: "trigger",
        name : 'temp_trigger2',
        target : 'temp_table',
        action : 'AFTER INSERT',
        code   : 'INSERT INTO update_log (id) VALUES (new.id);'
    }


    const addTable = () => new Promise((resolve,reject) => {
        const stmt = `CREATE TABLE IF NOT EXISTS temp_table (
            id int(11) NOT NULL,
            date datetime NOT NULL DEFAULT(current_timestamp)
        );`

        db.serialize(function(){
            db.exec(stmt, err => err ? reject(err) : resolve(true))
        })
    })

    const getTriggerRaw = (name) => new Promise((resolve, reject) => {
        db.get(`SELECT * FROM sqlite_master WHERE name="${name}" and type="trigger" LIMIT 1;`, (err,row) => {
            if( err ) reject(err)
            resolve(row)
        })
    })

    const getAllTriggerRaw = () => new Promise((resolve, reject) => {
        db.all(`SELECT * FROM sqlite_master WHERE type="trigger" `, (err,rows) => {
            if( err ) reject(err)
            resolve(rows)
        })
    })

    before(async function(){
        await addTable()
    })


    it('create', async function(){
        assert.ok( !await getTriggerRaw(initial.name) , 'already exists')

        await upd.createTrigger(initial.name, initial.action, initial.target, initial.code)
        assert.ok( !!await getTriggerRaw(initial.name) , 'wasnt created')
    })


    it('update', async function(){
        initial.action = 'AFTER UPDATE'

        await upd.updateTrigger(initial.name, initial.action, initial.target, initial.code)
        const expected = await getTriggerRaw(initial.name)
        assert.ok(expected.sql.indexOf('temp_trigger AFTER UPDATE ON temp_table') > 0, 'action wasnt changed')
    })


    it('info', async function(){
        const expected = await upd.getTriggerInfo(initial.name)
        assert.ok( !!expected, 'not exists')
        assert.equal( expected.type, initial.type, 'type not match')
        assert.equal( expected.name, initial.name, 'name not match')
        assert.equal( expected.target, initial.target, 'target not match')
        assert.equal( expected.action, initial.action, 'action not match')
        assert.equal( expected.code, initial.code, 'code not match')
    })

    it('import', async function(){
        initial.action = 'BEFORE UPDATE'
        const list = [ initial , secondary ]
        await upd.updateTriggers(list)

        const dump = await getAllTriggerRaw()
        assert.equal( dump.length, list.length, 'exports not macth')

        for(let i = 0; i < dump.length; i++ ) {
            const expected = dump[i]
            assert.equal( expected.type, list[i].type, 'type not match')
            assert.equal( expected.name, list[i].name, 'name not match')
            assert.ok( expected.sql.indexOf( `ON ${list[i].target}` ) > 0, 'target not match')
            assert.ok( expected.sql.indexOf( `${list[i].name} ${list[i].action} ON ` ) > 0, 'action not match')
            assert.ok( expected.sql.indexOf( list[i].code ) > 0, 'code not match')
        }
    })

    it('export', async function(){
        const list = [ initial , secondary ]
        const dump = await upd.exportTriggers()
        assert.equal( dump.length, list.length, 'exports not macth')

        for(let i = 0; i < dump.length; i++ ) {
            const expected = dump[i]
            assert.equal( expected.type, list[i].type, 'type not match')
            assert.equal( expected.name, list[i].name, 'name not match')
            assert.equal( expected.target, list[i].target, 'target not match')
            assert.equal( expected.action, list[i].action, 'action not match')
            assert.equal( expected.code, list[i].code, 'code not match')
        }
    })

    it('drop', async function(){
        assert.ok( !!await getTriggerRaw(initial.name), 'not exists' )

        await upd.dropTrigger(initial.name)
        const expected = await getTriggerRaw(initial.name)
        assert.ok( !expected, 'wasnt dropped' )
    })

})