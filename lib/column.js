/**
 * @param {any} data
 * @returns {Column}
 */
const Column = function(data){
    if( typeof data === "string" ) data = { name : data }
    this.cid = typeof data.cid !== "undefined" ? data.cid : null
    this.name = data.name
    this.type = data.type || 'varchar(10)'
    this.notnull = (typeof data.notnull !== "undefined" && data.notnull) ? 1 : 0
    this.dflt_value = data.dflt_value || null
    this.pk = (typeof data.pk !== "undefined" && data.pk) ? 1 : 0
}
//--
module.exports = Column