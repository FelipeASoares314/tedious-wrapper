'use-strict'
const { Connection, Request, TYPES } = require('tedious')
const uuidv4 = require('uuid/v4')
// 1 pool para todas as instancias
// Os objs do pool devem ser { id: uuid, used: t/f, conn: connection, ready: t/f }
let pool = []

const defaults = {
  poolSize: 10,
  timeout: 1500
}

const timeout = (time = 100) => new Promise((resolve) => setTimeout(() => resolve(), time))

function MSClient(params = {}) {
  this.poolSize = params.poolSize || defaults.poolSize
  this.queryTimeout = params.timeout || defaults.timeout
  this.userName = params.userName
  this.password = params.password
  this.url = params.url
}

MSClient.prototype.createConnection = function () {
  return new Promise((resolve, reject) => {
    let conection = new Connection({ userName: this.userName, password: this.password, server: this.url })
    
    conection.on('connect', (err) => {
      if (err) {
        return reject(err)
      } else {
        let con = {
          id: uuidv4(),
          used: true,
          conn: connection,
          ready: true
        }
        pool.push(con)
        resolve(con)
      }
    })

    conection.on('end', () => {
      let index = pool.findIndex(o => o.id == conn.id)
      if (index != -1) {
        pool.splice(index, 1)
      }
    })
  })
}

MSClient.prototype.canGetConn = async function(sinceRequestTime = new Date()) {
  if (pool.find(o => !o.used && o.ready) || pool.length < this.poolSize) {
    return true
  }

  let now = new Date()
  if ((now - sinceRequestTime) > this.queryTimeout) {
    throw new Error('Could not get conection')
  }

  await timeout()
  return this.canGetConn(sinceRequestTime)
}

MSClient.prototype.getConnection = async function() {
  try {
    await this.canGetConn()
    let conn = pool.find(o => !o.used)
    return conn
      ? conn
      : createConnection()
  } catch(err) {
    throw err
  }
}

const getRequest = (err) => new Promise((resolve, reject) => {
  if (err) reject(err)
  resolve()
})

MSClient.prototype.query = function(sql, params = []) {
  return new Promise(async (resolve, reject) => {
    try {
      let conn = await this.getConnection()
      let request = await new Request(sql, getRequest)

      let cols = []
      request.on('row', (columns) => {
        let row = {}
        columns.forEach(c => {
          row[c.metadata.colName] = c.value
        })

        cols.push(row)
      })

      request.on('requestCompleted', () => {
        conn.conn.reset((err) => {
          if (err) {
            console.error('Erro ao soltar conexão de id', conn.id)
            conn.conn.close()
          }
          conn.used = false // release da conexão para o pool
        })
        resolve(cols)
      })

      request.on('error', (err) => { reject(err) })

      params.map(p => {
        request.addParameter(p.nome, TYPES[p.type], p.value)
      })

      conn.conn.execSql(request)
    } catch(err) {
      reject(err)
    }
  })
}

module.exports = MSClient
