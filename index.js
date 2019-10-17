'use-strict'
const { Connection, Request, TYPES } = require('tedious')
const uuidv4 = require('uuid/v4')
// 1 pool para todas as instancias
// Os objs do pool devem ser { id: uuid, used: t/f, conn: connection, ready: t/f }
let pool = []

const timeout = (time = 100) => new Promise((resolve) => setTimeout(() => resolve(), time))

function MSClient(params) {
  this.config = params
}

MSClient.prototype.createConnection = function () {
  return new Promise((resolve, reject) => {
    let conection = new Connection(this.config)

    let con = {
      id: uuidv4(),
      used: true,
      conn: conection,
      ready: false
    }

    pool.push(con)
    conection.on('connect', err => {
      if (err) {
        reject(err)
      } else {
        con.ready = true
        resolve(con)
      }
    })

    conection.on('end', () => {
      let index = pool.findIndex(o => o.id == con.id)
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
    let conn = pool.find(o => !o.used && o.ready)

    return conn
      ? new Promise(resolve => {
        conn.used = true
        resolve(conn)
      })
      : this.createConnection()
  } catch(err) {
    throw err
  }
}

MSClient.prototype.query = function(sql, params = []) {
  return new Promise(async (resolve, reject) => {
    try {
      let conn = await this.getConnection()
      let cols = []
      let request = await new Request(sql, err => new Promise(() => {
        if (err) {
          reject(err)
        } else {
          conn.used = false
          resolve(cols)
        }
      }))

      request.on('row', columns => {
        let row = columns.reduce((acc, atual) => {
          acc[atual.metadata.colName] = atual.value
          return acc
        }, {})
        cols.push(row)
      })

      request.on('requestCompleted', () => {
        conn.conn.reset(err => {
          if (err) {
            conn.ready = false
            conn.conn.close()
            reject(err)
          }
        })
      })

      request.on('error', (err) => reject(err))
      params.map(p => request.addParameter(p.name, TYPES[p.type], p.value))
      conn.conn.execSql(request)
    } catch(err) {
      reject(err)
    }
  })
}

module.exports = MSClient
