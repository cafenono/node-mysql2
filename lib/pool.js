'use strict';

const process = require('process');
const mysql = require('../index.js');

const EventEmitter = require('events').EventEmitter;
const PoolConnection = require('./pool_connection.js');
const Connection = require('./connection.js');

function spliceConnection(array, connection) {
  let index;

  if ((index = array.indexOf(connection)) !== -1) {
    array.splice(index, 1);
  }
}

class Pool extends EventEmitter {
  constructor(options) {
    super();
    this.config = options.config;
    this.config.connectionConfig.pool = this;

    // use native array intead of denque
    this._acquiringConnections = [];
    this._allConnections = [];
    this._freeConnections = [];
    this._connectionQueue = [];
    this._closed = false;
  }

  promise(promiseImpl) {
    const PromisePool = require('../promise').PromisePool;
    return new PromisePool(this, promiseImpl);
  }

  getConnection(cb) {
    if (this._closed) {
      const err = new Error('Pool is closed.');
      err.code = 'POOL_CLOSED';
      return process.nextTick(() => cb(err));
    }

    let connection;

    // 풀에 여유가 있다면 앞에서 꺼내옴.
    if (this._freeConnections.length > 0) {
      connection = this._freeConnections.shift();
      connection._endTimer();
      return this.acquireConnection(connection, cb);
    }

    // 커넥션 생성
    if (
      this.config.connectionLimit === 0 ||
      this._allConnections.length < this.config.connectionLimit
    ) {
      connection = new PoolConnection(this, {
        config: this.config.clone()
      });

      this._acquiringConnections.push(connection);
      this._allConnections.push(connection);

      return connection.connect(err => {
        spliceConnection(this._acquiringConnections, connection);

        if (this._closed) {
          err = new Error('Pool is closed.');
          err.code = 'POOL_CLOSED';
        }

        if (err) {
          this._purgeConnection(connection);
          return cb(err);
        }

        this.emit('connection', connection);
        this.emit('acquire', connection);
        return cb(null, connection);
      });
    }

    if (!this.config.waitForConnections) {
      const err = new Error('No connections available.');
      err.code = 'POOL_CONNLIMIT';
      return process.nextTick(() => cb(err));
    }

    if (
      this.config.queueLimit &&
      this._connectionQueue.length >= this.config.queueLimit
    ) {
      const err = new Error('Queue limit reached.');
      err.code = 'POOL_ENQUEUELIMIT';
      return process.nextTick(() => cb(err));
    }

    // Bind to domain, as dequeue will likely occur in a different domain
    const callback = process.domain ? process.domain.bind(cb) : cb;
    this._connectionQueue.push(callback);
    this.emit('enqueue');
  }

  acquireConnection(connection, cb) {
    if (connection._pool !== this) {
      throw new Error('Connection acquired from wrong pool.');
    }

    const changeUser = this._needsChangeUser(connection);

    this._acquiringConnections.push(connection);

    const onOperationComplete = err => {
      spliceConnection(this._acquiringConnections, connection);

      if (this._closed) {
        err = new Error('Pool is closed.');
        err.code = 'POOL_CLOSED';
      }

      if (err) {
        this._connectionQueue.unshift(cb);
        this._purgeConnection(connection);
        return;
      }

      if (changeUser) {
        this.emit('connection', connection);
      }

      this.emit('acquire', connection);
      cb(null, connection);
    };

    if (changeUser) {
      connection.config = this.config.clone();
      connection.changeUser(onOperationComplete);
    } else {
      connection.ping(onOperationComplete);
    }
  }

  releaseConnection(connection) {
    if (this._acquiringConnections.indexOf(connection) !== -1 || this._allConnections.indexOf(connection) === -1) {
      return;
    }

    if (connection._pool) {
      if (connection._pool !== this) {
        throw new Error('Connection released to wrong pool.');
      }

      if (this._freeConnections.indexOf(connection) !== -1) {
        throw new Error('Connection already released.');
      } else if (this.config.connectionTimeout && (Date.now() - connection._startTime) > this.config.connectionTimeout) {
        this._purgeConnection(connection);
      } else {
        this._freeConnections.push(connection);
        connection._startTimer();
        this.emit('release', connection);
      }
    }

    // empty the connection queue
    if (this._closed) {
      this._connectionQueue.forEach(cb => {
        const err = new Error('Pool is closed');
        err.code = 'POOL_CLOSED';
        process.nextTick(() => cb(err));
      });
    } else if (this._connectionQueue.length) {
      this.getConnection(this._connectionQueue.shift());
    }
  }

  end(cb) {
    this._closed = true;
    if (typeof cb !== 'function') {
      cb = function(err) {
        if (err) {
          throw err;
        }
      };
    }
    let calledBack = false;
    let waitingClose = 0;
    let connection;

    const endCB = function(err) {
      if (calledBack) {
        return;
      }

      if (err || --waitingClose <= 0) {
        calledBack = true;
        cb(err);
        return;
      }
    }.bind(this);

    if (this._allConnections.length === 0) {
      endCB();
      return;
    }

    for (let i = 0; i < this._allConnections.length; i++) {
      waitingClose++;
      connection = this._allConnections[i];
      this._purgeConnection(connection, endCB);
    }

    if (waitingClose === 0) {
      process.nextTick(() => endCB);
    }
  }

  query(sql, values, cb) {
    const cmdQuery = Connection.createQuery(
      sql,
      values,
      cb,
      this.config.connectionConfig
    );
    if (typeof cmdQuery.namedPlaceholders === 'undefined') {
      cmdQuery.namedPlaceholders = this.config.connectionConfig.namedPlaceholders;
    }
    this.getConnection((err, conn) => {
      if (err) {
        if (typeof cmdQuery.onResult === 'function') {
          cmdQuery.onResult(err);
        } else {
          cmdQuery.emit('error', err);
        }
        return;
      }
      try {
        conn.query(cmdQuery).once('end', () => {
          conn.release();
        }).once('error', () => {
          conn.destroy();
        });
      } catch (e) {
        conn.destroy();
        throw e;
      }
    });
    return cmdQuery;
  }

  execute(sql, values, cb) {
    // TODO construct execute command first here and pass it to connection.execute
    // so that polymorphic arguments logic is there in one place
    if (typeof values === 'function') {
      cb = values;
      values = [];
    }
    this.getConnection((err, conn) => {
      if (err) {
        return cb(err);
      }
      try {
        conn.execute(sql, values, cb).once('end', () => {
          conn.release();
        });
      } catch (e) {
        conn.release();
        throw e;
      }
    });
  }

  _needsChangeUser(connection) {
    const connectionConfig = connection.config;
    const poolConfig = this.config.connectionConfig;

    // check if changeUser values are different
    return connectionConfig.user !== poolConfig.user
      || connectionConfig.database !== poolConfig.database
      || connectionConfig.password !== poolConfig.password
      || connectionConfig.charsetNumber !== poolConfig.charsetNumber;
  }

  _purgeConnection(connection, callback) {
    const cb = callback || function () {};
    spliceConnection(this._allConnections, connection);
    spliceConnection(this._freeConnections, connection);
    connection.destroy();
    connection._realEnd(cb);
  }

  format(sql, values) {
    return mysql.format(
      sql,
      values,
      this.config.connectionConfig.stringifyObjects,
      this.config.connectionConfig.timezone
    );
  }

  escape(value) {
    return mysql.escape(
      value,
      this.config.connectionConfig.stringifyObjects,
      this.config.connectionConfig.timezone
    );
  }

  escapeId(value) {
    return mysql.escapeId(value, false);
  }
}

module.exports = Pool;
