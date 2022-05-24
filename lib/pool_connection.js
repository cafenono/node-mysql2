'use strict';

const Connection = require('../index.js').Connection;
const Events = require('events');

class PoolConnection extends Connection {
  constructor(pool, options) {
    super(options);
    this._pool = pool;

    if (Events.usingDomains) {
      this.domain = pool.domain;
    }

    this._startTime = Date.now();

    // When a fatal error occurs the connection's protocol ends, which will cause
    // the connection to end as well, thus we only need to watch for the end event
    // and we will be notified of disconnects.
    // REVIEW: Moved to `once`
    this.once('end', () => {
      this._removeFromPool();
    });
    this.once('error', () => {
      this._removeFromPool();
    });
  }

  release() {
    if (!this._pool || this._pool._closed) {
      return;
    }
    this._pool.releaseConnection(this);
  }

  promise(promiseImpl) {
    const PromisePoolConnection = require('../promise').PromisePoolConnection;
    return new PromisePoolConnection(this, promiseImpl);
  }

  end() {
    const err = new Error(
      'Calling conn.end() to release a pooled connection is ' +
        'deprecated. In next version calling conn.end() will be ' +
        'restored to default conn.end() behavior. Use ' +
        'conn.release() instead.'
    );
    this.emit('warn', err);
    // eslint-disable-next-line no-console
    console.warn(err.message);
    this.release();
  }

  destroy() {
    this._removeFromPool();
  }

  _removeFromPool() {
    if (this._closed()) {
      return;
    }

    const pool = this._pool;
    this._pool = null;
    pool._purgeConnection(this);
  }

  _closed() {
    return !this._pool || this._pool._closed;
  }

  _startTimer() {
    if (this._timer || this._closed()) {
      return;
    }

    this._startTime = Date.now();
    const connectionTimeout = this._pool.config.connectionTimeout;

    if (connectionTimeout) {
      this._timer = setTimeout(() => {
        this.destroy();
      }, connectionTimeout);
    }
  }

  _endTimer() {
    if (this._timer) {
      clearTimeout(this._timer);
      this._timer = null;
    }
  }
}

PoolConnection.statementKey = Connection.statementKey;
module.exports = PoolConnection;

// TODO: Remove this when we are removing PoolConnection#end
PoolConnection.prototype._realEnd = Connection.prototype.end;
