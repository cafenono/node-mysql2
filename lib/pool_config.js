'use strict';

const ConnectionConfig = require('./connection_config.js');

class PoolConfig {
  constructor(options) {
    if (typeof options === 'string') {
      options = ConnectionConfig.parseUrl(options);
    }

    this.acquireTimeout =
      options.acquireTimeout === undefined
        ? 10 * 1000
        : Number(options.acquireTimeout);
    this.connectionConfig = new ConnectionConfig(options);
    this.waitForConnections =
      options.waitForConnections === undefined
        ? true
        : Boolean(options.waitForConnections);
    this.connectionLimit = isNaN(options.connectionLimit)
      ? 10
      : Number(options.connectionLimit);
    this.connectionTimeout = isNaN(options.connectionTimeout)
      ? 0
      : Number(options.connectionTimeout);
    this.queueLimit = isNaN(options.queueLimit)
      ? 0
      : Number(options.queueLimit);
  }

  clone() {
    const connectionConfig = new ConnectionConfig(this.connectionConfig);
    return connectionConfig;
  }
}

module.exports = PoolConfig;
