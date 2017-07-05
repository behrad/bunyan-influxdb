'use strict';

var util = require('util');
var stream = require('stream');
var Writable = stream.Writable;
var cargo = require('async-timed-cargo');
var async = require('async');
var Influx = require('influx');


function InfluxDBStream(options) {
  this.options = options || {};
  this.clients = {};
  this.client = new Influx.InfluxDB(options.influx);

  this.cargo = cargo(
    this.processCargo.bind(this),
    this.options.batchLimit || 5000,
    this.options.batchInterval || 2000
  );
}

util.inherits(InfluxDBStream, Writable);


InfluxDBStream.prototype.write = function (chunk) {
  chunk = chunk || {};
  if (!this.options.transform) {
    return;
  }
  var list = this.options.transform(chunk);
  if (list && list.length > 0 && list[0].splice) {
  } else {
    list = [list];
  }
  var that = this;
  list.map(function (transformed) {
    if (!transformed || transformed.length < 3) {
      return;
    }

    if (typeof transformed[1] !== 'object') {
      transformed[1] = {value: transformed[1]}
    }

    var cargoItem = {
      measurement: transformed[0],
      fields: transformed[1],
      tags: transformed[2],
      db: transformed[2].appId || that.options.influx.database
    };

    if(transformed[3]) {
      cargoItem.timestamp = transformed[3];
    }
    delete transformed[2].appId;

    that.cargo.push(cargoItem, function (err) {
      err && console.error('bunyan-influxdb ', err.message);
    });
  });
};


InfluxDBStream.prototype.processCargo = function (tasks, callback) {
  var dbs = {};

  tasks.map(function (t) {
    var db_key = 'nodb';
    if(t.db) {
      db_key = t.db;
      delete t.db;
    }
    dbs[db_key] = dbs[db_key] || [];
    dbs[db_key].push(t);
  });

  Object.keys(dbs).map(function(db){
    this.writePointsWithRetry(db, dbs[db], callback);
  }.bind(this));
};

InfluxDBStream.prototype.writePointsWithRetry = function (db, points, callback) {
  async.retry(
    {
      times: this.options.tries || 1,
      interval: this.options.tryInterval || 5000
    },

    function (clbk) {
      var options = {};
      if (db !== 'nodb') {
        options.database = db;
      }
      this
        .getClient(db)
        .writePoints(points, options)
        .then(clbk)
        .catch(clbk)
    }.bind(this),

    callback
  );

};

InfluxDBStream.prototype.getClient = function (db) {
  // if (!db || db === 'nodb') {
  //   return this.client;
  // }
  // if (!this.clients[db]) {
  //   this.clients[db] = new Influx.InfluxDB(this.options.influx);
  // }
  // return this.clients[db];
  return this.client;
};


module.exports = function (options) {
  if (!options) {
    options = {};
  }
  return new InfluxDBStream(options);
};