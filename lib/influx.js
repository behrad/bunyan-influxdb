'use strict';

var util = require('util');
var stream = require('stream');
var Writable = stream.Writable;
var cargo = require('async-timed-cargo');
var async = require('async');
var Influx = require('influx');


function InfluxDBStream(options) {
  this.options = options || {};
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
  var transformed = this.options.transform(chunk);
  if (!transformed || transformed.length < 3) {
    return;
  }

  var cargoItem = {
    measurement: transformed[0],
    fields: transformed[1],
    tags: transformed[2],
    db: transformed[2].appId || this.options.influx.database
  };

  if(transformed[3]) {
    cargoItem.timestamp = transformed[3];
  }
  delete transformed[2].appId;

  this.cargo.push(cargoItem, function (err) {
    err && console.error('bunyan-influxdb ', err.message);
  });

};


InfluxDBStream.prototype.processCargo = function (tasks, callback) {
  var self = this;
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
    async.retry(
      {
        times: self.options.tries || 1,
        interval: self.options.tryInterval || 5000
      },

      function (clbk) {
        var options = {};
        if (db !== 'nodb') {
          options.database = db;
        }
        self.client.writePoints(dbs[db], options)
          .then(clbk)
          .catch(function (err) {
            // err && console.error("Influxdb write error ", err.message);
            clbk(err);
          })
      },

      callback
    );
  });
};


module.exports = function (options) {
  if (!options) {
    options = {};
  }
  return new InfluxDBStream(options);
};