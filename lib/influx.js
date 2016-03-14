'use strict';

var stream   = require('stream');
var util     = require('util');
var Writable = stream.Writable;
var influx   = require('influx');
var cargo    = require('async-timed-cargo');
var async    = require('async');


function InfluxDBStream(options) {
  this.options = options || {};
  this.clientInflux = influx(options.influx);
  this.cargo = cargo( function (tasks, callback) {
    var points = tasks.map(function(t){ return t.points;});
    async.retry({
      times: this.options.tries || 1,
      interval: this.options.tryInterval || 3000
    }, function( clbk, results ) {
      this.clientInflux.writePoints( tasks[0].series, points, function(err) {
        if(err) {
          console.error( "Influxdb write error ", err );
        }
        clbk && clbk( err );
      }.bind(this));
    }.bind(this), function(err, result) {
      callback(err, result);
    }.bind(this));
  }, 500, 1000);
}


util.inherits(InfluxDBStream, Writable);



InfluxDBStream.prototype.write = function( chunk ) {
  chunk = chunk || {};
  if( !this.options.transform ) {
    return;
  }
  var transformed = this.options.transform(chunk);
  if( !transformed || transformed.length < 3 ) {
    return;
  }

  var series = transformed[0];
  var point = transformed[1];
  var tags = transformed[2];
  var time = transformed[3] || Date.now();

  this.cargo.push({
    series: series,
    points: [{value: point, time : time}, tags]
  },function(err) {
      //console.log('');
    }
  );

};


module.exports = function (options) {
  if (!options) {
      options = {};
  }
  return new InfluxDBStream(options);
};

