'use strict';

var stream = require('stream');
var util = require('util');
var Writable = stream.Writable;
var influx = require('influx');


function InfluxDBStream(options) {
  this.options = options || {};
  this.clientInflux = influx(options.influx);  
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

  if( point.length && point.split ) {
    point = '"' + point + '"';
  }

  //console.log( "Write Point ", series, point, tags, time );
  this.clientInflux.writePoint( series, point, tags, time, function(err) {
    if(err) {
      console.error( "Error ", err );
    }
  });

};


module.exports = function (options) {
    if (!options) {
        options = {};
    }
    return new InfluxDBStream(options);
};

