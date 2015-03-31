/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
var assert = require('assert');
var async = require('async');
var q = require('q');
var uuid = require('node-uuid');
var request = require('request');

var hawkularMetricsRoot = 'http://localhost:8080/hawkular-metrics';

var cluster = require('cluster');
var numCPUs = require('os').cpus().length;
var iteration = 0;

function post(uri, body) {
  var deferred = q.defer();
  request({
    uri: hawkularMetricsRoot + uri,
    method: 'POST',
    headers:{'Content-Type': 'application/json'},
    body: JSON.stringify(body)
  }, function(err, response, body) {
    if (err) {
      return deferred.reject(err);
    }
    deferred.resolve(response);
  });

  return deferred.promise;
}

function callRest(task, callback) {
  var tenantId = uuid.v4();
  var metricId = uuid.v4();

  return q(post('/tenants',{
    id: tenantId
  }))
  .then(function(response) {
    assert.equal(response.statusCode, 201);

    return post('/' + tenantId + '/metrics/numeric', {
      id: metricId,
      tags: {a: '1', b: '2'},
      dataRetention: 24
    });
  })
  .then(function() {
    return post('/' + tenantId + '/metrics/numeric/data', {
      id: metricId,
      data: [{
          timestamp: new Date().getTime(),
          value: Math.random() * 100
        }]
    });
  })
  .then(function() {
    callback();
  })
  .catch(function(err){
    console.log(err);
    callback(err);
  });
}


if (cluster.isMaster) {
  for (var i = 0; i < numCPUs * 3; i++) {
    cluster.fork();
  }

   Object.keys(cluster.workers).forEach(function(id) {
    console.log('Worker running with ID : ' + cluster.workers[id].process.pid);
  });

  cluster.on('exit', function(worker, code, signal) {
    console.log('worker ' + worker.process.pid + ' died');
  });
} else {
  var rollingCallback = function(err) {
    iteration++;
    if (iteration%500 === 0) {
      console.log(iteration, cluster.worker.process.pid);
    }

    if (err) {
      console.log(err);
    }
  };

  var rolling = async.queue(callRest, 200);

  for(var i=0; i<100000; i++){
    rolling.push({}, rollingCallback);
  }
}
