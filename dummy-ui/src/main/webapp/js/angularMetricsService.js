var app = angular.module('rhqApp');
app.factory('metricSearchService', function($http) {

    var service = {};

    /**
     @param keyword - The keyword to search for.
     @param callback - A function to call when the search is complete.
     The function will be passed a single argument which is an array
     of resources items matching the keyword.
     */
    service.findMetrics = function(keyword, callback) {

        $http.get('../rhq-metrics/metrics/.json',
                { params :
                    {q : keyword}
                })
                .success(function(data) {
                    callback(data);
                })
                .error(function(data, status, headers, config) {
                    console.log(status)
                })
    };

    service.loadMetrics = function(callback) {
        $http.get('../rhq-metrics/metrics/.json')
                .success(function(data) {
                    callback(data);
                })
                .error(function(data, status, headers, config) {
                    console.log(status)
                })
    };

    service.populateDummy = function(callback) {

        var now = Date.now();
        var myData = [
            {"timestamp" : now-810000,
             "id" : "dummy",
             "value": 100},
            {"timestamp" : now-800000,
             "id" : "dummy",
             "value": 150},
            {"timestamp" : now-710000,
             "id" : "dummy",
             "value": 1000},
            {"timestamp" : now-700000,
             "id" : "dummy",
             "value": 1500},
            {"timestamp" : now-610000,
             "id" : "dummy",
             "value": 1237},
            {"timestamp" : now-600000,
             "id" : "dummy",
             "value": 1400},
            {"timestamp" : now-510000,
             "id" : "dummy",
             "value": 1234},
            {"timestamp" : now-500000,
             "id" : "dummy",
             "value": 1534},
            {"timestamp" : now-160000,
             "id" : "dummy",
             "value" : 2300},
            {"timestamp" : now-150000,
             "id" : "dummy",
             "value" : 345},
            {"timestamp" : now-110000,
             "id" : "dummy",
             "value" : 50},
            {"timestamp" : now-100000,
             "id" : "dummy",
             "value" : 42},
            {"timestamp" : now-710000,
             "id" : "dummy",
             "value": 100},
            {"timestamp" : now-700000,
             "id" : "dummy",
             "value": 150},
            {"timestamp" : now-510000,
             "id" : "dummy",
             "value": 1000},
            {"timestamp" : now-500000,
             "id" : "dummy",
             "value": 1500},
            {"timestamp" : now-210000,
             "id" : "dummy",
             "value": 1237},
            {"timestamp" : now-200000,
             "id" : "dummy",
             "value": 1400},
            {"timestamp" : now-110000,
             "id" : "dummy",
             "value": 1234},
            {"timestamp" : now-100000,
             "id" : "dummy",
             "value": 1534},
            {"timestamp" : now-60000,
             "id" : "dummy",
             "value" : 2300},
            {"timestamp" : now-50000,
             "id" : "dummy",
             "value" : 345},
            {"timestamp" : now-10000,
             "id" : "dummy",
             "value" : 50},
            {"timestamp" : now,
             "id" : "dummy",
             "value" : 42}
        ];

        $http.post('../rhq-metrics/metrics', myData)
            .success(function(data) {
                console.log("Successfully pushed dummy data");
                callback();
            })
            .error(function(result) {
                console.error("Data push failed: " + result);
            })
    };

    return service;

});