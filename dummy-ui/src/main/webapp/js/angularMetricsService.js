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

    return service;

});