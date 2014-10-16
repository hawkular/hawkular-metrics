/// <reference path="../../vendor/vendor.d.ts" />
var Services;
(function (Services) {
    'use strict';

    var MetricDataService = (function () {
        function MetricDataService($q, $rootScope, $http, $log, $localStorage, BASE_URL) {
            this.$q = $q;
            this.$rootScope = $rootScope;
            this.$http = $http;
            this.$log = $log;
            this.$localStorage = $localStorage;
            this.BASE_URL = BASE_URL;
        }
        MetricDataService.prototype.makeBaseUrl = function () {
            var baseUrl = 'http://' + this.$rootScope.$storage.server.replace(/['"]+/g, '') + ':' + this.$rootScope.$storage.port + this.BASE_URL;
            return baseUrl;
        };

        MetricDataService.prototype.getAllMetrics = function () {
            this.$log.info('-- Retrieving all metrics');
            var base = this.makeBaseUrl(), that = this, deferred = this.$q.defer();

            this.$http.get(base).success(function (data) {
                deferred.resolve(data);
            }).error(function (reason, status) {
                that.$log.error('Error Retrieving all metrics :' + status + ", " + reason);
                toastr.warning('No Metrics retrieved.');
                deferred.reject(status + " - " + reason);
            });

            return deferred.promise;
        };

        MetricDataService.prototype.getMetricsForTimeRange = function (id, startDate, endDate, buckets) {
            this.$log.info('-- Retrieving metrics data for id: ' + id);
            this.$log.info('-- Date Range: ' + startDate + ' - ' + endDate);
            var numBuckets = buckets || 60, base = this.makeBaseUrl(), deferred = this.$q.defer(), searchParams = {
                params: {
                    start: startDate.getTime(),
                    end: endDate.getTime(),
                    buckets: numBuckets
                }
            };

            if (startDate >= endDate) {
                this.$log.warn("Start date was after end date");
                deferred.reject("Start date was after end date");
            }

            this.$http.get(base + '/' + id, searchParams).success(function (data) {
                deferred.resolve(data);
            }).error(function (reason, status) {
                //this.$log.error('Error Loading Chart Data:' + status + ", " + reason);
                deferred.reject(status + " - " + reason);
            });

            return deferred.promise;
        };

        MetricDataService.prototype.insertSinglePayload = function (id, jsonPayload) {
            var url = this.makeBaseUrl(), deferred = this.$q.defer();
            this.$http.post(url + '/' + id, jsonPayload).success(function () {
                deferred.resolve("Success");
            }).error(function (response, status) {
                console.error("Error: " + status + " --> " + response);
                deferred.reject(status);
            });
            return deferred.promise;
        };

        MetricDataService.prototype.insertMultiplePayload = function (jsonPayload) {
            var url = this.makeBaseUrl(), deferred = this.$q.defer();
            this.$http.post(url + '/', jsonPayload).success(function () {
                deferred.resolve("Success");
            }).error(function (response, status) {
                console.error("Error: " + status + " --> " + response);
                deferred.reject(status);
            });
            return deferred.promise;
        };
        MetricDataService.$inject = ['$q', '$rootScope', '$http', '$log', '$localStorage', 'BASE_URL'];
        return MetricDataService;
    })();
    Services.MetricDataService = MetricDataService;

    angular.module('rhqm.services').service('metricDataService', MetricDataService);
})(Services || (Services = {}));
//# sourceMappingURL=metric-data-service.js.map
