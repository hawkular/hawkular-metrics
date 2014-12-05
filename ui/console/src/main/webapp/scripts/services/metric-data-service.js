/// <reference path="../../vendor/vendor.d.ts" />
var Services;
(function (Services) {
    'use strict';

    var MetricDataService = (function () {
        function MetricDataService($q, $rootScope, $http, $localStorage, BASE_URL, TENANT_ID) {
            this.$q = $q;
            this.$rootScope = $rootScope;
            this.$http = $http;
            this.$localStorage = $localStorage;
            this.BASE_URL = BASE_URL;
            this.TENANT_ID = TENANT_ID;
        }
        MetricDataService.prototype.getBaseUrl = function () {
            var baseUrl = 'http://' + this.$rootScope.$storage.server.replace(/['"]+/g, '') + ':' + this.$rootScope.$storage.port + this.BASE_URL + '/' + this.TENANT_ID;
            return baseUrl;
        };

        MetricDataService.prototype.getAllMetrics = function () {
            console.info('-- Retrieving all metrics');
            var base = this.getBaseUrl() + '/metrics/?type=num', deferred = this.$q.defer();

            this.$http.get(base).success(function (data) {
                deferred.resolve(data);
            }).error(function (reason, status) {
                console.error('Error Retrieving all metrics :' + status + ", " + reason);
                toastr.warning('No Metrics retrieved.');
                deferred.reject(status + " - " + reason);
            });

            return deferred.promise;
        };

        MetricDataService.prototype.getMetricsForTimeRange = function (id, startDate, endDate, buckets) {
            console.info('-- Retrieving metrics data for id: ' + id);
            console.info('-- Date Range: ' + startDate + ' - ' + endDate);
            var numBuckets = buckets || 60, base = this.getBaseUrl(), deferred = this.$q.defer(), searchParams = {
                params: {
                    start: startDate.getTime(),
                    end: endDate.getTime(),
                    buckets: numBuckets
                }
            };

            if (startDate >= endDate) {
                console.warn("Start date was after end date");
                deferred.reject("Start date was after end date");
            }

            this.$http.get(base + '/metrics/numeric/' + id + '/data', searchParams).success(function (data) {
                deferred.resolve(data);
            }).error(function (reason, status) {
                console.error('Error Loading Chart Data:' + status + ", " + reason);
                deferred.reject(status + " - " + reason);
            });

            return deferred.promise;
        };
        MetricDataService.$inject = ['$q', '$rootScope', '$http', '$localStorage', 'BASE_URL', 'TENANT_ID'];
        return MetricDataService;
    })();
    Services.MetricDataService = MetricDataService;

    angular.module('rhqm.services').service('metricDataService', MetricDataService);
})(Services || (Services = {}));
//# sourceMappingURL=metric-data-service.js.map
