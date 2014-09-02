/// <reference path="../../vendor/vendor.d.ts" />
'use strict';
angular.module('rhqm.services').factory('metricDataService', [
    '$q', '$rootScope', '$http', '$log', '$localStorage', 'BASE_URL', function ($q, $rootScope, $http, $log, $localStorage, BASE_URL) {
        function makeBaseUrl() {
            var baseUrl = 'http://' + $rootScope.$storage.server + ':' + $rootScope.$storage.port + BASE_URL;
            return baseUrl;
        }

        return {
            getMetricsForTimeRange: function (id, startDate, endDate, buckets) {
                $log.info("-- Retrieving metrics data for id: " + id);
                $log.info("-- Date Range: " + new Date(startDate) + " - " + new Date(endDate));
                var numBuckets = buckets || 60, base = makeBaseUrl(), deferred = $q.defer(), searchParams = {
                    params: {
                        start: startDate.getTime(),
                        end: endDate.getTime(),
                        buckets: numBuckets
                    }
                };

                if (startDate >= endDate) {
                    $log.warn("Start date was after end date");
                    deferred.reject("Start date was after end date");
                }

                $http.get(base + '/' + id, searchParams).success(function (data) {
                    deferred.resolve(data);
                }).error(function (reason, status) {
                    $log.error('Error Loading Chart Data:' + status + ", " + reason);
                    deferred.reject(status + " - " + reason);
                });

                return deferred.promise;
            },
            insertSinglePayload: function (id, jsonPayload) {
                var url = makeBaseUrl(), deferred = $q.defer();
                $http.post(url + '/' + id, jsonPayload).success(function () {
                    deferred.resolve("Success");
                }).error(function (response, status) {
                    console.error("Error: " + status + " --> " + response);
                    deferred.reject(status);
                });
                return deferred.promise;
            },
            insertMultiplePayload: function (jsonPayload) {
                var url = makeBaseUrl(), deferred = $q.defer();
                $http.post(url + '/', jsonPayload).success(function () {
                    deferred.resolve("Success");
                }).error(function (response, status) {
                    console.error("Error: " + status + " --> " + response);
                    deferred.reject(status);
                });
                return deferred.promise;
            }
        };
    }]);
//# sourceMappingURL=metric-data-service.js.map
