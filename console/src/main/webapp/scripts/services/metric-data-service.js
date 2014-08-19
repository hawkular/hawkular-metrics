'use strict';

angular.module('rhqm.services')
    .factory('metricDataService', ['$q', '$rootScope', '$http', '$log', '$localStorage', 'BASE_URL', function ($q, $rootScope, $http, $log, $localStorage, BASE_URL) {

        function makeBaseUrl() {
            var baseUrl = 'http://' + $rootScope.$storage.server + ':' + $rootScope.$storage.port + BASE_URL;
            $log.debug('MakeUrl: ' + baseUrl);
            return baseUrl;
        }

        return {
            getMetricsForTimeRange: function (id, startDate, endDate, buckets) {
                $log.info("-- Retrieving metrics data for id: " + id);
                $log.info("-- Date Range: " + new Date(startDate) + " - " + new Date(endDate));
                var numBuckets = buckets || 60,
                    base = makeBaseUrl(),
                    deferred = $q.defer(),
                    searchParams =
                    {
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
                var url = makeBaseUrl();
                $http.post(url + '/' + id, jsonPayload
                ).success(function () {
                        toastr.success('Inserted value for ID: ' + id, 'Success');
                    }).error(function (response, status) {
                        console.error("Error: " + status + " --> " + response);
                        toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + status);
                    });
            },

            insertMultiplePayload: function (jsonPayload) {
                var url = makeBaseUrl();
                $http.post(url + '/', jsonPayload
                ).success(function () {
                        toastr.success('Inserted Multiple values Successfully.', 'Success');
                    }).error(function (response, status) {
                        console.error("Error: " + status + " --> " + response);
                        toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + status);
                    });
            }
        };
    }]);
