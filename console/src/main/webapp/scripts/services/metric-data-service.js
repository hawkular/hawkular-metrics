'use strict';

angular.module('rhqm.services')
    .factory('metricDataService', ['$http', '$log', '$localStorage', 'BASE_URL', function ($http, $log, $localStorage, BASE_URL) {

        function makeBaseUrl () {
            $log.debug('hMakeurl: '+ 'http://' + this.$storage.server + ':' + this.$storage.port + '/' + BASE_URL);
            return encodeUri('http://' + $localStorage.server + ':' + $localStorage.port + '/' + BASE_URL);
        }

        return {
            getMetricsForTimeRange: function (id, startDate, endDate, buckets) {
                $log.info("Retrieving metrics data for id: " + id);
                $log.info("Date Range: " + new Date(startDate) + " - " + new Date(endDate));
                var numBuckets = buckets || 60,
                    base = makeBaseUrl();

                if (startDate >= endDate) {
                    $log.warn("Start date was after end date");
                    return;
                }

                return $http.get(base + '/' + id,
                    {
                        params: {
                            start: startDate.getTime(),
                            end: endDate.getTime(),
                            buckets: numBuckets
                        }
                    }
                );
            },

            insertSinglePayload: function (id, jsonPayload) {
                $http.post(makeBaseUrl() + '/' + id, jsonPayload
                ).success(function () {
                        toastr.success('Inserted value for ID: ' + id, 'Success');
                    }).error(function (response, status) {
                        console.error("Error: " + status + " --> " + response);
                        toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + status);
                    });
            },

            insertMultiplePayload: function (jsonPayload) {
                $http.post(makeBaseUrl + '/', jsonPayload
                ).success(function () {
                        toastr.success('Inserted Multiple values Successfully.', 'Success');
                    }).error(function (response, status) {
                        console.error("Error: " + status + " --> " + response);
                        toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + status);
                    });
            }
        };
    }]);
