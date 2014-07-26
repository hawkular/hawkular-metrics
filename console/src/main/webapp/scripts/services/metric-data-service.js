'use strict';

angular.module('rhqm.services')
    .factory('metricDataService', ['$http', '$log', 'BASE_URL', function ($http, $log, BASE_URL) {

        return {
            getMetricsForTimeRange: function (id, startDate, endDate, buckets) {
                $log.info("Retrieving metrics data for id: " + id);
                $log.info("Date Range: " + new Date(startDate) + " - " + new Date(endDate));
                var numBuckets = buckets || 60;

                if(startDate >= endDate){
                    $log.warn("Start date was after end date");
                    return;
                }

                return $http.get(BASE_URL + '/' + id,
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
                $http.post(BASE_URL + '/' + id, jsonPayload
                ).success(function () {
                        toastr.success('Inserted value for ID: ' + id, 'Success');
                    }).error(function (response, status) {
                        console.error("Error: " + status + " --> " + response);
                        toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + status);
                    });
            },

            insertMultiplePayload: function (jsonPayload) {
                $http.post(BASE_URL + '/', jsonPayload
                ).success(function () {
                        toastr.success('Inserted Multiple values Successfully.', 'Success');
                    }).error(function (response, status) {
                        console.error("Error: " + status + " --> " + response);
                        toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + status);
                    });
            }
        };
    }]);
