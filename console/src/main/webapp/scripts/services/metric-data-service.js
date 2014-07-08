'use strict';

angular.module('rhqm.services')
    .factory('metricDataService', ['$http', '$log', 'BASE_URL', function ($http, $log, BASE_URL) {

        return {
            getMetricsForTimeRange: function (id, startDate, endDate) {
                $log.info("Retrieving metrics data for id: " + id);
                $log.info("Date Range: " + startDate + " - " + endDate);

                return $http.get(BASE_URL + '/' + id,
                    {
                        params: {
                            start: startDate.getTime(),
                            end: endDate.getTime(),
                            buckets: 60
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
            },

            createRandomValue: function (min, max) {
                return Math.floor(Math.random() * (max - min + 1) + min);
            }

        };
    }]);
