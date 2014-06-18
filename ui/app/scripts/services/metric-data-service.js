

'use strict';

angular.module('chartingApp')
    .factory('metricDataService', ['$http', 'BASE_URL', function ($http, BASE_URL) {

        return {
            insertSinglePayload: function (id, jsonPayload) {
                $http.post(BASE_URL + '/' + id, jsonPayload
                ).success(function () {
                        toastr.success('Inserted value for ID: '+id, 'Success')
                    }).error(function (response, status) {
                        console.error("Error: " + status + " --> " + response);
                        toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + status);
                    });
            },

            insertMultiplePayload: function (jsonPayload) {
                $http.post(BASE_URL + '/', jsonPayload
                ).success(function () {
                        toastr.success('Inserted Multiple values Successfully.','Success')
                    }).error(function (response, status) {
                        console.error("Error: " + status + " --> " + response);
                        toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + status);
                    });
            },

            createRandomValue : function (min,max) {
                return Math.floor(Math.random()*(max-min+1)+min);
            }

        };
    }]);
