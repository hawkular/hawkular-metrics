'use strict';


/**
 * @ngdoc controller
 * @name InsertMetricsController
 * @param {expression} insertMetricsController
 */
angular.module('chartingApp')
    .controller('InsertMetricsController', ['$scope',  'metricDataService', function ($scope,  metricDataService) {

        $scope.timeIntervalInMinutes = [1, 5, 10, 15, 30, 60];
        $scope.showOpenGroup = true;

        $scope.quickInsertData = {
            timeStamp: moment().valueOf(),
            id: "",
            jsonPayload: "",
            value: ""
        };

        $scope.multiInsertData = {
            id: "",
            jsonPayload: ""
        };


        $scope.rangeDurations = [1, 2, 5, 7];

        $scope.rangeInsertData = {
            timeStamp: moment().valueOf(),
            id: "",
            jsonPayload: "",
            startNumber: 1,
            endNumber: 100,
            selectedIntervalInMinutes: $scope.timeIntervalInMinutes[2],
            selectedDuration: $scope.rangeDurations[1]
        };



        $scope.quickInsert = function (numberOfHoursPast) {
            var  computedTimestamp;

            if (angular.isUndefined(numberOfHoursPast)) {
                computedTimestamp = moment();
            } else {
                computedTimestamp = moment().subtract('hours', numberOfHoursPast);
            }
            console.log("Generated Timestamp is: " + computedTimestamp.fromNow());

            $scope.quickInsertData.jsonPayload = { timestamp: computedTimestamp.valueOf(), value: $scope.quickInsertData.value };
            console.info("quick insert for id: " + $scope.quickInsertData.id);

            metricDataService.insertPayload($scope.quickInsertData.id,$scope.quickInsertData.jsonPayload);

        };


        $scope.multiInsert = function () {
            console.info("multi insert for: " + $scope.multiInsertData.id);
            metricDataService.insertPayload($scope.multiInsertData.id,$scope.multiInsertData.jsonPayload);

        };




    }]);
