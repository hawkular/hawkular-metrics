'use strict';


/**
 * @ngdoc controller
 * @name InsertMetricsController
 */
angular.module('chartingApp')
    .controller('InsertMetricsController', ['$scope', '$rootScope', '$log',  'metricDataService', function ($scope, $rootScope, $log,  metricDataService) {

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
            selectedTimeInterval:  5,
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
            $log.debug("Generated Timestamp is: " + computedTimestamp.fromNow());

            $scope.quickInsertData.jsonPayload = { timestamp: computedTimestamp.valueOf(), value: $scope.quickInsertData.value };
            $log.info("quick insert for id:  %s " , $scope.quickInsertData.id);

            metricDataService.insertSinglePayload($scope.quickInsertData.id,$scope.quickInsertData.jsonPayload);

            $scope.quickInsertData.value = "";

        };


        $scope.multiInsert = function () {
            metricDataService.insertMultiplePayload($scope.multiInsertData.jsonPayload);
            $scope.multiInsertData.jsonPayload = "";
        };


        $scope.rangeInsert = function () {
            var  jsonPayload,
                currentTimeMoment = moment();

            $log.debug("range insert for: " + $scope.rangeInsertData.id);
            console.dir($scope.rangeInsertData);

            jsonPayload = calculateTimestamps($scope.rangeInsertData.selectedDuration,
                $scope.rangeInsertData.selectedIntervalInMinutes, currentTimeMoment);
            $log.debug("JsonPayload: "+ jsonPayload);
            metricDataService.insertMultiplePayload(jsonPayload);
            $scope.rangeInsertData.id = "";

        };

        function calculateTimestamps(numberOfDays, intervalInMinutes, currentTimeMoment){
            var intervalTimestamps = [], randomValue;

            for(var i = 0; i < numberOfDays * 24 * 60 *   intervalInMinutes; i = i + intervalInMinutes ) {

                var calculatedTimeInMillis =  currentTimeMoment.subtract('minutes', i).valueOf();
                randomValue = metricDataService.createRandomValue($scope.rangeInsertData.startNumber, $scope.rangeInsertData.endNumber );
                intervalTimestamps.push({id: $scope.rangeInsertData.id, timestamp: calculatedTimeInMillis, value : randomValue});
            }
            return angular.toJson(intervalTimestamps);

        }


    }]);
