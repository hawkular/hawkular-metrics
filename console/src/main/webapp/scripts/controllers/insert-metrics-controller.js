'use strict';


/**
 * @ngdoc controller
 * @name InsertMetricsController
 * @description A controller for inserting metrics into the rhq-metrics data store (either in-memory or Cassandra).
 *
 */
angular.module('chartingApp')
    .controller('InsertMetricsController', ['$scope', '$rootScope', '$log','$interval', 'metricDataService', function ($scope, $rootScope, $log, $interval, metricDataService) {
        var streamingIntervalPromise,
            randomIntFromInterval = function (min, max) {
                return Math.floor(Math.random() * (max - min + 1) + min);
            };

        $scope.timeInterval = [1, 5, 10, 15, 30, 60];
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
            selectedTimeInterval: 5,
            jsonPayload: "",
            startNumber: 1,
            endNumber: 100,
            selectedIntervalInMinutes: $scope.timeInterval[2],
            selectedDuration: $scope.rangeDurations[1]
        };


        $scope.streamingInsertData = {
            timeStamp: moment().valueOf(),
            id: "",
            jsonPayload: "",
            count: 1,
            startNumber: 1,
            endNumber: 100,
            //refreshTimerValue: 30,
            isStreamingStarted: false,
            lastStreamedValue: 2,
            selectedRefreshInterval: $scope.timeInterval[0]
        };


        $scope.quickInsert = function (numberOfHoursPast) {
            var computedTimestamp;

            if (angular.isUndefined(numberOfHoursPast)) {
                computedTimestamp = moment();
            } else {
                computedTimestamp = moment().subtract('hours', numberOfHoursPast);
            }
            $log.debug("Generated Timestamp is: " + computedTimestamp.fromNow());

            $scope.quickInsertData.jsonPayload = { timestamp: computedTimestamp.valueOf(), value: $scope.quickInsertData.value };
            $log.info("quick insert for id:  %s ", $scope.quickInsertData.id);

            metricDataService.insertSinglePayload($scope.quickInsertData.id, $scope.quickInsertData.jsonPayload);

            $scope.quickInsertData.value = "";

        };


        $scope.multiInsert = function () {
            metricDataService.insertMultiplePayload($scope.multiInsertData.jsonPayload);
            $scope.multiInsertData.jsonPayload = "";
        };


        $scope.rangeInsert = function () {
            var jsonPayload,
                currentTimeMoment = moment();

            $log.debug("range insert for: " + $scope.rangeInsertData.id);

            jsonPayload = calculateRangeTimestamps($scope.rangeInsertData.selectedDuration,
                $scope.rangeInsertData.selectedIntervalInMinutes, currentTimeMoment);
            $log.debug("JsonPayload: " + jsonPayload);
            metricDataService.insertMultiplePayload(jsonPayload);
            $scope.rangeInsertData.id = "";

        };

        function calculateRangeTimestamps(numberOfDays, intervalInMinutes, currentTimeMoment) {
            var intervalTimestamps = [], randomValue;

            for (var i = 0; i < numberOfDays * 24 * 60 * intervalInMinutes; i = i + intervalInMinutes) {

                var calculatedTimeInMillis = currentTimeMoment.subtract('minutes', i).valueOf();
                randomValue = metricDataService.createRandomValue($scope.rangeInsertData.startNumber, $scope.rangeInsertData.endNumber);
                intervalTimestamps.push({id: $scope.rangeInsertData.id, timestamp: calculatedTimeInMillis, value: randomValue});
            }
            return angular.toJson(intervalTimestamps);

        }

        $scope.startStreaming = function () {
            $log.info("Start Streaming Inserts");
            $scope.streamingInsertData.isStreamingStarted = true;
            $scope.streamingInsertData.count = 0;
            $scope.streamingInsertData.lastStreamedValue = 0;
            streamingIntervalPromise = $interval(function () {
                $log.log("Timer has Run! for seconds: " + $scope.streamingInsertData.selectedRefreshInterval);
                $scope.streamingInsertData.count = $scope.streamingInsertData.count + 1;
                $scope.streamingInsertData.lastStreamedValue = randomIntFromInterval($scope.streamingInsertData.startNumber, $scope.streamingInsertData.endNumber);
                $scope.streamingInsertData.jsonPayload = { timestamp: moment().valueOf(), value: $scope.streamingInsertData.lastStreamedValue };


                metricDataService.insertSinglePayload($scope.streamingInsertData.id, $scope.streamingInsertData.jsonPayload);

            }, $scope.streamingInsertData.selectedRefreshInterval * 1000);
            $scope.$on('$destroy', function () {
                $log.debug('Destroying intervalPromise');
                $interval.cancel(streamingIntervalPromise);
            });

        };

        $scope.stopStreaming = function () {
            toastr.info('Stop Streaming Data.');
            $log.info('Stop Streaming Data.');
            $scope.streamingInsertData.isStreamingStarted = false;
            $interval.cancel(streamingIntervalPromise);
        };


    }]);
