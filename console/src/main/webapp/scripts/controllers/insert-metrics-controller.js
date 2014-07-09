'use strict';


/**
 * @ngdoc controller
 * @name InsertMetricsController
 * @description A controller for inserting metrics into the rhq-metrics data store (either in-memory or Cassandra).
 *
 */
angular.module('chartingApp')
    .controller('InsertMetricsController', ['$scope', '$rootScope', '$log','$interval', 'metricDataService', function ($scope, $rootScope, $log, $interval, metricDataService) {
        var streamingIntervalPromise;

        $scope.streamingTimeRanges = [
            { "range": "1s", "rangeInSeconds": 1 },
            { "range": "5s", "rangeInSeconds": 5 },
            { "range": "30s", "rangeInSeconds": 30 },
            { "range": "1m", "rangeInSeconds": 60 },
            { "range": "5m", "rangeInSeconds": 5 * 60 },
            { "range": "10m", "rangeInSeconds": 10 * 60 },
            { "range": "15m", "rangeInSeconds": 15 * 60 },
            { "range": "30m", "rangeInSeconds": 30 * 60 },
            { "range": "1h", "rangeInSeconds": 60 * 60 }
        ];


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
            timeStamp: _.now(),
            id: "",
            selectedTimeInterval: 5,
            jsonPayload: "",
            startNumber: 1,
            endNumber: 100,
            selectedIntervalInMinutes: $scope.timeInterval[2],
            selectedDuration: $scope.rangeDurations[1]
        };


        $scope.streamingInsertData = {
            timeStamp: _.now(),
            id: "",
            jsonPayload: "",
            count: 1,
            startNumber: 1,
            endNumber: 100,
            isStreamingStarted: false,
            lastStreamedValue: 2,
            selectedRefreshInterval: $scope.streamingTimeRanges[1].range
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
            var jsonPayload;

            jsonPayload = calculateRangeTimestamps($scope.rangeInsertData.id, $scope.rangeInsertData.selectedDuration,
                $scope.rangeInsertData.selectedIntervalInMinutes, $scope.rangeInsertData.startNumber,
                $scope.rangeInsertData.endNumber);
            $log.debug("JsonPayload: " + jsonPayload);
            metricDataService.insertMultiplePayload(jsonPayload);
            $scope.rangeInsertData.id = "";

        };

        function calculateRangeTimestamps(id, numberOfDays, intervalInMinutes, randomStart, randomEnd) {
            var intervalTimestamps = [],
                startDate = moment().subtract('days', numberOfDays).valueOf(),
                endDate = _.now(),
                step = intervalInMinutes * 60 * 1000,
                dbData = [];

            intervalTimestamps = _.range(startDate, endDate, step);
            dbData = _.map(intervalTimestamps, function(ts){return {id: id, timestamp: ts, value: _.random(randomStart, randomEnd)};});

            return angular.toJson(dbData);

        }

        $scope.startStreaming = function () {
            var selectedTimeRangeInSeconds = 5;

            angular.forEach($scope.streamingTimeRanges, function(value){
              if(value.range === $scope.streamingInsertData.selectedRefreshInterval)  {
                 selectedTimeRangeInSeconds = value.rangeInSeconds;
              }
            });
            $scope.streamingInsertData.isStreamingStarted = true;
            $scope.streamingInsertData.count = 0;
            $scope.streamingInsertData.lastStreamedValue = 0;
            streamingIntervalPromise = $interval(function () {
                $log.log("Timer has Run! for seconds: " + selectedTimeRangeInSeconds);
                $scope.streamingInsertData.count = $scope.streamingInsertData.count + 1;
                $scope.streamingInsertData.lastStreamedValue = _.random($scope.streamingInsertData.startNumber, $scope.streamingInsertData.endNumber);
                $scope.streamingInsertData.jsonPayload = { timestamp: _.now(), value: $scope.streamingInsertData.lastStreamedValue };


                metricDataService.insertSinglePayload($scope.streamingInsertData.id, $scope.streamingInsertData.jsonPayload);

            }, selectedTimeRangeInSeconds * 1000);
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
