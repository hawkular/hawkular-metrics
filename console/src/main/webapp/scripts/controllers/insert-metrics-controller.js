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
            vm = this;

        $rootScope.showOpenGroup = true;

        vm.streamingTimeRanges = [
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


        vm.timeInterval = [1, 5, 10, 15, 30, 60];

        vm.quickInsertData = {
            timeStamp: _.now(),
            id: "",
            jsonPayload: "",
            value: ""
        };

        vm.multiInsertData = {
            id: "",
            jsonPayload: ""
        };
        vm.rangeDurations = [1, 2, 5, 7];

        vm.rangeInsertData = {
            timeStamp: _.now(),
            id: "",
            selectedTimeInterval: 5,
            jsonPayload: "",
            startNumber: 1,
            endNumber: 100,
            selectedIntervalInMinutes: vm.timeInterval[2],
            selectedDuration: vm.rangeDurations[1]
        };


        vm.streamingInsertData = {
            timeStamp: _.now(),
            id: "",
            jsonPayload: "",
            count: 1,
            startNumber: 1,
            endNumber: 100,
            isStreamingStarted: false,
            lastStreamedValue: 2,
            selectedRefreshInterval: vm.streamingTimeRanges[1].range
        };


        vm.quickInsert = function (numberOfHoursPast) {
            var computedTimestamp;

            if (angular.isUndefined(numberOfHoursPast)) {
                computedTimestamp = moment();
            } else {
                computedTimestamp = moment().subtract('hours', numberOfHoursPast);
            }
            $log.debug("Generated Timestamp is: " + computedTimestamp.fromNow());

            vm.quickInsertData.jsonPayload = { timestamp: computedTimestamp.valueOf(), value: vm.quickInsertData.value };
            $log.info("quick insert for id:  %s ", vm.quickInsertData.id);

            metricDataService.insertSinglePayload(vm.quickInsertData.id, vm.quickInsertData.jsonPayload);

            vm.quickInsertData.value = "";

        };


        vm.multiInsert = function () {
            metricDataService.insertMultiplePayload(vm.multiInsertData.jsonPayload);
            vm.multiInsertData.jsonPayload = "";
        };


        vm.rangeInsert = function () {
            var jsonPayload;

            jsonPayload = calculateRangeTimestamps(vm.rangeInsertData.id, vm.rangeInsertData.selectedDuration,
                vm.rangeInsertData.selectedIntervalInMinutes, vm.rangeInsertData.startNumber,
                vm.rangeInsertData.endNumber);
            $log.debug("JsonPayload: " + jsonPayload);
            metricDataService.insertMultiplePayload(jsonPayload);
            vm.rangeInsertData.id = "";

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

        vm.startStreaming = function () {
            var selectedTimeRangeInSeconds = 5;

            angular.forEach(vm.streamingTimeRanges, function(value){
              if(value.range === vm.streamingInsertData.selectedRefreshInterval)  {
                 selectedTimeRangeInSeconds = value.rangeInSeconds;
              }
            });
            vm.streamingInsertData.isStreamingStarted = true;
            vm.streamingInsertData.count = 0;
            vm.streamingInsertData.lastStreamedValue = 0;
            streamingIntervalPromise = $interval(function () {
                $log.log("Timer has Run! for seconds: " + selectedTimeRangeInSeconds);
                vm.streamingInsertData.count = vm.streamingInsertData.count + 1;
                vm.streamingInsertData.lastStreamedValue = _.random(vm.streamingInsertData.startNumber, vm.streamingInsertData.endNumber);
                vm.streamingInsertData.jsonPayload = { timestamp: _.now(), value: vm.streamingInsertData.lastStreamedValue };


                metricDataService.insertSinglePayload(vm.streamingInsertData.id, vm.streamingInsertData.jsonPayload);

            }, selectedTimeRangeInSeconds * 1000);
            $scope.$on('$destroy', function () {
                $log.debug('Destroying intervalPromise');
                $interval.cancel(streamingIntervalPromise);
            });

        };

        vm.stopStreaming = function () {
            toastr.info('Stop Streaming Data.');
            $log.info('Stop Streaming Data.');
            vm.streamingInsertData.isStreamingStarted = false;
            $interval.cancel(streamingIntervalPromise);
        };


    }]);
