/// <reference path="../../vendor/vendor.d.ts" />
'use strict';
/**
* @ngdoc controller
* @name InsertMetricsController
* @description A controller for inserting metrics into the rhq-metrics data store (either in-memory or Cassandra).
*
*/
var InsertMetricsController = (function () {
    function InsertMetricsController($scope, $rootScope, $log, $interval, metricDataService) {
        this.$scope = $scope;
        this.$rootScope = $rootScope;
        this.$log = $log;
        this.$interval = $interval;
        this.metricDataService = metricDataService;
        this.showOpenGroup = true;
        this.streamingTimeRanges = [
            { 'range': '1s', 'rangeInSeconds': 1 },
            { 'range': '5s', 'rangeInSeconds': 5 },
            { 'range': '30s', 'rangeInSeconds': 30 },
            { 'range': '1m', 'rangeInSeconds': 60 },
            { 'range': '5m', 'rangeInSeconds': 5 * 60 },
            { 'range': '10m', 'rangeInSeconds': 10 * 60 },
            { 'range': '15m', 'rangeInSeconds': 15 * 60 },
            { 'range': '30m', 'rangeInSeconds': 30 * 60 },
            { 'range': '1h', 'rangeInSeconds': 60 * 60 }
        ];
        this.timeInterval = [1, 5, 10, 15, 30, 60];
        this.quickInsertData = {
            timeStamp: _.now(),
            id: '',
            jsonPayload: '',
            value: ''
        };
        this.multiInsertData = {
            id: '',
            jsonPayload: ''
        };
        this.rangeDurations = [1, 2, 5, 7];
        this.rangeInsertData = {
            timeStamp: _.now(),
            id: '',
            selectedTimeInterval: 5,
            jsonPayload: '',
            startNumber: 1,
            endNumber: 100,
            selectedIntervalInMinutes: this.timeInterval[2],
            selectedDuration: this.rangeDurations[1]
        };
        this.streamingInsertData = {
            timeStamp: _.now(),
            id: '',
            jsonPayload: '',
            count: 1,
            startNumber: 1,
            endNumber: 100,
            isStreamingStarted: false,
            lastStreamedValue: 2,
            selectedRefreshInterval: this.streamingTimeRanges[1].range
        };
        $scope.vm = this;
    }
    InsertMetricsController.prototype.quickInsert = function (numberOfHoursPast) {
        var computedTimestamp;

        if (angular.isUndefined(numberOfHoursPast)) {
            computedTimestamp = moment();
        } else {
            computedTimestamp = moment().subtract('hours', numberOfHoursPast);
        }
        this.$log.debug('Generated Timestamp is: ' + computedTimestamp.fromNow());

        this.quickInsertData.jsonPayload = { timestamp: computedTimestamp.valueOf(), value: this.quickInsertData.value };

        this.metricDataService.insertSinglePayload(this.quickInsertData.id, this.quickInsertData.jsonPayload).then(function (success) {
            toastr.success('Inserted value: ' + this.quickInsertData.value + ' for ID: ' + this.quickInsertData.id, 'Success');
            this.quickInsertData.value = '';
        }, function (error) {
            toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + error);
        });
    };

    InsertMetricsController.prototype.multiInsert = function () {
        this.metricDataService.insertMultiplePayload(this.multiInsertData.jsonPayload).then(function (success) {
            toastr.success('Inserted Multiple values Successfully.', 'Success');
            this.multiInsertData.jsonPayload = "";
        }, function (error) {
            this.insertError(error);
        });
    };

    InsertMetricsController.prototype.rangeInsert = function () {
        var jsonPayload;

        jsonPayload = this.calculateRangeTimestamps(this.rangeInsertData.id, this.rangeInsertData.selectedDuration, this.rangeInsertData.selectedIntervalInMinutes, this.rangeInsertData.startNumber, this.rangeInsertData.endNumber);
        this.$log.debug("JsonPayload: " + jsonPayload);
        this.metricDataService.insertMultiplePayload(jsonPayload).then(function (success) {
            toastr.success('Advanced Range Inserted Multiple values Successfully.', 'Success');
            this.rangeInsertData.id = "";
        }, function (error) {
            this.insertError(error);
        });
    };

    InsertMetricsController.prototype.insertError = function (error) {
        toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + error);
    };

    InsertMetricsController.prototype.calculateRangeTimestamps = function (id, numberOfDays, intervalInMinutes, randomStart, randomEnd) {
        var intervalTimestamps = [], startDate = moment().subtract('days', numberOfDays).valueOf(), endDate = _.now(), step = intervalInMinutes * 60 * 1000, startSeed = _.random(randomStart, randomEnd), dbData = [];

        intervalTimestamps = _.range(startDate, endDate, step);
        dbData = _.map(intervalTimestamps, function (ts) {
            return { id: id, timestamp: ts, value: startSeed + _.random(-5, 5) };
        });

        return angular.toJson(dbData);
    };

    InsertMetricsController.prototype.startStreaming = function () {
        var selectedTimeRangeInSeconds = 5;

        angular.forEach(this.streamingTimeRanges, function (value) {
            if (value.range === this.streamingInsertData.selectedRefreshInterval) {
                selectedTimeRangeInSeconds = value.rangeInSeconds;
            }
        });
        this.streamingInsertData.isStreamingStarted = true;
        this.streamingInsertData.count = 0;
        this.streamingInsertData.lastStreamedValue = 0;
        this.streamingIntervalPromise = this.$interval(function () {
            this.$log.log("Timer has Run! for seconds: " + selectedTimeRangeInSeconds);
            this.streamingInsertData.count = this.streamingInsertData.count + 1;
            this.streamingInsertData.lastStreamedValue = _.random(this.streamingInsertData.startNumber, this.streamingInsertData.endNumber);
            this.streamingInsertData.jsonPayload = { timestamp: _.now(), value: this.streamingInsertData.lastStreamedValue };

            this.metricDataService.insertSinglePayload(this.streamingInsertData.id, this.streamingInsertData.jsonPayload).then(function (success) {
                toastr.success('Successfully inserted: ' + this.streamingInsertData.lastStreamedValue, 'Streaming Insert');
            }, function (error) {
                this.insertError(error);
            });
        }, selectedTimeRangeInSeconds * 1000);
        this.$scope.$on('$destroy', function () {
            this.$log.debug('Destroying intervalPromise');
            this.$interval.cancel(this.streamingIntervalPromise);
        });
    };

    InsertMetricsController.prototype.stopStreaming = function () {
        toastr.info('Stop Streaming Data.');
        this.$log.info('Stop Streaming Data.');
        this.streamingInsertData.isStreamingStarted = false;
        this.$interval.cancel(this.streamingIntervalPromise);
    };
    return InsertMetricsController;
})();

angular.module('chartingApp').controller('InsertMetricsController', ['$scope', '$rootScope', '$log', '$interval', 'metricDataService', InsertMetricsController]);
//# sourceMappingURL=insert-metrics-controller.js.map
