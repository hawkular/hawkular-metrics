/// <reference path="../../vendor/vendor.d.ts" />
'use strict';



interface IInsertMetricsController {
    showOpenGroup: boolean;
    timeInterval :number[];

    quickInsert (numberOfHoursPast:number): void;
    multiInsert():void;
    rangeInsert():void;
    startStreaming ():void;
    stopStreaming ():void;
}

/**
 * @ngdoc controller
 * @name InsertMetricsController
 * @description A controller for inserting metrics into the rhq-metrics data store (either in-memory or Cassandra).
 *
 */
class InsertMetricsController implements IInsertMetricsController{

    constructor(private $scope:ng.IScope, private $rootScope:ng.IRootScopeService, private $log:ng.ILogService, private $interval:ng.IIntervalService, private metricDataService:any) {
        $scope.vm = this;
    }
        private streamingIntervalPromise: ng.IPromise<number>;

        this.$rootScope.showOpenGroup = true;

        streamingTimeRanges = [
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


        timeInterval :number[] = [1, 5, 10, 15, 30, 60];

        quickInsertData = {
            timeStamp: _.now(),
            id: '',
            jsonPayload: '',
            value: ''
        };

        multiInsertData = {
            id: '',
            jsonPayload: ''
        };
        rangeDurations: number[] = [1, 2, 5, 7];

        rangeInsertData = {
            timeStamp: _.now(),
            id: '',
            selectedTimeInterval: 5,
            jsonPayload: '',
            startNumber: 1,
            endNumber: 100,
            selectedIntervalInMinutes: this.timeInterval[2],
            selectedDuration: this.rangeDurations[1]
        };


        streamingInsertData = {
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


        quickInsert (numberOfHoursPast:number): void {
            computedTimestamp:Moment;

            if (angular.isUndefined(numberOfHoursPast)) {
                computedTimestamp = moment();
            } else {
                computedTimestamp = moment().subtract('hours', numberOfHoursPast);
            }
            this.$log.debug('Generated Timestamp is: ' + computedTimestamp.fromNow());

            quickInsertData.jsonPayload = { timestamp: computedTimestamp.valueOf(), value: this.quickInsertData.value };

            this.metricDataService.insertSinglePayload(this.quickInsertData.id, this.quickInsertData.jsonPayload).then(function (success) {
                toastr.success('Inserted value: ' + quickInsertData.value + ' for ID: ' + quickInsertData.id, 'Success');
                quickInsertData.value = '';
            }, function (error) {
                toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + error);
            });


        }


        multiInsert():void {
            this.metricDataService.insertMultiplePayload(this.multiInsertData.jsonPayload).then(function (success) {
                toastr.success('Inserted Multiple values Successfully.', 'Success');
                this.multiInsertData.jsonPayload = "";
            }, function (error) {
                this.insertError(error);
            });
        }


        rangeInsert():void {
             jsonPayload = this.calculateRangeTimestamps(this.rangeInsertData.id, this.rangeInsertData.selectedDuration,
                this.rangeInsertData.selectedIntervalInMinutes, this.rangeInsertData.startNumber,
                this.rangeInsertData.endNumber);
            this.$log.debug("JsonPayload: " + jsonPayload);
            this.$log.warn("About to rangeInsert: "+ this.rangeInsertData.id);
            this.metricDataService.insertMultiplePayload(jsonPayload).then(function (success) {
                toastr.success('Advanced Range Inserted Multiple values Successfully.', 'Success');
                this.rangeInsertData.id = "";
            }, function (error) {
                this.insertError(error);
            });

        }

        private  insertError(error:string):void {
            toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + error);
        }

        private  calculateRangeTimestamps(id:string, numberOfDays:number, intervalInMinutes:number, randomStart:number, randomEnd:number):any {
            intervalTimestamps = [],
                startDate = moment().subtract('days', numberOfDays).valueOf(),
                endDate = _.now(),
                step = intervalInMinutes * 60 * 1000,
                startSeed = _.random(randomStart, randomEnd),
                dbData = [];

            this.$log.warn("*** Mike *** id: "+ id+ ", days: "+ numberOfDays);
            intervalTimestamps = _.range(startDate, endDate, step);
            dbData = _.map(intervalTimestamps, function (ts) {
                return {id: id, timestamp: ts, value: startSeed + _.random(-5, 5)};
            });

            return angular.toJson(dbData);

        }

        startStreaming ():void {
            selectedTimeRangeInSeconds = 5;

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

        }

        stopStreaming ():void {
            toastr.info('Stop Streaming Data.');
            this.$log.info('Stop Streaming Data.');
            this.streamingInsertData.isStreamingStarted = false;
           this.$interval.cancel(this.streamingIntervalPromise);
        }

}

angular.module('chartingApp')
    .controller('InsertMetricsController', ['$scope', '$rootScope', '$log', '$interval', 'metricDataService', InsertMetricsController ]);
