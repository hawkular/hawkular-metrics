///
/// Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
/// and other contributors as indicated by the @author tags.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///    http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

/// <reference path="../../vendor/vendor.d.ts" />
module Controllers {
    'use strict';

    export interface IInsertMetricsController {
        showOpenGroup: boolean;
        timeInterval :number[];
        quickInsertData:IQuickInsertData;

        quickInsert (numberOfHoursPast:number): void;
        multiInsert():void;
        rangeInsert():void;
        startStreaming ():void;
        stopStreaming ():void;
    }

    export interface IQuickInsertData {
        timeStamp:number;
        id:string;
        jsonPayload: {};
        value:any;
    }

    export interface ITimeRangeSelection {
        range:string;
        rangeInSeconds:number;
    }

    export interface IMetricDataPoint {
        timeStamp:number;
        value:number;
    }

    export interface IInsertRequestBody {
        tenantId:string;
        name:string;
        data:IMetricDataPoint[];
    }


    /**
     * @ngdoc controller
     * @name InsertMetricsController
     * @description A controller for inserting metrics into the rhq-metrics data store (either in-memory or Cassandra).
     *
     */
    export class InsertMetricsController {

        public static  $inject = ['$scope', '$rootScope', '$log', '$interval', 'metricDataService', 'TENANT_ID'];

        showOpenGroup = true;

        constructor(private $scope:ng.IScope,
                    private $rootScope:ng.IRootScopeService,
                    private $log:ng.ILogService,
                    private $interval:ng.IIntervalService,
                    private metricDataService:any,
                    private TENANT_ID:string) {
            $scope.vm = this;

        }

        private streamingIntervalPromise:ng.IPromise<number>;

        streamingTimeRanges:ITimeRangeSelection[] = [
            {'range': '1s', 'rangeInSeconds': 1},
            {'range': '5s', 'rangeInSeconds': 5},
            {'range': '30s', 'rangeInSeconds': 30},
            {'range': '1m', 'rangeInSeconds': 60},
            {'range': '5m', 'rangeInSeconds': 5 * 60},
            {'range': '10m', 'rangeInSeconds': 10 * 60},
            {'range': '15m', 'rangeInSeconds': 15 * 60},
            {'range': '30m', 'rangeInSeconds': 30 * 60},
            {'range': '1h', 'rangeInSeconds': 60 * 60}
        ];


        timeInterval:number[] = [1, 5, 10, 15, 30, 60];

        quickInsertData:IQuickInsertData = {
            timeStamp: _.now(),
            id: '',
            jsonPayload: {},
            value: ''
        };

        multiInsertData = {
            id: '',
            jsonPayload: {}
        };
        rangeDurations:number[] = [1, 2, 5, 7];

        rangeInsertData = {
            timeStamp: _.now(),
            id: '',
            selectedTimeInterval: 5,
            jsonPayload: {},
            startNumber: 1,
            endNumber: 100,
            selectedIntervalInMinutes: this.timeInterval[2],
            selectedDuration: this.rangeDurations[1]
        };


        streamingInsertData = {
            timeStamp: _.now(),
            id: '',
            jsonPayload: {},
            count: 1,
            startNumber: 1,
            endNumber: 100,
            isStreamingStarted: false,
            lastStreamedValue: 2,
            selectedRefreshInterval: this.streamingTimeRanges[1].range
        };


        quickInsert(numberOfHoursPast:number):void {
            var computedTimestamp:number;

            if (angular.isUndefined(numberOfHoursPast)) {
                computedTimestamp = moment().unix();
            } else {
                computedTimestamp = moment().subtract('hours', numberOfHoursPast).unix();
            }
            this.$log.debug('Generated Timestamp is: ' + computedTimestamp);

            this.quickInsertData.jsonPayload = [{ name: this.quickInsertData.id, tenantId: this.TENANT_ID, data: [{timestamp: computedTimestamp, value: this.quickInsertData.value}]}];

            this.metricDataService.insertMultiplePayload(this.quickInsertData.jsonPayload).then((success) => {
                toastr.success('Inserted value: ' + this.quickInsertData.value + ' for ID: ' + this.quickInsertData.id, 'Success');
                this.quickInsertData.value = '';
            }, (error) => {
                toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + error);
            });
        }


        multiInsert():void {
            this.metricDataService.insertMultiplePayload(this.multiInsertData.jsonPayload).then((success)=> {
                toastr.success('Inserted Multiple values Successfully.', 'Success');
                this.multiInsertData.jsonPayload = "";
            }, (error) => {
                InsertMetricsController.insertError(error);
            });
        }


        rangeInsert():void {
            var jsonPayload :IInsertRequestBody[] = [];
               jsonPayload.push(
                this.calculateRangeDataForMetric(this.rangeInsertData.id, this.TENANT_ID, this.rangeInsertData.selectedDuration,
                    this.rangeInsertData.selectedIntervalInMinutes, this.rangeInsertData.startNumber,
                    this.rangeInsertData.endNumber));

            console.dir(jsonPayload);
            this.metricDataService.insertMultiplePayload(jsonPayload).then((success) => {
                toastr.success('Advanced Range Inserted Multiple values Successfully for id: ' + this.rangeInsertData.id, 'Success');
                this.rangeInsertData.id = "";
            }, (error) => {
                InsertMetricsController.insertError(error);
            });
        }

        private  calculateRangeDataForMetric(metricId:string, tenantId:string, numberOfDays:number, intervalInMinutes:number, randomStart:number, randomEnd:number):IInsertRequestBody {
            var intervalTimestamps = [],
                startDate = moment().subtract('days', numberOfDays).valueOf(),
                endDate = _.now(),
                step = intervalInMinutes * 60 * 1000,
                startSeed = _.random(randomStart, randomEnd),
                dbData = [];

            intervalTimestamps = _.range(startDate, endDate, step);
            dbData = _.map(intervalTimestamps, (ts) => {
                return {timestamp: ts, value: startSeed + _.random(-5, 5)};
            });

            return {tenantId: tenantId, name: metricId, data: dbData};
        }

        startStreaming():void {
            var selectedTimeRangeInSeconds = 5;

            angular.forEach(this.streamingTimeRanges, (value)=> {
                if (value.range === this.streamingInsertData.selectedRefreshInterval) {
                    selectedTimeRangeInSeconds = value.rangeInSeconds;
                }
            });
            this.streamingInsertData.isStreamingStarted = true;
            this.streamingInsertData.count = 0;
            this.streamingInsertData.lastStreamedValue = 0;
            this.streamingIntervalPromise = this.$interval(()=> {
                this.$log.log("Timer has Run! for seconds: " + selectedTimeRangeInSeconds);
                this.streamingInsertData.count = this.streamingInsertData.count + 1;
                this.streamingInsertData.lastStreamedValue = _.random(this.streamingInsertData.startNumber, this.streamingInsertData.endNumber);

                this.streamingInsertData.jsonPayload =
                    [
                        {
                            tenantId: this.TENANT_ID,
                            name: this.streamingInsertData.id,
                            timestamp: _.now(),
                            value: this.streamingInsertData.lastStreamedValue,
                            data: [{ timestamp: _.now(), value: this.streamingInsertData.lastStreamedValue}]
                        }
                    ];

                this.metricDataService.insertMultiplePayload(this.streamingInsertData.jsonPayload).then((success) => {
                    toastr.success('Successfully inserted: ' + this.streamingInsertData.lastStreamedValue, 'Streaming Insert');
                }, (error) => {
                    InsertMetricsController.insertError(error);
                });

            }, selectedTimeRangeInSeconds * 1000);
            this.$scope.$on('$destroy', () => {
                this.$log.debug('Destroying intervalPromise');
                this.$interval.cancel(this.streamingIntervalPromise);
            });

        }

        stopStreaming():void {
            toastr.info('Stop Streaming Data.');
            this.$log.info('Stop Streaming Data.');
            this.streamingInsertData.isStreamingStarted = false;
            this.$interval.cancel(this.streamingIntervalPromise);
        }


        private static  insertError(error:string):void {
            toastr.error('An issue with inserting data has occurred. Please see the console logs. Status: ' + error);
        }


    }

    angular.module('chartingApp')
        .controller('InsertMetricsController', InsertMetricsController);
}
