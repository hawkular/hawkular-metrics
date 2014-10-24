/// <reference path="../../vendor/vendor.d.ts" />

module Controllers {
    'use strict';

    export interface IContextChartDataPoint {
        timestamp: number;
        value: number;
        avg: number;
        empty: boolean;
    }

    export interface IChartDataPoint extends IContextChartDataPoint {
        date: Date;
        min: number;
        max: number;
    }


    export interface IDateTimeRangeDropDown {
        range: string;
        rangeInSeconds:number;
    }

    /**
     * @ngdoc controller
     * @name ChartController
     * @description This controller is responsible for handling activity related to the Chart tab.
     * @param $scope
     * @param $rootScope
     * @param $interval
     * @param $log
     * @param metricDataService
     */
    export class DashboardController {
        public static  $inject = ['$scope', '$rootScope', '$interval', '$log', 'metricDataService' ];

        private updateLastTimeStampToNowPromise:ng.IPromise<number>;
        private chartData = {};
        private bucketedDataPoints:IChartDataPoint[] = [];

        selectedMetrics:string[] = [];
        searchId = '';
        updateEndTimeStampToNow = false;
        showAutoRefreshCancel = false;
        chartType = 'bar';
        chartTypes = ['bar', 'line', 'area' ];

        dateTimeRanges:IDateTimeRangeDropDown[] = [
            { 'range': '1h', 'rangeInSeconds': 60 * 60 } ,
            { 'range': '4h', 'rangeInSeconds': 4 * 60 * 60 } ,
            { 'range': '8h', 'rangeInSeconds': 8 * 60 * 60 },
            { 'range': '12h', 'rangeInSeconds': 12 * 60 * 60 },
            { 'range': '1d', 'rangeInSeconds': 24 * 60 * 60 },
            { 'range': '5d', 'rangeInSeconds': 5 * 24 * 60 * 60 },
            { 'range': '1m', 'rangeInSeconds': 30 * 24 * 60 * 60 },
            { 'range': '3m', 'rangeInSeconds': 3 * 30 * 24 * 60 * 60 },
            { 'range': '6m', 'rangeInSeconds': 6 * 30 * 24 * 60 * 60 }
        ];


        constructor(private $scope:ng.IScope, private $rootScope:ng.IRootScopeService, private $interval:ng.IIntervalService, private $log:ng.ILogService, private metricDataService, public startTimeStamp:Date, public endTimeStamp:Date, public dateRange:string) {
            $scope.vm = this;

            $scope.$on('GraphTimeRangeChangedEvent', function (event, timeRange) {
                console.debug("GraphTimeRangeChangedEvent received!");
                $scope.vm.startTimeStamp = timeRange[0];
                $scope.vm.endTimeStamp = timeRange[1];
                $scope.vm.dateRange = moment(timeRange[0]).from(moment(timeRange[1]));
                $scope.vm.refreshAllChartsDataForTimestamp($scope.vm.startTimeStamp, $scope.vm.endTimeStamp);
            });

            $rootScope.$on('NewChartEvent', function (event, metricId) {
                console.debug('NewChartEvent for: ' + metricId);
                if (_.contains($scope.vm.selectedMetrics, metricId)) {
                    toastr.warning(metricId + ' is already selected');
                } else {
                    $scope.vm.selectedMetrics.push(metricId);
                    $scope.vm.searchId = metricId;
                    $scope.vm.refreshHistoricalChartData(metricId, $scope.vm.startTimeStamp, $scope.vm.endTimeStamp);
                    toastr.success(metricId + ' Added to Dashboard!');
                }
            });

            $rootScope.$on('RemoveChartEvent', function (event, metricId) {
                console.debug('RemoveChartEvent for: ' + metricId);
                if (_.contains($scope.vm.selectedMetrics, metricId)) {
                    var pos = _.indexOf($scope.vm.selectedMetrics, metricId);
                    $scope.vm.selectedMetrics.splice(pos, 1);
                    $scope.vm.searchId = metricId;
                    toastr.info('Removed: ' + metricId + ' from Dashboard!');
                    $scope.vm.refreshAllChartsDataForTimestamp($scope.vm.startTimeStamp, $scope.vm.endTimeStamp);
                }
            });

        }


        private noDataFoundForId(id:string):void {
            this.$log.warn('No Data found for id: ' + id);
            toastr.warning('No Data found for id: ' + id);
        }


        deleteChart(metricId:string):void {
            var pos = _.indexOf(this.selectedMetrics, metricId);
            this.selectedMetrics.splice(pos, 1);
            this.$rootScope.$broadcast('RemoveSelectedMetricEvent',metricId);
        }


        cancelAutoRefresh():void {
            this.showAutoRefreshCancel = !this.showAutoRefreshCancel;
            this.$interval.cancel(this.updateLastTimeStampToNowPromise);
            toastr.info('Canceling Auto Refresh');
        }

        autoRefresh(intervalInSeconds:number):void {
            var that = this;
            toastr.info('Auto Refresh Mode started');
            this.updateEndTimeStampToNow = !this.updateEndTimeStampToNow;
            this.showAutoRefreshCancel = true;
            if (this.updateEndTimeStampToNow) {
                this.showAutoRefreshCancel = true;
                this.updateLastTimeStampToNowPromise = this.$interval(function () {
                    that.updateTimestampsToNow();
                    that.refreshAllChartsDataForTimestamp();
                }, intervalInSeconds * 1000);

            } else {
                this.$interval.cancel(this.updateLastTimeStampToNowPromise);
            }

            this.$scope.$on('$destroy', function () {
                this.$interval.cancel(this.updateLastTimeStampToNowPromise);
            });
        }

        private updateTimestampsToNow(): void {
            var interval:number = this.$scope.vm.endTimeStamp - this.$scope.vm.startTimeStamp;
            this.$scope.vm.endTimeStamp = new Date().getTime();
            this.$scope.vm.startTimeStamp = this.$scope.vm.endTimeStamp - interval;
        }

        refreshChartDataNow():void {
            this.updateTimestampsToNow();
            this.refreshAllChartsDataForTimestamp(this.$scope.vm.startTimeStamp, this.$scope.vm.endTimeStamp);
        }

        refreshHistoricalChartData(metricId:string, startDate:Date, endDate:Date):void {
            this.refreshHistoricalChartDataForTimestamp(metricId, startDate.getTime(), endDate.getTime());
        }

        refreshHistoricalChartDataForTimestamp(metricId:string, startTime?:number, endTime?:number):void {
            var that = this;
            // calling refreshChartData without params use the model values
            if (angular.isUndefined(endTime)) {
                endTime = this.$scope.vm.endTimeStamp;
            }
            if (angular.isUndefined(startTime)) {
                startTime = this.$scope.vm.startTimeStamp;
            }

            if (startTime >= endTime) {
                this.$log.warn('Start Date was >= End Date');
                toastr.warning('Start Date was after End Date');
                return;
            }

            if (metricId !== '') {

                this.metricDataService.getMetricsForTimeRange(metricId, new Date(startTime), new Date(endTime))
                    .then(function (response) {
                        // we want to isolate the response from the data we are feeding to the chart
                        that.bucketedDataPoints = that.formatBucketedChartOutput(response);

                        if (that.bucketedDataPoints.length !== 0) {
                            that.$log.info("Retrieving data for metricId: " + metricId);
                            // this is basically the DTO for the chart
                            that.chartData[metricId] = {
                                id: metricId,
                                startTimeStamp: that.startTimeStamp,
                                endTimeStamp: that.endTimeStamp,
                                dataPoints: that.bucketedDataPoints
                            };

                        } else {
                            that.noDataFoundForId(metricId);
                        }

                    }, function (error) {
                        toastr.error('Error Loading Chart Data: ' + error);
                    });
            }

        }

        refreshAllChartsDataForTimestamp(startTime?:number, endTime?:number):void {
            var that = this;

            _.each(this.selectedMetrics, function (aMetric) {
                that.$log.info("Reloading Metric Chart Data for: " + aMetric);
                that.refreshHistoricalChartDataForTimestamp(aMetric, startTime, endTime);
            });

        }

        getChartDataFor(metricId:string):IChartDataPoint[] {
            return this.chartData[metricId].dataPoints;
        }

        private formatBucketedChartOutput(response):IChartDataPoint[] {
            //  The schema is different for bucketed output
            return _.map(response, function (point:IChartDataPoint) {
                return {
                    timestamp: point.timestamp,
                    date: new Date(point.timestamp),
                    value: !angular.isNumber(point.value) ? 0 : point.value,
                    avg: (point.empty) ? 0 : point.avg,
                    min: !angular.isNumber(point.min) ? 0 : point.min,
                    max: !angular.isNumber(point.max) ? 0 : point.max,
                    empty: point.empty
                };
            });
        }

        private calculatePreviousTimeRange(startDate:Date, endDate:Date):any {
            var previousTimeRange:Date[] = [];
            var intervalInMillis = endDate.getTime() - startDate.getTime();

            previousTimeRange.push(new Date(startDate.getTime() - intervalInMillis));
            previousTimeRange.push(startDate);
            return previousTimeRange;
        }

        showPreviousTimeRange():void {
            var previousTimeRange = this.calculatePreviousTimeRange(this.startTimeStamp, this.endTimeStamp);

            this.startTimeStamp = previousTimeRange[0];
            this.endTimeStamp = previousTimeRange[1];
            this.refreshAllChartsDataForTimestamp(this.startTimeStamp.getTime(), this.endTimeStamp.getTime());

        }


        private calculateNextTimeRange(startDate:Date, endDate:Date):any {
            var nextTimeRange = [];
            var intervalInMillis = endDate.getTime() - startDate.getTime();

            nextTimeRange.push(endDate);
            nextTimeRange.push(new Date(endDate.getTime() + intervalInMillis));
            return nextTimeRange;
        }


        showNextTimeRange():void {
            var nextTimeRange = this.calculateNextTimeRange(this.startTimeStamp, this.endTimeStamp);

            this.startTimeStamp = nextTimeRange[0];
            this.endTimeStamp = nextTimeRange[1];
            this.refreshAllChartsDataForTimestamp(this.startTimeStamp.getTime(), this.endTimeStamp.getTime());

        }


        hasNext():boolean {
            var nextTimeRange = this.calculateNextTimeRange(this.startTimeStamp, this.endTimeStamp);
            // unsophisticated test to see if there is a next; without actually querying.
            //@fixme: pay the price, do the query!
            return nextTimeRange[1].getTime() < _.now();
        }


    }

    angular.module('chartingApp')
        .controller('DashboardController', DashboardController);
}
