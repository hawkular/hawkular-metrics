/// <reference path="../../vendor/vendor.d.ts" />
'use strict';
var Controllers;
(function (Controllers) {
    

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
    var ChartController = (function () {
        function ChartController($scope, $rootScope, $interval, $log, metricDataService, startTimeStamp, endTimeStamp, dateRange) {
            this.$scope = $scope;
            this.$rootScope = $rootScope;
            this.$interval = $interval;
            this.$log = $log;
            this.metricDataService = metricDataService;
            this.startTimeStamp = startTimeStamp;
            this.endTimeStamp = endTimeStamp;
            this.dateRange = dateRange;
            this.searchId = '';
            this.updateEndTimeStampToNow = false;
            this.collapseTable = true;
            this.tableButtonLabel = 'Show Table';
            this.showAvgLine = true;
            this.hideHighLowValues = false;
            this.showPreviousRangeDataOverlay = false;
            this.showContextZoom = true;
            this.showAutoRefreshCancel = false;
            this.chartType = 'bar';
            this.chartTypes = ['bar', 'line', 'area', 'scatter', 'scatterline', 'candlestick', 'histogram'];
            this.dateTimeRanges = [
                { 'range': '1h', 'rangeInSeconds': 60 * 60 },
                { 'range': '4h', 'rangeInSeconds': 4 * 60 * 60 },
                { 'range': '8h', 'rangeInSeconds': 8 * 60 * 60 },
                { 'range': '12h', 'rangeInSeconds': 12 * 60 * 60 },
                { 'range': '1d', 'rangeInSeconds': 24 * 60 * 60 },
                { 'range': '5d', 'rangeInSeconds': 5 * 24 * 60 * 60 },
                { 'range': '1m', 'rangeInSeconds': 30 * 24 * 60 * 60 },
                { 'range': '3m', 'rangeInSeconds': 3 * 30 * 24 * 60 * 60 },
                { 'range': '6m', 'rangeInSeconds': 6 * 30 * 24 * 60 * 60 }
            ];
            this.bucketedDataPoints = [];
            this.contextDataPoints = [];
            $scope.vm = this;

            this.startTimeStamp = moment().subtract('hours', 24).toDate(); //default time period set to 24 hours
            this.endTimeStamp = new Date();
            this.dateRange = moment().subtract('hours', 24).from(moment(), true);

            $scope.$on('GraphTimeRangeChangedEvent', function (event, timeRange) {
                $scope.vm.startTimeStamp = timeRange[0];
                $scope.vm.endTimeStamp = timeRange[1];
                $scope.vm.dateRange = moment(timeRange[0]).from(moment(timeRange[1]));
                $scope.vm.refreshHistoricalChartDataForTimestamp(startTimeStamp, endTimeStamp);
            });
        }
        //@todo: refactor out vars to I/F object
        //chartInputParams:IChartInputParams ;
        //       $rootScope.$on('DateRangeMove', function (event, message) {
        //            $log.debug('DateRangeMove on chart Detected.');
        //        });
        //
        ChartController.prototype.noDataFoundForId = function (id) {
            this.$log.warn('No Data found for id: ' + id);
            toastr.warning('No Data found for id: ' + id);
        };

        ChartController.prototype.calculatePreviousTimeRange = function (startDate, endDate) {
            var previousTimeRange = [];
            var intervalInMillis = endDate.getTime() - startDate.getTime();

            previousTimeRange.push(new Date(startDate.getTime() - intervalInMillis));
            previousTimeRange.push(startDate);
            return previousTimeRange;
        };

        ChartController.prototype.showPreviousTimeRange = function () {
            var previousTimeRange = this.calculatePreviousTimeRange(this.startTimeStamp, this.endTimeStamp);

            this.startTimeStamp = previousTimeRange[0];
            this.endTimeStamp = previousTimeRange[1];
            this.refreshHistoricalChartData(this.startTimeStamp, this.endTimeStamp);
        };

        ChartController.prototype.calculateNextTimeRange = function (startDate, endDate) {
            var nextTimeRange = [];
            var intervalInMillis = endDate.getTime() - startDate.getTime();

            nextTimeRange.push(endDate);
            nextTimeRange.push(new Date(endDate.getTime() + intervalInMillis));
            return nextTimeRange;
        };

        ChartController.prototype.showNextTimeRange = function () {
            var nextTimeRange = this.calculateNextTimeRange(this.startTimeStamp, this.endTimeStamp);

            this.startTimeStamp = nextTimeRange[0];
            this.endTimeStamp = nextTimeRange[1];
            this.refreshHistoricalChartData(this.startTimeStamp, this.endTimeStamp);
        };

        ChartController.prototype.hasNext = function () {
            var nextTimeRange = this.calculateNextTimeRange(this.startTimeStamp, this.endTimeStamp);

            // unsophisticated test to see if there is a next; without actually querying.
            //@fixme: pay the price, do the query!
            return nextTimeRange[1].getTime() < _.now();
        };

        ChartController.prototype.toggleTable = function () {
            this.collapseTable = !this.collapseTable;
            if (this.collapseTable) {
                this.tableButtonLabel = 'Show Table';
            } else {
                this.tableButtonLabel = 'Hide Table';
            }
        };

        ChartController.prototype.cancelAutoRefresh = function () {
            this.showAutoRefreshCancel = !this.showAutoRefreshCancel;
            this.$interval.cancel(this.updateLastTimeStampToNowPromise);
            toastr.info('Canceling Auto Refresh');
        };

        ChartController.prototype.autoRefresh = function (intervalInSeconds) {
            toastr.info('Auto Refresh Mode started');
            this.updateEndTimeStampToNow = !this.updateEndTimeStampToNow;
            this.showAutoRefreshCancel = true;
            if (this.updateEndTimeStampToNow) {
                this.refreshHistoricalChartDataForTimestamp();
                this.showAutoRefreshCancel = true;
                this.updateLastTimeStampToNowPromise = this.$interval(function () {
                    this.endTimeStamp = new Date();
                    this.refreshHistoricalChartData();
                }, intervalInSeconds * 1000);
            } else {
                this.$interval.cancel(this.updateLastTimeStampToNowPromise);
            }

            this.$scope.$on('$destroy', function () {
                this.$interval.cancel(this.updateLastTimeStampToNowPromise);
            });
        };

        ChartController.prototype.refreshChartDataNow = function (startTime) {
            var adjStartTimeStamp = moment().subtract('hours', 24).toDate();
            this.$rootScope.$broadcast('MultiChartOverlayDataChanged');
            this.endTimeStamp = new Date();
            this.refreshHistoricalChartData(angular.isUndefined(startTime) ? adjStartTimeStamp : startTime, this.endTimeStamp);
        };

        ChartController.prototype.refreshHistoricalChartData = function (startDate, endDate) {
            this.refreshHistoricalChartDataForTimestamp(startDate.getTime(), endDate.getTime());
        };

        ChartController.prototype.refreshHistoricalChartDataForTimestamp = function (startTime, endTime) {
            var that = this;

            // calling refreshChartData without params use the model values
            if (angular.isUndefined(endTime)) {
                endTime = this.endTimeStamp.getTime();
            }
            if (angular.isUndefined(startTime)) {
                startTime = this.startTimeStamp.getTime();
            }

            //
            //        if (startTime >= endTime) {
            //            $log.warn('Start Date was >= End Date');
            //            return;
            //        }
            if (this.searchId !== '') {
                this.metricDataService.getMetricsForTimeRange(this.searchId, new Date(startTime), new Date(endTime)).then(function (response) {
                    // we want to isolate the response from the data we are feeding to the chart
                    that.bucketedDataPoints = that.formatBucketedChartOutput(response);

                    if (that.bucketedDataPoints.length !== 0) {
                        // this is basically the DTO for the chart
                        that.chartData = {
                            id: that.searchId,
                            startTimeStamp: that.startTimeStamp,
                            endTimeStamp: that.endTimeStamp,
                            dataPoints: that.bucketedDataPoints,
                            contextDataPoints: that.contextDataPoints,
                            annotationDataPoints: []
                        };
                    } else {
                        that.noDataFoundForId(that.searchId);
                    }
                }, function (error) {
                    toastr.error('Error Loading Chart Data: ' + error);
                });
            }
        };

        ChartController.prototype.formatBucketedChartOutput = function (response) {
            //  The schema is different for bucketed output
            return _.map(response, function (point) {
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
        };

        ChartController.prototype.togglePreviousRangeDataOverlay = function () {
            if (this.showPreviousRangeDataOverlay) {
                this.chartData.prevDataPoints = [];
            } else {
                this.overlayPreviousRangeData();
            }
        };

        ChartController.prototype.overlayPreviousRangeData = function () {
            var previousTimeRange = this.calculatePreviousTimeRange(this.startTimeStamp, this.endTimeStamp);

            if (this.searchId !== '') {
                this.metricDataService.getMetricsForTimeRange(this.searchId, previousTimeRange[0], previousTimeRange[1]).then(function (response) {
                    // we want to isolate the response from the data we are feeding to the chart
                    var prevTimeRangeBucketedDataPoints = this.formatPreviousBucketedOutput(response);

                    if (angular.isDefined(prevTimeRangeBucketedDataPoints) && prevTimeRangeBucketedDataPoints.length !== 0) {
                        // this is basically the DTO for the chart
                        this.chartData = {
                            id: this.searchId,
                            prevStartTimeStamp: previousTimeRange[0],
                            prevEndTimeStamp: previousTimeRange[1],
                            prevDataPoints: prevTimeRangeBucketedDataPoints,
                            dataPoints: this.bucketedDataPoints,
                            contextDataPoints: this.contextDataPoints,
                            annotationDataPoints: []
                        };
                    } else {
                        this.noDataFoundForId(this.searchId);
                    }
                }, function (error) {
                    toastr.error('Error loading Prev Range graph data', 'Status: ' + error);
                });
            }
        };

        ChartController.prototype.formatPreviousBucketedOutput = function (response) {
            //  The schema is different for bucketed output
            var mappedNew = _.map(response, function (point, i) {
                return {
                    timestamp: this.bucketedDataPoints[i].timestamp,
                    originalTimestamp: point.timestamp,
                    value: !angular.isNumber(point.value) ? 0 : point.value,
                    avg: (point.empty) ? 0 : point.avg,
                    min: !angular.isNumber(point.min) ? 0 : point.min,
                    max: !angular.isNumber(point.max) ? 0 : point.max,
                    empty: point.empty
                };
            });
            return mappedNew;
        };

        ChartController.prototype.toggleContextZoom = function () {
            if (this.showContextZoom) {
                this.chartData.contextDataPoints = [];
            } else {
                this.refreshContextChart();
            }
        };

        ChartController.prototype.refreshContextChart = function () {
            // unsophisticated default time range to avoid DB checking right now
            // @fixme: add a real service to determine unbounded range
            var endTime = _.now(), startTime = moment().subtract('months', 24).valueOf();

            console.debug('refreshChartContext');
            if (this.searchId !== '') {
                if (startTime >= endTime) {
                    this.$log.warn('Start Date was >= End Date');
                    return;
                }

                this.metricDataService.getMetricsForTimeRange(this.searchId, new Date(startTime), new Date(endTime), 300).then(function (response) {
                    this.chartData.contextDataPoints = this.formatContextOutput(response);

                    if (angular.isUndefined(this.chartData.contextDataPoints) || this.chartData.contextDataPoints.length === 0) {
                        this.noDataFoundForId(this.searchId);
                    }
                }, function (error) {
                    toastr.error('Error loading Context graph data', 'Status: ' + error);
                });
            }
        };

        ChartController.prototype.formatContextOutput = function (response) {
            //  The schema is different for bucketed output
            return _.map(response, function (point) {
                return {
                    timestamp: point.timestamp,
                    value: !angular.isNumber(point.value) ? 0 : point.value,
                    avg: (point.empty) ? 0 : point.avg,
                    empty: point.empty
                };
            });
        };
        ChartController.$inject = ['$scope', '$rootScope', '$interval', '$log', 'metricDataService'];
        return ChartController;
    })();
    Controllers.ChartController = ChartController;

    angular.module('chartingApp').controller('ChartController', ChartController);
})(Controllers || (Controllers = {}));
//# sourceMappingURL=chart-controller.js.map
