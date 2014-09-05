/// <reference path="../../vendor/vendor.d.ts" />
'use strict';
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
    function ChartController($scope, $rootScope, $interval, $log, metricDataService) {
        this.$scope = $scope;
        this.$rootScope = $rootScope;
        this.$interval = $interval;
        this.$log = $log;
        this.metricDataService = metricDataService;
        this.bucketedDataPoints = [];
        this.contextDataPoints = [];
        this.chartParams = {
            searchId: '',
            startTimeStamp: moment().subtract('hours', 24).toDate(),
            endTimeStamp: new Date(),
            dateRange: moment().subtract('hours', 24).from(moment(), true),
            updateEndTimeStampToNow: false,
            collapseTable: true,
            tableButtonLabel: 'Show Table',
            showAvgLine: true,
            hideHighLowValues: false,
            showPreviousRangeDataOverlay: false,
            showContextZoom: true,
            showAutoRefreshCancel: false,
            chartType: 'bar',
            chartTypes: ['bar', 'line', 'area', 'scatter', 'scatterline', 'candlestick', 'histogram']
        };
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
        $scope.vm = this;
    }
    //        $rootScope.$on('DateRangeMove', function (event, message) {
    //            $log.debug('DateRangeMove on chart Detected.');
    //        });
    //    $rootScope.$on(  'GraphTimeRangeChangedEvent' , function(event, timeRange) {
    //
    //        // set to the new published time range
    //        chartParams.startTimeStamp = timeRange[0];
    //        chartParams.endTimeStamp = timeRange[1];
    //        chartParams.dateRange = moment(timeRange[0]).from(moment(timeRange[1]));
    //        refreshHistoricalChartData(chartParams.startTimeStamp, chartParams.endTimeStamp);
    //    });
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
        var previousTimeRange = this.calculatePreviousTimeRange(this.chartParams.startTimeStamp, this.chartParams.endTimeStamp);

        this.chartParams.startTimeStamp = previousTimeRange[0];
        this.chartParams.endTimeStamp = previousTimeRange[1];
        this.refreshHistoricalChartData(this.chartParams.startTimeStamp, this.chartParams.endTimeStamp);
    };

    ChartController.prototype.calculateNextTimeRange = function (startDate, endDate) {
        var nextTimeRange = [];
        var intervalInMillis = endDate.getTime() - startDate.getTime();

        nextTimeRange.push(endDate);
        nextTimeRange.push(new Date(endDate.getTime() + intervalInMillis));
        return nextTimeRange;
    };

    ChartController.prototype.showNextTimeRange = function () {
        var nextTimeRange = this.calculateNextTimeRange(this.chartParams.startTimeStamp, this.chartParams.endTimeStamp);

        this.chartParams.startTimeStamp = nextTimeRange[0];
        this.chartParams.endTimeStamp = nextTimeRange[1];
        this.refreshHistoricalChartData(this.chartParams.startTimeStamp, this.chartParams.endTimeStamp);
    };

    ChartController.prototype.hasNext = function () {
        var nextTimeRange = this.calculateNextTimeRange(this.chartParams.startTimeStamp, this.chartParams.endTimeStamp);

        // unsophisticated test to see if there is a next; without actually querying.
        //@fixme: pay the price, do the query!
        return nextTimeRange[1].getTime() < _.now();
    };

    ChartController.prototype.toggleTable = function () {
        this.chartParams.collapseTable = !this.chartParams.collapseTable;
        if (this.chartParams.collapseTable) {
            this.chartParams.tableButtonLabel = 'Show Table';
        } else {
            this.chartParams.tableButtonLabel = 'Hide Table';
        }
    };

    //    this.$scope.$on(  '$destroy' , function() {
    //        $interval.cancel(updateLastTimeStampToNowPromise);
    //    } );
    ChartController.prototype.cancelAutoRefresh = function () {
        this.chartParams.showAutoRefreshCancel = !this.chartParams.showAutoRefreshCancel;
        this.$interval.cancel(this.updateLastTimeStampToNowPromise);
        toastr.info('Canceling Auto Refresh');
    };

    ChartController.prototype.autoRefresh = function (intervalInSeconds) {
        toastr.info('Auto Refresh Mode started');
        this.chartParams.updateEndTimeStampToNow = !this.chartParams.updateEndTimeStampToNow;
        this.chartParams.showAutoRefreshCancel = true;
        if (this.chartParams.updateEndTimeStampToNow) {
            this.refreshHistoricalChartData();
            this.showAutoRefreshCancel = true;
            this.updateLastTimeStampToNowPromise = this.$interval(function () {
                this.chartParams.endTimeStamp = new Date();
                this.refreshHistoricalChartData();
            }, intervalInSeconds * 1000);
        } else {
            this.$interval.cancel(this.updateLastTimeStampToNowPromise);
        }
    };

    ChartController.prototype.refreshChartDataNow = function (startTime) {
        this.$rootScope.$broadcast('MultiChartOverlayDataChanged');
        this.chartParams.endTimeStamp = new Date();
        this.refreshHistoricalChartData(startTime, new Date());
    };

    ChartController.prototype.refreshHistoricalChartData = function (startDate, endDate) {
        this.refreshHistoricalChartDataForTimestamp(startDate.getTime(), endDate.getTime());
    };

    ChartController.prototype.refreshHistoricalChartDataForTimestamp = function (startTime, endTime) {
        // calling refreshChartData without params use the model values
        if (angular.isUndefined(endTime)) {
            endTime = this.chartParams.endTimeStamp;
        }
        if (angular.isUndefined(startTime)) {
            startTime = this.chartParams.startTimeStamp;
        }

        //
        //        if (startTime >= endTime) {
        //            $log.warn('Start Date was >= End Date');
        //            return;
        //        }
        if (this.chartParams.searchId !== '') {
            this.metricDataService.getMetricsForTimeRange(this.chartParams.searchId, startTime, endTime).then(function (response) {
                // we want to isolate the response from the data we are feeding to the chart
                this.bucketedDataPoints = this.formatBucketedOutput(response);

                if (this.bucketedDataPoints.length !== 0) {
                    // this is basically the DTO for the chart
                    this.chartData = {
                        id: this.chartParams.searchId,
                        startTimeStamp: this.chartParams.startTimeStamp,
                        endTimeStamp: this.chartParams.endTimeStamp,
                        dataPoints: this.bucketedDataPoints,
                        contextDataPoints: this.contextDataPoints,
                        annotationDataPoints: []
                    };
                } else {
                    this.noDataFoundForId(this.chartParams.searchId);
                }
            }, function (error) {
                toastr.error('Error Loading Chart Data: ' + error);
            });
        }
    };

    ChartController.prototype.formatBucketedOutput = function (response) {
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
        if (this.chartParams.showPreviousRangeDataOverlay) {
            this.chartData.prevDataPoints = [];
        } else {
            this.overlayPreviousRangeData();
        }
    };

    ChartController.prototype.overlayPreviousRangeData = function () {
        var previousTimeRange = this.calculatePreviousTimeRange(this.chartParams.startTimeStamp, this.chartParams.endTimeStamp);

        if (this.chartParams.searchId !== '') {
            this.metricDataService.getMetricsForTimeRange(this.chartParams.searchId, previousTimeRange[0], previousTimeRange[1]).then(function (response) {
                // we want to isolate the response from the data we are feeding to the chart
                var prevTimeRangeBucketedDataPoints = this.formatPreviousBucketedOutput(response);

                if (angular.isDefined(prevTimeRangeBucketedDataPoints) && prevTimeRangeBucketedDataPoints.length !== 0) {
                    // this is basically the DTO for the chart
                    this.chartData = {
                        id: this.chartParams.searchId,
                        prevStartTimeStamp: previousTimeRange[0],
                        prevEndTimeStamp: previousTimeRange[1],
                        prevDataPoints: prevTimeRangeBucketedDataPoints,
                        dataPoints: this.bucketedDataPoints,
                        contextDataPoints: this.contextDataPoints,
                        annotationDataPoints: []
                    };
                } else {
                    this.noDataFoundForId(this.chartParams.searchId);
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
        if (this.chartParams.showContextZoom) {
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
        if (this.chartParams.searchId !== '') {
            if (startTime >= endTime) {
                this.$log.warn('Start Date was >= End Date');
                return;
            }

            this.metricDataService.getMetricsForTimeRange(this.chartParams.searchId, new Date(startTime), new Date(endTime), 300).then(function (response) {
                this.chartData.contextDataPoints = this.formatContextOutput(response);

                if (angular.isUndefined(this.chartData.contextDataPoints) || this.chartData.contextDataPoints.length === 0) {
                    this.noDataFoundForId(this.chartParams.searchId);
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

angular.module('chartingApp').controller('ChartController', ChartController);
//# sourceMappingURL=chart-controller.js.map
