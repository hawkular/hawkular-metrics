/// <reference path="../../vendor/vendor.d.ts" />
'use strict';

interface IBucketDataPoints {
    timestamp:number;
    date: Date;
    value: number;
    avg: number;
    min: number;
    max: number;
    empty: boolean;
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
class ChartController {
    static  $inject = ['$scope', '$rootScope', '$interval', '$log', 'metricDataService' ];

    constructor($scope, $rootScope:ng.IRootScopeService, $interval:ng.IIntervalService, $log:ng.ILogService, metricDataService) {
        $scope.vm = this;
    }
    updateLastTimeStampToNowPromise;
    bucketedDataPoints = [];
    contextDataPoints = [];

    chartParams = {
        searchId: '',
        startTimeStamp: moment().subtract('hours', 24).toDate(), //default time period set to 24 hours
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

    dateTimeRanges = [
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

//        $rootScope.$on('DateRangeMove', function (event, message) {
//            $log.debug('DateRangeMove on chart Detected.');
//        });

//    $rootScope . $on(  'GraphTimeRangeChangedEvent' , function(event, timeRange) {
//
//        // set to the new published time range
//        chartParams.startTimeStamp = timeRange[0];
//        chartParams.endTimeStamp = timeRange[1];
//        chartParams.dateRange = moment(timeRange[0]).from(moment(timeRange[1]));
//        refreshHistoricalChartData(chartParams.startTimeStamp, chartParams.endTimeStamp);
//    }
//
//);

    private noDataFoundForId(id):void {
        $log.warn('No Data found for id: ' + id);
        toastr.warning('No Data found for id: ' + id);
    }

    showPreviousTimeRange () {
        var previousTimeRange = calculatePreviousTimeRange(chartParams.startTimeStamp, chartParams.endTimeStamp);

        chartParams.startTimeStamp = previousTimeRange[0];
        chartParams.endTimeStamp = previousTimeRange[1];
        refreshHistoricalChartData(chartParams.startTimeStamp, chartParams.endTimeStamp);

    }

    private calculatePreviousTimeRange(startDate, endDate) {
        var previousTimeRange = [];
        var intervalInMillis = endDate - startDate;

        previousTimeRange.push(new Date(startDate.getTime() - intervalInMillis));
        previousTimeRange.push(startDate);
        return previousTimeRange;
    }

    private calculateNextTimeRange(startDate, endDate) {
        var nextTimeRange = [];
        var intervalInMillis = endDate - startDate;

        nextTimeRange.push(endDate);
        nextTimeRange.push(new Date(endDate.getTime() + intervalInMillis));
        return nextTimeRange;
    }


    showNextTimeRange():void {
        nextTimeRange = calculateNextTimeRange(chartParams.startTimeStamp, chartParams.endTimeStamp);

        chartParams.startTimeStamp = nextTimeRange[0];
        chartParams.endTimeStamp = nextTimeRange[1];
        refreshHistoricalChartData(chartParams.startTimeStamp, chartParams.endTimeStamp);

    }


    hasNext():boolean {
        nextTimeRange = calculateNextTimeRange(chartParams.startTimeStamp, chartParams.endTimeStamp);
        // unsophisticated test to see if there is a next; without actually querying.
        //@fixme: pay the price, do the query!
        return nextTimeRange[1].getTime() < _.now();
    }


    toggleTable():void {
        chartParams.collapseTable = !chartParams.collapseTable;
        if (chartParams.collapseTable) {
            chartParams.tableButtonLabel = 'Show Table';
        } else {
            chartParams.tableButtonLabel = 'Hide Table';
        }
    }


//    $scope . $on(  '$destroy' , function() {
//        $interval.cancel(updateLastTimeStampToNowPromise);
//    } );

    cancelAutoRefresh():void {
        chartParams.showAutoRefreshCancel = !chartParams.showAutoRefreshCancel;
        $interval.cancel(updateLastTimeStampToNowPromise);
        toastr.info('Canceling Auto Refresh');
    }

    autoRefresh(intervalInSeconds):void {
        toastr.info('Auto Refresh Mode started');
        chartParams.updateEndTimeStampToNow = !chartParams.updateEndTimeStampToNow;
        chartParams.showAutoRefreshCancel = true;
        if (chartParams.updateEndTimeStampToNow) {
            refreshHistoricalChartData();
            showAutoRefreshCancel = true;
            updateLastTimeStampToNowPromise = $interval(function () {
                chartParams.endTimeStamp = new Date();
                refreshHistoricalChartData();
            }, intervalInSeconds * 1000);

        } else {
            $interval.cancel(updateLastTimeStampToNowPromise);
        }

    }

    refreshChartDataNow(startTime):void {
        $rootScope.$broadcast('MultiChartOverlayDataChanged');
        chartParams.endTimeStamp = new Date();
        refreshHistoricalChartData(startTime, new Date());
    }

    refreshHistoricalChartData(startTime, endTime):void {
        // calling refreshChartData without params use the model values
        if (angular.isUndefined(endTime)) {
            endTime = chartParams.endTimeStamp;
        }
        if (angular.isUndefined(startTime)) {
            startTime = chartParams.startTimeStamp;
        }

//
//        if (startTime >= endTime) {
//            $log.warn('Start Date was >= End Date');
//            return;
//        }

        if (chartParams.searchId !== '') {

            metricDataService.getMetricsForTimeRange(chartParams.searchId, startTime, endTime)
                .then(function (response) {
                    // we want to isolate the response from the data we are feeding to the chart
                    bucketedDataPoints = formatBucketedOutput(response);

                    if (bucketedDataPoints.length !== 0) {

                        // this is basically the DTO for the chart
                        chartData = {
                            id: chartParams.searchId,
                            startTimeStamp: chartParams.startTimeStamp,
                            endTimeStamp: chartParams.endTimeStamp,
                            dataPoints: bucketedDataPoints,
                            contextDataPoints: contextDataPoints,
                            annotationDataPoints: []
                        };

                    } else {
                        noDataFoundForId(chartParams.searchId);
                    }

                }, function (error) {
                    toastr.error('Error Loading Chart Data: ' + error);
                });
        }

    }

    private formatBucketedOutput(response): IBucketDataPoints[] {
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
    }


    togglePreviousRangeDataOverlay():void {
        if (chartParams.showPreviousRangeDataOverlay) {
            chartData.prevDataPoints = [];
        } else {
            overlayPreviousRangeData();
        }
    }


    overlayPreviousRangeData():void {
        var previousTimeRange = calculatePreviousTimeRange(chartParams.startTimeStamp, chartParams.endTimeStamp);

        if (chartParams.searchId !== '') {
            metricDataService.getMetricsForTimeRange(chartParams.searchId, previousTimeRange[0], previousTimeRange[1])
                .then(function (response) {
                    // we want to isolate the response from the data we are feeding to the chart
                    var prevTimeRangeBucketedDataPoints = formatPreviousBucketedOutput(response);

                    if (angular.isDefined(prevTimeRangeBucketedDataPoints) && prevTimeRangeBucketedDataPoints.length !== 0) {

                        // this is basically the DTO for the chart
                        chartData = {
                            id: chartParams.searchId,
                            prevStartTimeStamp: previousTimeRange[0],
                            prevEndTimeStamp: previousTimeRange[1],
                            prevDataPoints: prevTimeRangeBucketedDataPoints,
                            dataPoints: bucketedDataPoints,
                            contextDataPoints: contextDataPoints,
                            annotationDataPoints: []
                        };

                    } else {
                        noDataFoundForId(chartParams.searchId);
                    }

                }, function (error) {
                    toastr.error('Error loading Prev Range graph data', 'Status: ' + error);
                });
        }
    }

    private formatPreviousBucketedOutput(response) {
        //  The schema is different for bucketed output
        var mappedNew = _.map(response, function (point, i) {
            return {
                timestamp: bucketedDataPoints[i].timestamp,
                originalTimestamp: point.timestamp,
                value: !angular.isNumber(point.value) ? 0 : point.value,
                avg: (point.empty) ? 0 : point.avg,
                min: !angular.isNumber(point.min) ? 0 : point.min,
                max: !angular.isNumber(point.max) ? 0 : point.max,
                empty: point.empty
            };
        });
        return mappedNew;
    }


    toggleContextZoom():void {
        if (chartParams.showContextZoom) {
            chartData.contextDataPoints = [];
        } else {
            refreshContextChart();
        }
    }

    refreshContextChart():void {
        // unsophisticated default time range to avoid DB checking right now
        // @fixme: add a real service to determine unbounded range
        var endTime = _.now(),
            startTime = moment().subtract('months', 24).valueOf();

        console.debug('refreshChartContext');
        if (chartParams.searchId !== '') {
            if (startTime >= endTime) {
                $log.warn('Start Date was >= End Date');
                return;
            }

            metricDataService.getMetricsForTimeRange(chartParams.searchId, new Date(startTime), new Date(endTime), 300)
                .then(function (response) {

                    chartData.contextDataPoints = formatContextOutput(response);

                    if (angular.isUndefined(chartData.contextDataPoints) || chartData.contextDataPoints.length === 0) {
                        noDataFoundForId(chartParams.searchId);
                    }

                }, function (error) {
                    toastr.error('Error loading Context graph data', 'Status: ' + error);
                });
        }
    }

    private formatContextOutput(response) {
        //  The schema is different for bucketed output
        return _.map(response, function (point) {
            return {
                timestamp: point.timestamp,
                value: !angular.isNumber(point.value) ? 0 : point.value,
                avg: (point.empty) ? 0 : point.avg,
                empty: point.empty
            };
        });

    }
}

angular.module('chartingApp')
    .controller('ChartController', ['$scope', '$rootScope', '$interval', '$log', 'metricDataService', ChartController]);
