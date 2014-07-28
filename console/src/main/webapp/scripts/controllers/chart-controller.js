'use strict';


/**
 * @ngdoc controller
 * @name ChartController
 * @param {expression} chartController
 * @description This controller is responsible for handling activity related to the Chart tab.
 */
angular.module('chartingApp')
    .controller('ChartController', ['$scope', '$rootScope', '$interval', '$log', 'metricDataService', function ($scope, $rootScope, $interval, $log, metricDataService) {
        var updateLastTimeStampToNowPromise,
            vm = this;
        $rootScope.bucketedDataPoints = [];
        // for the little range graph to select a subrange for the graph
        $rootScope.contextDataPoints = [];

        vm.chartParams = {
            searchId: "",
            startTimeStamp: moment().subtract('hours', 24).toDate(), //default time period set to 24 hours
            endTimeStamp: new Date(),
            dateRange: moment().subtract('hours', 24).from(moment(), true),
            updateEndTimeStampToNow: false,
            collapseTable: true,
            tableButtonLabel: 'Show Table',
            showAvgLine: true,
            showPreviousRangeDataOverlay: false,
            showContextZoom: true,
            chartType: 'bar',
            chartTypes: ['bar', 'line', 'area', 'scatter','candlestick']
        };

        vm.dateTimeRanges = [
            { "range": "1h", "rangeInSeconds": 60 * 60 } ,
            { "range": "4h", "rangeInSeconds": 4 * 60 * 60 } ,
            { "range": "8h", "rangeInSeconds": 8 * 60 * 60 },
            { "range": "12h", "rangeInSeconds": 12 * 60 * 60 },
            { "range": "1d", "rangeInSeconds": 24 * 60 * 60 },
            { "range": "5d", "rangeInSeconds": 5 * 24 * 60 * 60 },
            { "range": "1m", "rangeInSeconds": 30 * 24 * 60 * 60 },
            { "range": "3m", "rangeInSeconds": 3 * 30 * 24 * 60 * 60 },
            { "range": "6m", "rangeInSeconds": 6 * 30 * 24 * 60 * 60 }
        ];

//        $rootScope.$on('DateRangeMove', function (event, message) {
//            $log.debug('DateRangeMove on chart Detected.');
//        });

        $rootScope.$on('GraphTimeRangeChangedEvent', function (event, timeRange) {

            // set to the new published time range
            vm.chartParams.startTimeStamp = timeRange[0];
            vm.chartParams.endTimeStamp = timeRange[1];
            vm.chartParams.dateRange = moment(timeRange[0]).from(moment(timeRange[1]));
            vm.refreshChartData();
        });


        vm.showPreviousTimeRange = function () {
            var previousTimeRange;
            previousTimeRange = calculatePreviousTimeRange(vm.chartParams.startTimeStamp, vm.chartParams.endTimeStamp);

            vm.chartParams.startTimeStamp = previousTimeRange[0];
            vm.chartParams.endTimeStamp = previousTimeRange[1];
            vm.refreshChartData();

        };

        function calculatePreviousTimeRange(startDate, endDate) {
            var previousTimeRange = [];
            var intervalInMillis = endDate - startDate;

            previousTimeRange.push(new Date(startDate.getTime() - intervalInMillis));
            previousTimeRange.push(startDate);
            return previousTimeRange;
        }

        function calculateNextTimeRange(startDate, endDate) {
            var nextTimeRange = [];
            var intervalInMillis = endDate - startDate;

            nextTimeRange.push(endDate);
            nextTimeRange.push(new Date(endDate.getTime() + intervalInMillis));
            return nextTimeRange;
        }


        vm.showNextTimeRange = function () {
            var nextTimeRange = calculateNextTimeRange(vm.chartParams.startTimeStamp, vm.chartParams.endTimeStamp);

            vm.chartParams.startTimeStamp = nextTimeRange[0];
            vm.chartParams.endTimeStamp = nextTimeRange[1];
            vm.refreshChartData();

        };


        vm.hasNext = function () {
            var nextTimeRange = calculateNextTimeRange(vm.chartParams.startTimeStamp, vm.chartParams.endTimeStamp);
            // unsophisticated test to see if there is a next; without actually querying.
            //@fixme: pay the price, do the query!
            return nextTimeRange[1].getTime() < _.now();
        };


        vm.toggleTable = function () {
            vm.chartParams.collapseTable = !vm.chartParams.collapseTable;
            if (vm.chartParams.collapseTable) {
                vm.chartParams.tableButtonLabel = 'Show Table';
            } else {
                vm.chartParams.tableButtonLabel = 'Hide Table';
            }
        };


        $scope.$on('$destroy', function () {
            $interval.cancel(updateLastTimeStampToNowPromise);
        });


        vm.autoRefresh = function () {
            vm.chartParams.updateEndTimeStampToNow = !vm.chartParams.updateEndTimeStampToNow;
            if (vm.chartParams.updateEndTimeStampToNow) {
                vm.refreshChartData();
                updateLastTimeStampToNowPromise = $interval(function () {
                    vm.chartParams.endTimeStamp = new Date();
                    vm.refreshChartData();
                }, 10 * 1000);

            } else {
                $interval.cancel(updateLastTimeStampToNowPromise);
            }

        };

        vm.refreshChartData = function (startTime, endTime) {
            // calling refreshChartData without params use the model values
            if (angular.isUndefined(endTime)) {
                endTime = vm.chartParams.endTimeStamp;
            }
            if (angular.isUndefined(startTime)) {
                startTime = vm.chartParams.startTimeStamp;
            }


            if (startTime >= endTime) {
                $log.warn('Start Date was >= End Date');
                return;
            }

            if (vm.chartParams.searchId !== "") {

                metricDataService.getMetricsForTimeRange(vm.chartParams.searchId, startTime, endTime)
                    .success(function (response) {
                        // we want to isolate the response from the data we are feeding to the chart
                        $rootScope.bucketedDataPoints = formatBucketedOutput(response);

                        if ($rootScope.bucketedDataPoints.length !== 0) {

                            // this is basically the DTO for the chart
                            vm.chartData = {
                                id: vm.chartParams.searchId,
                                startTimeStamp: vm.chartParams.startTimeStamp,
                                endTimeStamp: vm.chartParams.endTimeStamp,
                                dataPoints: $rootScope.bucketedDataPoints,
                                contextDataPoints: $rootScope.contextDataPoints,
                                annotationDataPoints: []
                            };

                        } else {
                            $log.warn('No Data found for id: ' + vm.chartParams.searchId);
                            toastr.warn('No Data found for id: ' + vm.chartParams.searchId);
                        }

                    }).error(function (response, status) {
                        $log.error('Error loading graph data: ' + response);
                        toastr.error('Error loading graph data', 'Status: ' + status);
                    });
            }
        };

        function formatBucketedOutput(response) {
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


        vm.togglePreviousRangeDataOverlay = function () {
            if (vm.chartParams.showPreviousRangeDataOverlay) {
                vm.chartData.prevDataPoints = [];
            } else {
                overlayPreviousRangeData();
            }
        };

        function overlayPreviousRangeData() {
            var previousTimeRange = calculatePreviousTimeRange(vm.chartParams.startTimeStamp, vm.chartParams.endTimeStamp);

            if (vm.chartParams.searchId !== "") {
                metricDataService.getMetricsForTimeRange(vm.chartParams.searchId, previousTimeRange[0], previousTimeRange[1])
                    .success(function (response) {
                        // we want to isolate the response from the data we are feeding to the chart
                        var prevTimeRangeBucketedDataPoints = formatPreviousBucketedOutput(response);

                        if (angular.isDefined(prevTimeRangeBucketedDataPoints) && prevTimeRangeBucketedDataPoints.length !== 0) {

                            // this is basically the DTO for the chart
                            vm.chartData = {
                                id: vm.chartParams.searchId,
                                prevStartTimeStamp: previousTimeRange[0],
                                prevEndTimeStamp: previousTimeRange[1],
                                prevDataPoints: prevTimeRangeBucketedDataPoints,
                                dataPoints: $rootScope.bucketedDataPoints,
                                contextDataPoints: $rootScope.contextDataPoints,
                                annotationDataPoints: []
                            };

                        } else {
                            $log.warn('No Prev Range Data found for id: ' + vm.chartParams.searchId);
                            toastr.warn('No Prev Range Data found for id: ' + vm.chartParams.searchId);
                        }

                    }).error(function (response, status) {
                        $log.error('Error loading Prev Range graph data: ' + response);
                        toastr.error('Error loading Prev Range graph data', 'Status: ' + status);
                    });
            }
        }

        function formatPreviousBucketedOutput(response) {
            //  The schema is different for bucketed output
            var mappedNew = _.map(response, function (point, i) {
                return {
                    timestamp: $rootScope.bucketedDataPoints[i].timestamp,
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


        vm.toggleContextZoom = function () {
            if (vm.chartParams.showContextZoom) {
                vm.chartData.contextDataPoints = [];
            } else {
                refreshContextChart();
            }
        };

        function refreshContextChart() {
            // unsophisticated default time range to avoid DB checking right now
            // @fixme: add a real service to determine unbounded range
            var endTime = _.now(),
                startTime = moment().subtract('months', 24).valueOf();

            console.debug('refreshChartContext');
            if (vm.chartParams.searchId !== "") {
                if (startTime >= endTime) {
                    $log.warn('Start Date was >= End Date');
                    return;
                }

                metricDataService.getMetricsForTimeRange(vm.chartParams.searchId, new Date(startTime), new Date(endTime), 300)
                    .success(function (response) {

                        vm.chartData.contextDataPoints = formatContextOutput(response);

                        if (angular.isUndefined(vm.chartData.contextDataPoints) || vm.chartData.contextDataPoints.length === 0) {
                            $log.warn('No Context Data found for id: ' + vm.chartParams.searchId);
                            toastr.warn('No Context Data found for id: ' + vm.chartParams.searchId);
                        }

                    }).error(function (response, status) {
                        $log.error('Error loading Context graph data: ' + response);
                        toastr.error('Error loading Context graph data', 'Status: ' + status);
                    });
            }

            function formatContextOutput(response) {
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


    }]);
