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

        vm.chartParams = {
            searchId: "",
            startTimeStamp: moment().subtract('hours', 24).toDate(), //default time period set to 24 hours
            endTimeStamp: new Date(),
            dateRange: moment().subtract('hours', 24).from(moment(), true),
            updateEndTimeStampToNow: false,
            collapseTable: true,
            tableButtonLabel: "Show Table",
            showAvgLine: true,
            showPreviousRangeDataOverlay: false,
            chartType: "bar",
            chartTypes: ["bar", "line", "area", "scatter"]
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

//        $rootScope.$on("DateRangeMove", function (event, message) {
//            $log.debug("DateRangeMove on chart Detected.");
//        });

        $rootScope.$on("GraphTimeRangeChangedEvent", function (event, message) {
            var timeRange = message;

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
            return nextTimeRange[1].getTime() < _.now();
        };


        vm.toggleTable = function () {
            vm.chartParams.collapseTable = !vm.chartParams.collapseTable;
            if (vm.chartParams.collapseTable) {
                vm.chartParams.tableButtonLabel = "Show Table";
            } else {
                vm.chartParams.tableButtonLabel = "Hide Table";
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

        vm.refreshChartData = function () {

            metricDataService.getMetricsForTimeRange(vm.chartParams.searchId, vm.chartParams.startTimeStamp, vm.chartParams.endTimeStamp)
                .success(function (response) {
                    // we want to isolate the response from the data we are feeding to the chart
                    $rootScope.bucketedDataPoints = formatBucketedOutput(response);

                    if ($rootScope.bucketedDataPoints.length !== 0) {

                        // this is basically the DTO for the chart
                        vm.chartData = {
                            id: vm.chartParams.id,
                            startTimeStamp: vm.chartParams.startTimeStamp,
                            endTimeStamp: vm.chartParams.endTimeStamp,
                            dataPoints: $rootScope.bucketedDataPoints
                        };

                    } else {
                        $log.warn('No Data found for id: ' + vm.chartParams.searchId);
                        toastr.warn('No Data found for id: ' + vm.chartParams.searchId);
                    }

                }).error(function (response, status) {
                    $log.error('Error loading graph data: ' + response);
                    toastr.error('Error loading graph data', 'Status: ' + status);
                });
        };

        function formatBucketedOutput(response) {
            //  The schema is different for bucketized output
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

            metricDataService.getMetricsForTimeRange(vm.chartParams.searchId, previousTimeRange[0], previousTimeRange[1])
                .success(function (response) {
                    // we want to isolate the response from the data we are feeding to the chart
                    var prevTimeRangeBucketizedDataPoints = formatPreviousBucketedOutput(response);

                    if (angular.isDefined(prevTimeRangeBucketizedDataPoints) && prevTimeRangeBucketizedDataPoints.length !== 0) {

                        // this is basically the DTO for the chart
                        vm.chartData = {
                            id: vm.chartParams.id,
                            prevStartTimeStamp: previousTimeRange[0],
                            prevEndTimeStamp: previousTimeRange[1],
                            prevDataPoints: prevTimeRangeBucketizedDataPoints,
                            dataPoints: $rootScope.bucketedDataPoints
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

        function formatPreviousBucketedOutput(response) {
            //  The schema is different for bucketized output
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


    }]);
