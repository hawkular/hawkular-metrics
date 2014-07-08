'use strict';


/**
 * @ngdoc controller
 * @name ChartController
 * @param {expression} chartController
 * @description This controller is responsible for handling activity related to the Chart tab.
 */
angular.module('chartingApp')
    .controller('ChartController', ['$scope', '$rootScope', '$interval', '$log', 'metricDataService', function ($scope, $rootScope, $interval, $log, metricDataService) {
        var updateLastTimeStampToNowPromise;
        $rootScope.bucketedDataPoints = [];

        $scope.chartParams = {
            searchId: "",
            startTimeStamp: moment().subtract('hours', 8).toDate(), //default time period set to 8 hours
            endTimeStamp: new Date(),
            dateRange: moment().subtract('hours', 8).from(moment(), true),
            updateEndTimeStampToNow: false,
            collapseTable: true,
            tableButtonLabel: "Show Table"
        };

        $scope.dateTimeRanges = [
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

        $rootScope.$on("DateRangeChanged", function (event, message) {
            $log.debug("DateRangeChanged Fired from Chart!");
        });

//        $rootScope.$on("DateRangeMove", function (event, message) {
//            $log.debug("DateRangeMove on chart Detected.");
//        });


        $scope.autoRefresh = function () {
            $scope.chartParams.updateEndTimeStampToNow = !$scope.chartParams.updateEndTimeStampToNow;
            if ($scope.chartParams.updateEndTimeStampToNow) {
                $scope.refreshChartData();
                updateLastTimeStampToNowPromise = $interval(function () {
                    $scope.chartParams.endTimeStamp = new Date();
                    $scope.refreshChartData();
                }, 30 * 1000);

            } else {
                $interval.cancel(updateLastTimeStampToNowPromise);
            }

        };

        $scope.showPreviousTimeRange = function () {
            var previousTimeRange;
            previousTimeRange = calculatePreviousTimeRange($scope.chartParams.startTimeStamp, $scope.chartParams.endTimeStamp);

            $scope.chartParams.startTimeStamp = previousTimeRange[0];
            $scope.chartParams.endTimeStamp = previousTimeRange[1];
            $scope.refreshChartData();

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


        $scope.showNextTimeRange = function () {
            var nextTimeRange = calculateNextTimeRange($scope.chartParams.startTimeStamp, $scope.chartParams.endTimeStamp);

            $scope.chartParams.startTimeStamp = nextTimeRange[0];
            $scope.chartParams.endTimeStamp = nextTimeRange[1];
            $scope.refreshChartData();

        };


        $scope.hasNext = function () {
            var nextTimeRange = calculateNextTimeRange($scope.chartParams.startTimeStamp, $scope.chartParams.endTimeStamp);
            // unsophisticated test to see if there is a next; without actually querying.
            return nextTimeRange[1].getTime() < moment().valueOf();
        };


        $scope.toggleTable = function () {
            $scope.chartParams.collapseTable = !$scope.chartParams.collapseTable;
            if ($scope.chartParams.collapseTable) {
                $scope.chartParams.tableButtonLabel = "Show Table";
            } else {
                $scope.chartParams.tableButtonLabel = "Hide Table";
            }
        };

        $scope.$on('$destroy', function () {
            $interval.cancel(updateLastTimeStampToNowPromise);
        });

        $scope.refreshChartData = function () {

            metricDataService.getMetricsForTimeRange($scope.chartParams.searchId, $scope.chartParams.startTimeStamp, $scope.chartParams.endTimeStamp)
                .success(function (response) {
                    // we want to isolate the response from the data we are feeding to the chart
                    $rootScope.bucketedDataPoints = formatBucketizedOutput(response);

                    if ($rootScope.bucketedDataPoints.length !== 0) {

                        $log.debug("# Transformed DataPoints: " + $rootScope.bucketedDataPoints.length);

                        // this is basically the DTO for the chart
                        $scope.chartData = {
                            id: $scope.chartParams.id,
                            startTimeStamp: $scope.chartParams.startTimeStamp,
                            endTimeStamp: $scope.chartParams.endTimeStamp,
                            dataPoints: $rootScope.bucketedDataPoints
                            //nvd3DataPoints: formatForNvD3(response),
                            //rickshawDataPoints: formatForRickshaw(response)
                        };

                    } else {
                        $log.warn('No Data found for id: ' + $scope.chartParams.searchId);
                        toastr.warn('No Data found for id: ' + $scope.chartParams.searchId);
                    }

                }).error(function (response, status) {
                    $log.error('Error loading graph data: ' + response);
                    toastr.error('Error loading graph data', 'Status: ' + status);
                });
        };

        function formatBucketizedOutput(response) {
            //  The schema is different for bucketized output
            return $.map(response, function (point) {
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

        $scope.overlayPreviousRangeData = function () {
            $log.debug("pushed Overlay Previous RangeData");
            var previousTimeRange = calculatePreviousTimeRange($scope.chartParams.startTimeStamp, $scope.chartParams.endTimeStamp);

            metricDataService.getMetricsForTimeRange($scope.chartParams.searchId, previousTimeRange[0], previousTimeRange[1])
                .success(function (response) {
                    // we want to isolate the response from the data we are feeding to the chart
                    var prevTimeRangeBucketizedDataPoints = formatPreviousBucketizedOutput(response);

                    if (!angular.isUndefined(prevTimeRangeBucketizedDataPoints) && prevTimeRangeBucketizedDataPoints.length !== 0) {

                        $log.debug("# Transformed Prev DataPoints: " + prevTimeRangeBucketizedDataPoints.length);

                        // this is basically the DTO for the chart
                        $scope.chartData = {
                            id: $scope.chartParams.id,
                            prevStartTimeStamp: previousTimeRange[0],
                            prevEndTimeStamp: previousTimeRange[1],
                            prevDataPoints: prevTimeRangeBucketizedDataPoints,
                            dataPoints: $rootScope.bucketedDataPoints
                            //nvd3DataPoints: formatForNvD3(response),
                            //rickshawDataPoints: formatForRickshaw(response)
                        };

                    } else {
                        $log.warn('No Prev Range Data found for id: ' + $scope.chartParams.searchId);
                        toastr.warn('No Prev Range Data found for id: ' + $scope.chartParams.searchId);
                    }

                }).error(function (response, status) {
                    $log.error('Error loading Prev Range graph data: ' + response);
                    toastr.error('Error loading Prev Range graph data', 'Status: ' + status);
                });
        };

        function formatPreviousBucketizedOutput(response) {
            //  The schema is different for bucketized output
            var mappedNew = $.map(response, function (point, i) {
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
            console.warn("Overlay Data");
            console.dir(mappedNew);
            return mappedNew;
        }

//        function formatForNvD3(dataPoints) {
//
//            // do this for nvd3
//            var nvd3ValuesArray = [];
//            dataPoints.forEach(function (myPoint) {
//                nvd3ValuesArray.push(new Array(myPoint.timestamp, myPoint.value));
//            });
//
//            var nvd3Data = {
//                "key": "Metrics",
//                "values": nvd3ValuesArray
//            };
//
//            return nvd3Data;
//
//        }
//
//        function formatForRickshaw(dataPoints) {
//
//            var rickshawData = $.map(dataPoints, function (point) {
//                return {
//                    "x": point.timestamp,
//                    "y": point.value,
//                };
//            });
//            return rickshawData;
//        }

    }]);
