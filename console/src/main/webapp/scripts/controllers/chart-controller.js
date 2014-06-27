'use strict';


/**
 * @ngdoc controller
 * @name ChartController
 * @param {expression} chartController
 * @description This controller is responsible for handling activity related to the Chart tab.
 */
angular.module('chartingApp')
    .controller('ChartController', ['$scope', '$rootScope', '$http', '$interval', '$log', 'BASE_URL', function ($scope, $rootScope, $http, $interval, $log, BASE_URL) {
        var updateLastTimeStampToNowPromise;

        $scope.chartParams = {
            searchId: "",
            startTimeStamp: moment().subtract('hours', 8).toDate(), //default time period set to 8 hours
            endTimeStamp: new Date(),
            dateRange: moment().subtract('hours', 8).from(moment(),true),
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

            $log.info("Retrieving metrics data for id: " + $scope.chartParams.searchId);
            $log.info("Date Range: " + $scope.chartParams.startTimeStamp + " - " + $scope.chartParams.endTimeStamp);

            $http.get(BASE_URL + '/' + $scope.chartParams.searchId,
                {
                    params: {
                        start: moment($scope.chartParams.startTimeStamp).valueOf(),
                        end: moment($scope.chartParams.endTimeStamp).valueOf(),
                        buckets: 60
                    }
                }
            ).success(function (response) {
                    // we want to isolate the response from the data we are feeding to the chart
                    var bucketizedDataPoints = formatBucketizedOutput(response);

                    if (bucketizedDataPoints.length !== 0) {

                        $log.debug("# Transformed DataPoints: " + bucketizedDataPoints.length);

                        // this is basically the DTO for the chart
                        $scope.chartData = {
                            id: $scope.chartParams.id,
                            startTimeStamp: $scope.chartParams.startTimeStamp,
                            endTimeStamp: $scope.chartParams.endTimeStamp,
                            dataPoints: bucketizedDataPoints
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
