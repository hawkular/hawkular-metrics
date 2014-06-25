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

        $scope.restParams = {
            searchId: "",
            startTimeStamp: moment().subtract('hours', 8).toDate(), //default time period set to 8 hours
            endTimeStamp: new Date(),
            updateEndTimeStampToNow: false,
            collapseTable: true,
            tableButtonLabel: "Show Table"
        };

        $rootScope.$on("DateRangeChanged", function (event, message) {
            $log.debug("DateRangeChanged Fired from Chart!");
        });

//        $rootScope.$on("DateRangeMove", function (event, message) {
//            $log.debug("DateRangeMove on chart Detected.");
//        });


        $scope.updateEndTimeStampToNow = function () {
            $scope.restParams.updateEndTimeStampToNow = !$scope.restParams.updateEndTimeStampToNow;
            if ($scope.restParams.updateEndTimeStampToNow) {
                updateLastTimeStampToNowPromise = $interval(function () {
                    $scope.restParams.endTimeStamp = new Date();
                }, 30 * 1000);

            } else {
                $interval.cancel(updateLastTimeStampToNowPromise);
            }

        };

        $scope.toggleTable = function () {
            $scope.restParams.collapseTable = !$scope.restParams.collapseTable;
            if ($scope.restParams.collapseTable) {
                $scope.restParams.tableButtonLabel = "Show Table";
            } else {
                $scope.restParams.tableButtonLabel = "Hide Table";
            }
        };

        $scope.$on('$destroy', function () {
            $interval.cancel(updateLastTimeStampToNowPromise);
        });

        $scope.refreshChartData = function () {

            $log.info("Retrieving metrics data for id: " + $scope.restParams.searchId);
            $log.info("Date Range: " + $scope.restParams.startTimeStamp + " - " + $scope.restParams.endTimeStamp);

            $http.get(BASE_URL + '/' + $scope.restParams.searchId,
                {
                    params: {
                        start: moment($scope.restParams.startTimeStamp).valueOf(),
                        end: moment($scope.restParams.endTimeStamp).valueOf(),
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
                            id: $scope.restParams.id,
                            startTimeStamp: $scope.restParams.startTimeStamp,
                            endTimeStamp: $scope.restParams.endTimeStamp,
                            dataPoints: bucketizedDataPoints
                            //nvd3DataPoints: formatForNvD3(response),
                            //rickshawDataPoints: formatForRickshaw(response)
                        };

                    } else {
                        $log.warn('No Data found for id: ' + $scope.restParams.searchId);
                        toastr.warn('No Data found for id: ' + $scope.restParams.searchId);
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
