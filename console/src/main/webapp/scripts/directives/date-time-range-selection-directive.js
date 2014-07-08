'use strict';

angular.module('rhqm.directives')
    .directive('relativeTimeRangeButtonBar', function () {
        return {
            templateUrl: '../views/directives/date-time-range-selection.tpl.html',
            controller: function ($scope) {

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

                $scope.dateTimeRangeButtonBarModel = {
                    graphTimeRangeSelection:  '8h'// also sets the default range value
                };


                $scope.startDateTime = moment().subtract('hours', 8).toDate();
                $scope.endDateTime = new Date();


                $scope.handleGraphTimeRangeSelection = function () {
                    console.debug("Button click: " + $scope.dateTimeRangeButtonBarModel.graphTimeRangeSelection);
                    $scope.endTimeStamp = new Date();
                    $scope.startTimeStamp = moment().subtract('seconds', $scope.dateTimeRangeButtonBarModel.graphTimeRangeSelection).toDate();
                };

//                $scope.$watch('dateTimeRangeButtonBarModel.graphTimeRangeSelection', function (newValue, oldValue) {
//                    var startDateMoment,
//                        endDateMoment;
//                    $scope.$emit('graphTimeRangeChangedEvent', newValue, oldValue);
//                    $scope.endDateTime = new Date();
//                    endDateMoment = moment();
//                    for (var i = 0; i < $scope.dateTimeRanges.length; i++) {
//                        var dateTimeRange = $scope.dateTimeRanges[i];
//                        if (dateTimeRange.range === $scope.dateTimeRangeButtonBarModel.graphTimeRangeSelection) {
//                            console.debug('dateTimeRangeSelection in seconds: '+ dateTimeRange.rangeInSeconds);
//                            startDateMoment = endDateMoment.subtract('seconds', dateTimeRange.rangeInSeconds);
//                            break;
//                        }
//                    }
//                    $scope.startDateTime = startDateMoment.toDate();
//                });

            },
            replace: true,
            restrict: 'EA',
            scope: {
                startTimeStamp: '=',
                endTimeStamp: '='
            }
        };
    });
