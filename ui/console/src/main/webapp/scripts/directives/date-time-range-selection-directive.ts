/// <reference path="../../vendor/vendor.d.ts" />

module Directives {
    'use strict';

    angular.module('rhqm.directives')
        .directive('relativeTimeRangeButtonBar', function ():ng.IDirective {
            return {
                templateUrl: '../views/directives/date-time-range-selection.tpl.html',
                controller: ($scope) => {

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
                        graphTimeRangeSelection: '1d'// also sets the default range value
                    };


                    $scope.$watch('dateTimeRangeButtonBarModel.graphTimeRangeSelection', (newValue, oldValue) =>{
                        var startDateMoment,
                            endDateMoment,
                            startEndArray = [];
                        endDateMoment = moment();
                        for (var i = 0; i < $scope.dateTimeRanges.length; i++) {
                            var dateTimeRange = $scope.dateTimeRanges[i];
                            if (dateTimeRange.range === $scope.dateTimeRangeButtonBarModel.graphTimeRangeSelection) {
                                startDateMoment = endDateMoment.subtract('seconds', dateTimeRange.rangeInSeconds);
                                break;
                            }
                        }

                        startEndArray.push(startDateMoment.toDate());
                        startEndArray.push(new Date());
                        $scope.$emit('GraphTimeRangeChangedEvent', startEndArray);
                    });

                },
                replace: true,
                restrict: 'EA',
                scope: {
                    startTimeStamp: '=',
                    endTimeStamp: '='
                }
            };
        });
}
