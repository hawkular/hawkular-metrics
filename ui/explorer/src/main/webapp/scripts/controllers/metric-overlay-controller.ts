///
/// Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
/// and other contributors as indicated by the @author tags.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///    http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

/// <reference path="../../vendor/vendor.d.ts" />
module Controllers {
    'use strict';

    /**
     * @ngdoc controller
     * @name MetricOverlayController
     * @description This controller is
     * @param $log
     * @param metricDataService
     */
    export class MetricOverlayController {

        public static  $inject = ['$scope', '$q', '$log', '$rootScope', 'metricDataService' ];

        constructor(private $scope, private $q, private $log, private $rootScope, private metricDataService) {
            $scope.vm = this;
        }

        data = [];
        multiChart = {
            newMetric: ''
        };
        metricList = [];
        gridOptions = {data: 'mc.metricList', headerRowHeight: 0 };

        deleteMetric(i:number):void {
            this.metricList.splice(i, 1);
        }

        addMetric():void {
            var metrics = this.multiChart.newMetric.split(',');
            this.metricList = [];
            _.each(metrics, function (metric) {
                this.metricList.push(metric);
            });
            console.dir(this.metricList);
            this.queryMetrics(this.metricList);
        }

        queryMetrics(metricList):void {
            var promise,
                all,
                promises = [];

            console.time('multiMetrics');
            angular.forEach(metricList, function (metricItem) {
                //@todo: remove fixed 8 hr period
                promise = this.metricDataService.getMetricsForTimeRange(metricItem, moment().subtract('hours', 8).toDate(), moment().toDate());
                promise.then(function (successData) {
                    console.dir(successData);
                    this.data.push(successData);
                }, function (error) {
                    console.log(error);
                    toastr.error('Error loading Context graph data: ' + error);
                });
                promises.push(promise);
            });

            all = this.$q.all(promises);
            all.then(function (data) {
                this.$log.debug('Finished querying all metrics');
                this.$rootScope.$broadcast('MultiChartOverlayDataChanged', data);
                console.timeEnd('multiMetrics');
            });

        }

    }

    angular.module('chartingApp')
        .controller('MetricOverlayController', MetricOverlayController);
}
