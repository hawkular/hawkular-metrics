/// <reference path="../../vendor/vendor.d.ts" />

'use strict';


module Controllers {

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
