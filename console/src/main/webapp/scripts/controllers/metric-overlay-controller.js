/// <reference path="../../vendor/vendor.d.ts" />
'use strict';
var Controllers;
(function (Controllers) {
    /**
    * @ngdoc controller
    * @name MetricOverlayController
    * @description This controller is
    * @param $log
    * @param metricDataService
    */
    var MetricOverlayController = (function () {
        function MetricOverlayController($scope, $q, $log, $rootScope, metricDataService) {
            this.$scope = $scope;
            this.$q = $q;
            this.$log = $log;
            this.$rootScope = $rootScope;
            this.metricDataService = metricDataService;
            this.data = [];
            this.multiChart = {
                newMetric: ''
            };
            this.metricList = [];
            this.gridOptions = { data: 'mc.metricList', headerRowHeight: 0 };
            $scope.vm = this;
        }
        MetricOverlayController.prototype.deleteMetric = function (i) {
            this.metricList.splice(i, 1);
        };

        MetricOverlayController.prototype.addMetric = function () {
            var metrics = this.multiChart.newMetric.split(',');
            this.metricList = [];
            _.each(metrics, function (metric) {
                this.metricList.push(metric);
            });
            console.dir(this.metricList);
            this.queryMetrics(this.metricList);
        };

        MetricOverlayController.prototype.queryMetrics = function (metricList) {
            var promise, all, promises = [];

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
        };
        MetricOverlayController.$inject = ['$scope', '$q', '$log', '$rootScope', 'metricDataService'];
        return MetricOverlayController;
    })();
    Controllers.MetricOverlayController = MetricOverlayController;

    angular.module('chartingApp').controller('MetricOverlayController', MetricOverlayController);
})(Controllers || (Controllers = {}));
//# sourceMappingURL=metric-overlay-controller.js.map
