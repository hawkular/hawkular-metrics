'use strict';


/**
 * @ngdoc controller
 * @name MetricOverlayController
 * @description This controller is
 * @param $log
 * @param metricDataService
 */
function MetricOverlayController($q, $log, $rootScope, metricDataService) {
    var vm = this,
        data = [];

    vm.multiChart = {
        newMetric: ''
    };

    vm.metricList = [];

    vm.gridOptions = {data: 'mc.metricList', headerRowHeight: 0 };

    vm.deleteMetric = function (i) {
        vm.metricList.splice(i, 1);
    };

    vm.addMetric = function () {
        var metrics = vm.multiChart.newMetric.split(',');
        vm.metricList = [];
        _.each(metrics, function (metric) {
            vm.metricList.push(metric);
        });
        console.dir(vm.metricList);
        queryMetrics(vm.metricList);
    };

    function queryMetrics(metricList) {
        var promise,
            all,
            promises = [];

        console.time('multiMetrics');
        angular.forEach(metricList, function (metricItem) {
            //@todo: remove fixed 8 hr period
            promise = metricDataService.getMetricsForTimeRange(metricItem, moment().subtract('hours', 8).toDate(), moment().toDate());
            promise.then(function (successData) {
                console.dir(successData);
                data.push(successData);
            }, function (error) {
                console.log(error);
                toastr.error('Error loading Context graph data: ' + error);
            });
            promises.push(promise);
        });

        all = $q.all(promises);
        all.then(function (data) {
            $log.debug('Finished querying all metrics');
            $rootScope.$broadcast('MultiChartOverlayDataChanged', data);
            console.timeEnd('multiMetrics');
        });

    }

}

angular.module('chartingApp')
    .controller('MetricOverlayController', ['$q', '$log', '$rootScope', 'metricDataService', MetricOverlayController]);
