'use strict';


/**
 * @ngdoc controller
 * @name MetricOverlayController
 * @description This controller is
 * @param $log
 * @param metricDataService
 */
function MetricOverlayController($q, $log, metricDataService) {
    var vm = this,
        data = [];

    vm.multiChart = {
        newMetric: '',
        combinedData: []
    };


    vm.metricList = [];

    vm.metricList.push("100");
    vm.metricList.push("200");
    vm.metricList.push("apache.server2");

    vm.gridOptions = {data: 'mc.metricList', headerRowHeight: 0 };

    vm.deleteMetric = function (i) {
        vm.metricList.splice(i, 1);
    };

    vm.addMetric = function () {
        if (_.contains(vm.metriclist, vm.multiChart.newMetric)) {
            vm.metricList.push(vm.multiChart.newMetric);
            vm.multiChart.newMetric = '';
            queryMetrics(vm.metricList);
        }
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
                console.debug('Added %s', metricItem);
            }, function (error) {
                console.log(error);
                toastr.error('Error loading Context graph data: ' + error);
            });
            promises.push(promise);
        });

        all = $q.all(promises);
        all.then(function (data) {
            $log.debug("Finished querying all metrics");
            console.dir(data);
            vm.multiChart.combinedData = data;
            console.timeEnd('multiMetrics');
        });

    }

}

angular.module('chartingApp')
    .controller('MetricOverlayController', ['$q', '$log', 'metricDataService', MetricOverlayController]);
