'use strict';

var MetricItem = function(name){
    this.name = name;
    this.enabled = true;
    this.color = "#FF5D07";
};


/**
 * @ngdoc controller
 * @name MetricOverlayController
 * @description This controller is
 * @param $scope
 * @param $rootScope
 * @param $interval
 * @param $log
 * @param metricDataService
 */
function MetricOverlayController ($scope, $rootScope, $interval, $log, metricDataService) {
    var  vm = this;

    vm.multiChart = {
        newMetric: ''
    };
    vm.metricList = [];

    vm.metricList.push(new MetricItem("100"));
    vm.metricList.push(new MetricItem("200"));
    vm.metricList.push(new MetricItem("300"));

    vm.toggleEnabled = function(i) {

    };

    vm.deleteMetric = function (i) {
        vm.metricList.splice(i,1);
    };

    vm.addMetric = function () {
        var metricItem = new MetricItem(vm.newMetric);
        vm.metricList.push(metricItem);
        vm.multiChart.newMetric = '';
    };

}

angular.module('chartingApp')
    .controller('MetricOverlayController', ['$scope', '$rootScope', '$interval', '$log', 'metricDataService', MetricOverlayController]);
