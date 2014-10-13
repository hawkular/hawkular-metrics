/// <reference path="../../vendor/vendor.d.ts" />
var Controllers;
(function (Controllers) {
    'use strict';

    var SidebarController = (function () {
        function SidebarController($scope, $log, metricDataService) {
            this.$scope = $scope;
            this.$log = $log;
            this.metricDataService = metricDataService;
            $scope.vm = this;
        }
        SidebarController.prototype.populateMetricsSidebar = function () {
            var that = this;

            this.metricDataService.getAllMetrics().then(function (response) {
                that.allMetrics = response;
            }, function (error) {
                this.$log.error('Error Retrieving all metrics: ' + error);
                toastr.error('Error Retrieving all metrics: ' + error);
            });
        };

        SidebarController.prototype.showChart = function (metricId) {
            console.log('show chart for: ' + metricId);
        };
        SidebarController.$inject = ['$scope', '$log', 'metricDataService'];
        return SidebarController;
    })();
    Controllers.SidebarController = SidebarController;

    angular.module('chartingApp').controller('SidebarController', SidebarController);
})(Controllers || (Controllers = {}));
//# sourceMappingURL=sidebar-controller.js.map
