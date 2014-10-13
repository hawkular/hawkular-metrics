/// <reference path="../../vendor/vendor.d.ts" />
var Controllers;
(function (Controllers) {
    'use strict';

    var SidebarController = (function () {
        function SidebarController($scope, $rootScope, $log, metricDataService) {
            this.$scope = $scope;
            this.$rootScope = $rootScope;
            this.$log = $log;
            this.metricDataService = metricDataService;
            $scope.vm = this;

            $scope.$on('RemoveSelectedMetricEvent', function (event, metricId) {
                console.debug('RemoveSelectedMeticEvent for: ' + metricId);
                if (_.contains($scope.vm.allMetrics, metricId)) {
                    var pos = _.indexOf($scope.vm.allMetrics, metricId);
                    $scope.vm.allMetrics.splice(pos, 1);
                }
            });
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

        SidebarController.prototype.addRemoveChart = function (metricId, checked) {
            console.log('show chart for: ' + metricId + ', checked: ' + checked);
            if (checked) {
                this.$rootScope.$emit('RemoveChartEvent', metricId);
            } else {
                this.$rootScope.$emit('NewChartEvent', metricId);
            }
        };
        SidebarController.$inject = ['$scope', '$rootScope', '$log', 'metricDataService'];
        return SidebarController;
    })();
    Controllers.SidebarController = SidebarController;

    angular.module('chartingApp').controller('SidebarController', SidebarController);
})(Controllers || (Controllers = {}));
//# sourceMappingURL=sidebar-controller.js.map
