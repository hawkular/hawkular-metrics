/// <reference path="../../vendor/vendor.d.ts" />
var Controllers;
(function (Controllers) {
    'use strict';

    var SidebarController = (function () {
        function SidebarController(MAX_SEARCH_ENTRIES, $scope, $rootScope, $log, metricDataService) {
            this.MAX_SEARCH_ENTRIES = MAX_SEARCH_ENTRIES;
            this.$scope = $scope;
            this.$rootScope = $rootScope;
            this.$log = $log;
            this.metricDataService = metricDataService;
            $scope.vm = this;

            $scope.$on('RemoveSelectedMetricEvent', function (event, metricId) {
                angular.forEach($scope.vm.allMetrics, function (value) {
                    if (value.title === metricId) {
                        value.selected = false;
                    }
                });
            });
        }
        SidebarController.prototype.populateMetricsSidebar = function () {
            var that = this;

            this.metricDataService.getAllMetrics().then(function (response) {
                that.allMetrics = response;
                if (response.length >= this.MAX_SEARCH_ENTRIES) {
                    toastr.info('More than 50 entries please use filter to narrow results');
                }
            }, function (error) {
                this.$log.error('Error Retrieving all metrics: ' + error);
                toastr.error('Error Retrieving all metrics: ' + error);
            });
        };

        SidebarController.prototype.addRemoveChart = function (metricId, checked) {
            if (checked) {
                this.$rootScope.$emit('RemoveChartEvent', metricId);
            } else {
                this.$rootScope.$emit('NewChartEvent', metricId);
            }
        };
        SidebarController.$inject = ['MAX_SEARCH_ENTRIES', '$scope', '$rootScope', '$log', 'metricDataService'];
        return SidebarController;
    })();
    Controllers.SidebarController = SidebarController;

    angular.module('chartingApp').controller('SidebarController', SidebarController);
})(Controllers || (Controllers = {}));
//# sourceMappingURL=sidebar-controller.js.map
