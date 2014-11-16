/// <reference path="../../vendor/vendor.d.ts" />
var Controllers;
(function (Controllers) {
    'use strict';

    var SidebarController = (function () {
        function SidebarController($scope, $rootScope, $log, metricDataService) {
            var _this = this;
            this.$scope = $scope;
            this.$rootScope = $rootScope;
            this.$log = $log;
            this.metricDataService = metricDataService;
            $scope.vm = this;

            $scope.$on('RemoveSelectedMetricEvent', function (event, metricId) {
                angular.forEach(_this.allMetrics, function (value) {
                    if (value.title === metricId) {
                        value.selected = false;
                    }
                });
            });
            $scope.$on('LoadAllSidebarMetricsEvent', function () {
                _this.populateMetricsSidebar();
                _this.$rootScope.$emit('LoadInitialChartGroup');
            });

            $scope.$on('SelectedMetricsChangedEvent', function (event, selectedMetrics) {
                _this.selectedMetricsChanged(selectedMetrics);
            });
        }
        SidebarController.prototype.selectedMetricsChanged = function (selectedMetrics) {
            _.each(this.allMetrics, function (metric) {
                console.log('Metric ' + metric.title);
                _.each(selectedMetrics, function (selectedMetric) {
                    if (selectedMetric === metric.title) {
                        console.info("Selected Checked Metric: " + selectedMetric);
                        metric.selected = true;
                    }
                });
            });
        };

        SidebarController.prototype.populateMetricsSidebar = function () {
            var _this = this;
            this.retrieveMetricsPromise = this.metricDataService.getAllMetrics().then(function (response) {
                _this.allMetrics = response;
                _this.$rootScope.$emit('SidebarRefreshedEvent');
            }, function (error) {
                _this.$log.error('Error Retrieving all metrics: ' + error);
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
        SidebarController.$inject = ['$scope', '$rootScope', '$log', 'metricDataService'];
        return SidebarController;
    })();
    Controllers.SidebarController = SidebarController;

    angular.module('chartingApp').controller('SidebarController', SidebarController);
})(Controllers || (Controllers = {}));
//# sourceMappingURL=sidebar-controller.js.map
