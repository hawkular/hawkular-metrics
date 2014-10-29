/// <reference path="../../vendor/vendor.d.ts" />


module Controllers {
    'use strict';

    export class SidebarController {
        public static  $inject = ['$scope', '$rootScope', '$log', 'metricDataService' ];

        allMetrics;

        constructor(private $scope:ng.IScope, private $rootScope:ng.IRootScopeService, private $log:ng.ILogService, private metricDataService) {
            $scope.vm = this;

            $scope.$on('RemoveSelectedMetricEvent', (event, metricId) => {
                angular.forEach(this.allMetrics, function (value) {
                    if (value.title === metricId) {
                         value.selected = false;
                    }
                });
            });

        }

        populateMetricsSidebar() {

            this.metricDataService.getAllMetrics()
                .then((response) => {
                    this.allMetrics = response;
                    this.$rootScope.$emit('RefreshSidebarEvent');

                }, (error) => {
                    this.$log.error('Error Retrieving all metrics: ' + error);
                    toastr.error('Error Retrieving all metrics: ' + error);
                });

        }

        addRemoveChart(metricId:string, checked:boolean) {
            if (checked) {
                this.$rootScope.$emit('RemoveChartEvent', metricId);
            } else {
                this.$rootScope.$emit('NewChartEvent', metricId);
            }
        }

    }

    angular.module('chartingApp')
        .controller('SidebarController', SidebarController);
}
