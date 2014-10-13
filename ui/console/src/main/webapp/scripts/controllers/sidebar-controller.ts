/// <reference path="../../vendor/vendor.d.ts" />


module Controllers {
    'use strict';

    export class SidebarController {
        public static  $inject = ['$scope', '$log', 'metricDataService' ];

        allMetrics;

        constructor(private $scope:ng.IScope, private $log:ng.ILogService, private metricDataService) {
            $scope.vm = this;

        }

        populateMetricsSidebar() {
            var that = this;

            this.metricDataService.getAllMetrics()
                .then(function (response) {
                    that.allMetrics = response;

                }, function (error) {
                    this.$log.error('Error Retrieving all metrics: ' + error);
                    toastr.error('Error Retrieving all metrics: ' + error);
                });

        }

        showChart(metricId: string) {
            console.log('show chart for: '+ metricId);
        }
    }

    angular.module('chartingApp')
        .controller('SidebarController', SidebarController);
}
