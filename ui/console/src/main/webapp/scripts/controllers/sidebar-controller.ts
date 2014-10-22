/// <reference path="../../vendor/vendor.d.ts" />


module Controllers {
    'use strict';

    export class SidebarController {
        public static  $inject = ['MAX_SEARCH_ENTRIES', '$scope', '$rootScope', '$log', 'metricDataService' ];

        allMetrics;

        constructor(private MAX_SEARCH_ENTRIES: number, private $scope:ng.IScope, private $rootScope:ng.IRootScopeService, private $log:ng.ILogService, private metricDataService) {
            $scope.vm = this;

            $scope.$on('RemoveSelectedMetricEvent', function (event, metricId) {
                angular.forEach($scope.vm.allMetrics, function (value) {
                    if (value.title === metricId) {
                         value.selected = false;
                    }
                });
            });

        }

        populateMetricsSidebar() {
            var that = this;

            this.metricDataService.getAllMetrics()
                .then(function (response) {
                    that.allMetrics = response;
                    if(response.length >= this.MAX_SEARCH_ENTRIES){
                        toastr.info('More than 50 entries please use filter to narrow results');
                    }

                }, function (error) {
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
