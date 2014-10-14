/// <reference path="../../vendor/vendor.d.ts" />


module Controllers {
    'use strict';

    export class SidebarController {
        public static  $inject = ['$scope', '$rootScope', '$log', 'metricDataService' ];

        allMetrics;

        constructor(private $scope:ng.IScope,private $rootScope:ng.IRootScopeService,  private $log:ng.ILogService, private metricDataService) {
            $scope.vm = this;

            $scope.$on('RemoveSelectedMetricEvent', function(event, metricId){
                console.debug('RemoveSelectedMeticEvent for: '+metricId);
                if(_.contains($scope.vm.allMetrics, metricId)){
                    var pos = _.indexOf($scope.vm.allMetrics, metricId);
                    $scope.vm.allMetrics.splice(pos, 1);
                }
            });

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

        addRemoveChart(metricId: string, checked: boolean) {
            console.log('show chart for: '+ metricId + ', checked: '+checked);
            if(checked){
                this.$rootScope.$emit('RemoveChartEvent', metricId);
            } else {
                this.$rootScope.$emit('NewChartEvent', metricId);
            }
        }

    }

    angular.module('chartingApp')
        .controller('SidebarController', SidebarController);
}
