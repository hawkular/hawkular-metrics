/// <reference path="../../vendor/vendor.d.ts" />


module Controllers {
    'use strict';


    export interface ISelectedMetric {
        title: string;
        href: string;
        rel:string;
        selected: boolean;
    }


    export class SidebarController {
        public static  $inject = ['$scope', '$rootScope', '$log', 'metricDataService' ];

        allMetrics:ISelectedMetric[];
        retrieveMetricsPromise;

        constructor(private $scope:ng.IScope, private $rootScope:ng.IRootScopeService, private $log:ng.ILogService, private metricDataService) {
            $scope.vm = this;

            $scope.$on('RemoveSelectedMetricEvent', (event, metricId) => {
                angular.forEach(this.allMetrics, function (value:ISelectedMetric) {
                    if (value.title === metricId) {
                         value.selected = false;
                    }
                });
            });
            $scope.$on('LoadAllSidebarMetricsEvent', () => {
                this.populateMetricsSidebar();
                this.$rootScope.$emit('LoadInitialChartGroup');
            });


            $scope.$on('SelectedMetricsChangedEvent', (event, selectedMetrics:string[]) => {
                this.selectedMetricsChanged(selectedMetrics);

            });

        }

        private selectedMetricsChanged(selectedMetrics:string[]) {
            _.each(this.allMetrics, function(metric:ISelectedMetric){
                console.log('Metric ' + metric.title);
                _.each(selectedMetrics, function(selectedMetric:string){
                    if(selectedMetric === metric.title){
                       console.info("Selected Checked Metric: " + selectedMetric);
                        metric.selected = true;
                    }
                });
            });
        }

        populateMetricsSidebar() {

            this.retrieveMetricsPromise = this.metricDataService.getAllMetrics()
                .then((response) => {
                    this.allMetrics = response;
                    this.$rootScope.$emit('SidebarRefreshedEvent');

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
