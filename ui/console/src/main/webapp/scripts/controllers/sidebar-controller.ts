/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/// <reference path="../../vendor/vendor.d.ts" />

/// Copyright 2014 Red Hat, Inc.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///    http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.

module Controllers {
    'use strict';


    export interface ITenantMetric {
       name: string;
       tenantId: string;
        selected: boolean;
    }


    export class SidebarController {
        public static  $inject = ['$scope', '$rootScope', 'metricDataService','dashboardStateService','Angularytics' ];

        allMetrics: ITenantMetric[];
        retrieveMetricsPromise;

        constructor(private $scope:ng.IScope, private $rootScope:ng.IRootScopeService,  private metricDataService, private dashboardStateService, private Angularytics) {
            $scope.vm = this;

            $scope.$on('RemoveSelectedMetricEvent', (event, metricId) => {
                angular.forEach(this.allMetrics, (value:ITenantMetric) => {
                    if (value.name === metricId) {
                         value.selected = false;
                        return;
                    }
                });
            });

            $scope.$on('LoadAllSidebarMetricsEvent', () => {
                this.Angularytics.trackEvent('LoadAllSidebarMetricsEvent', 'true');
                this.populateMetricsSidebar();
                this.$rootScope.$emit('LoadInitialChartGroup');
            });

            $scope.$on('SelectedMetricsChangedEvent', () => {
                this.selectedMetricsChanged();
            });

        }

        private selectedMetricsChanged() {
            _.each(this.allMetrics, (metric) =>{
                _.each(this.dashboardStateService.getSelectedMetrics(), (selectedMetric:string) => {
                    if(selectedMetric === metric.name){
                        metric.selected = true;
                    }
                });
            });
        }

        populateMetricsSidebar() {
            this.retrieveMetricsPromise = this.metricDataService.getAllMetrics()
                .then((response) => {
                    this.allMetrics = response;
                    this.selectedMetricsChanged();
                    this.$rootScope.$emit('SidebarRefreshedEvent');

                }, (error) => {
                    console.error('Error Retrieving all metrics: ' + error);
                    toastr.error('Error Retrieving all metrics: ' + error);
                });

        }

        addRemoveChart(metricId:string, checked:boolean) {
            if (checked) {
                this.Angularytics.trackEvent('SideBar', 'RemoveChart',metricId);
                this.$rootScope.$emit('RemoveChartEvent', metricId);
            } else {
                this.Angularytics.trackEvent('SideBar', 'AddChart',metricId );
                this.$rootScope.$emit('NewChartEvent', metricId);
            }
        }

    }

    angular.module('chartingApp')
        .controller('SidebarController', SidebarController);
}
