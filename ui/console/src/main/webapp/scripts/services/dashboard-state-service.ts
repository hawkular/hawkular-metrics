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

module Services {
    'use strict';

    export interface IDashboardStateService {
        add(metricId:string):void;
        remove(metricId:string):void;
        getSelectedMetrics():string[];
        getSelectedMetricsAsString():string;
    }

    export class DashboardStateService implements  IDashboardStateService {

        public static  $inject = ['$q', '$rootScope' ];

        public selectedMetricIds : string[] = [];

        constructor(private $q:ng.IQService, private $rootScope:ng.IRootScopeService) {

        }

        clear():void {
            this.selectedMetricIds = [];
        }

        setSelectedMetricIds(newSelectedMetricsIds:string[]):void{
            this.selectedMetricIds = newSelectedMetricsIds;
        }

        add(metricId:string):void{
            if(!_.contains(this.selectedMetricIds, metricId)){
                this.selectedMetricIds.push(metricId);
            }
        }

        remove(metricId:string):void {
            var pos = _.indexOf(this.selectedMetricIds, metricId);
            this.selectedMetricIds.splice(pos, 1);
        }


        getSelectedMetrics():string[] {
            return this.selectedMetricIds;
        }

        getSelectedMetricsAsString():string {
            return this.selectedMetricIds.join(',');
        }

    }


    angular.module('rhqm.services')
        .service('dashboardStateService', DashboardStateService);
}

