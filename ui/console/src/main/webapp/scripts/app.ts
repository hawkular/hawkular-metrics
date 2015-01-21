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
/// <reference path="../vendor/vendor.d.ts" />

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

'use strict';

angular.module('chartingApp', ['ui.bootstrap', 'ngStorage', 'ui.sortable', 'rhqm.directives', 'rhqm.services', 'rhqmCharts','cgBusy', 'ngClipboard','angularytics'])
    .constant('BASE_URL', '/rhq-metrics')
    .constant('TENANT_ID', 'test')
    .constant('DATE_TIME_FORMAT', 'MM/DD/YYYY h:mm a')
    .config(['$httpProvider', ($httpProvider) => {
        // enable CORS
        $httpProvider.defaults.useXDomain = true;
        // just a good security precaution to delete this
        delete $httpProvider.defaults.headers.common['X-Requested-With'];
    }]).config(['AngularyticsProvider',
    function(AngularyticsProvider) {
        AngularyticsProvider.setEventHandlers(
            ['GoogleUniversal', 'Console']);
    }]).run(['Angularytics', function(Angularytics) {
    Angularytics.init();
}]).run(($rootScope, $localStorage, $location, $interval) => {

        //NOTE: if we are then use port 8080 not 9000 that livereload server uses
        $rootScope.$storage = $localStorage.$default({
            server: $location.host(),
            port: ($location.host() === '127.0.0.1') ? '8080' : $location.port()
        });


        // due to timing issues we need to pause for a few seconds to allow the app to start
        var startIntervalPromise = $interval(() => {
            $rootScope.$broadcast('LoadAllSidebarMetricsEvent');
            $rootScope.$emit('LoadAllSidebarMetricsEvent');
            $interval.cancel(startIntervalPromise);
        }, 1000);

    });

angular.module('rhqm.directives', ['ui.bootstrap']);
angular.module('rhqm.services', ['ngStorage']);
