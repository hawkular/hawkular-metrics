///
/// Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
/// and other contributors as indicated by the @author tags.
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
///

/// <reference path="../vendor/vendor.d.ts" />
'use strict';

angular.module('chartingApp', [ 'ui.bootstrap',  'ui.bootstrap.datetimepicker', 'ui.router','ngStorage', 'rhqm.directives','hawkularCharts', 'rhqm.services'])
    .constant('BASE_URL', '/rhq-metrics')
    .constant('TENANT_ID', 'test')
    .constant('DATE_TIME_FORMAT', 'MM/DD/YYYY h:mm a')
    .config(['$stateProvider', '$urlRouterProvider',function ($stateProvider, $urlRouterProvider) {
        $urlRouterProvider.otherwise('/');

        $stateProvider
            .state('insert', {
                url: '/insert',
                templateUrl: 'views/metrics.html',
                controller: 'InsertMetricsController'

            })
            .state('chart', {
                url: '/chart',
                templateUrl: 'views/metrics.html',
                controller: 'ChartController'
            });

    }])
    .config(['$httpProvider',function ($httpProvider) {
        // enable CORS
        $httpProvider.defaults.useXDomain = true;
        // just a good security precaution to delete this
        delete $httpProvider.defaults.headers.common['X-Requested-With'];
    }]).run(function ($rootScope, $localStorage) {

        $rootScope.$storage = $localStorage.$default({
            server: 'localhost',
            port: '8080'
        });
    });

angular.module('rhqm.directives', [ 'ui.bootstrap' ]);
angular.module('rhqm.services', [ 'ngStorage' ]);
