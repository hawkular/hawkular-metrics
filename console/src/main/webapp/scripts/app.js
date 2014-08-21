'use strict';

angular.module('chartingApp', [ 'ui.bootstrap', 'ngGrid', 'ui.bootstrap.datetimepicker', 'ui.router','ngStorage', 'rhqm.directives', 'rhqm.services'])
    .constant('BASE_URL', '/rhq-metrics/metrics')
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

angular.module('rhqm.directives', [ 'ui.bootstrap', 'd3' ]);
angular.module('rhqm.services', [ 'ngStorage' ]);
