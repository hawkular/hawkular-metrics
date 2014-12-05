/// <reference path="../vendor/vendor.d.ts" />

'use strict';

angular.module('chartingApp', ['ui.bootstrap', 'ngStorage', 'ui.sortable', 'rhqm.directives', 'rhqm.services', 'rhqmCharts','cgBusy', 'ngClipboard'])
    .constant('BASE_URL', '/rhq-metrics')
    .constant('TENANT_ID', 'test')
    .constant('DATE_TIME_FORMAT', 'MM/DD/YYYY h:mm a')
    .config(['$httpProvider', ($httpProvider) => {
        // enable CORS
        $httpProvider.defaults.useXDomain = true;
        // just a good security precaution to delete this
        delete $httpProvider.defaults.headers.common['X-Requested-With'];
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
