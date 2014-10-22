/// <reference path="../vendor/vendor.d.ts" />

'use strict';

angular.module('chartingApp', [ 'ui.bootstrap',  'ui.bootstrap.datetimepicker', 'ui.router','ngStorage', 'rhqm.directives', 'rhqm.services'])
    .constant('BASE_URL', '/rhq-metrics/metrics')
    .constant('MAX_SEARCH_ENTRIES', 50)
    .constant('DATE_TIME_FORMAT', 'MM/DD/YYYY h:mm a')
    .config(['$httpProvider',function ($httpProvider) {
        // enable CORS
        $httpProvider.defaults.useXDomain = true;
        // just a good security precaution to delete this
        delete $httpProvider.defaults.headers.common['X-Requested-With'];
    }]).run(function ($rootScope, $localStorage, $location) {

        //NOTE: if we are then use port 8080 not 9000 that livereload server uses
        $rootScope.$storage = $localStorage.$default({
            server: $location.host(),
            port: ($location.host() === '127.0.0.1') ? '8080' : $location.port()
        });
    });

angular.module('rhqm.directives', [ 'ui.bootstrap'  ]);
angular.module('rhqm.services', [ 'ngStorage' ]);
