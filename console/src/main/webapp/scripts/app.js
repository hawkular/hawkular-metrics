'use strict';

angular.module('chartingApp', [ 'ui.bootstrap', 'ui.bootstrap.datetimepicker','rhqm.directives', 'rhqm.services'])
    .constant('BASE_URL', 'http://localhost:8080/rhq-metrics/metrics')
    .constant('DATE_TIME_FORMAT', 'MM/DD/YYYY h:mm a')
    .config(function ($httpProvider) {
        // enable CORS
        $httpProvider.defaults.useXDomain = true;
        // just a good security precaution to delete this
        delete $httpProvider.defaults.headers.common['X-Requested-With'];
    });

angular.module('rhqm.directives', [ 'ui.bootstrap' ]);
angular.module('rhqm.services', [  ]);
