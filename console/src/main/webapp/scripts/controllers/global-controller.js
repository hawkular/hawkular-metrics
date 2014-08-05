
'use strict';


/**
 * @ngdoc controller
 * @name GlobalController
 * @description This controller is responsible for changing servers to connect to.
 */
angular.module('chartingApp')
    .controller('GlobalController', ['$log','$rootScope', '$localStorage', function ($log, $rootScope, $localStorage ) {
        var vm = this;
        // NOTE: the $rootScope.$storage.server is setup in app.js run module;
        // $rootScope is needed here because it is accessible to run module as controller scopes are not

//        vm.$storage = $localStorage.$default({
//            server: 'localhost',
//            port: '8080'
//        });

        vm.saveServerPort = function(){
            $localStorage.server = $rootScope.$storage.server;
            $localStorage.port = $rootScope.$storage.port;

        };
    }
]);
