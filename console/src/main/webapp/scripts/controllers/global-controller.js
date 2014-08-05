
'use strict';


/**
 * @ngdoc controller
 * @name GlobalController
 * @description This controller is responsible for changing servers to connect to.
 */
angular.module('chartingApp')
    .controller('GlobalController', ['$log', '$localStorage', function ($log, $localStorage ) {
        var vm = this;

        vm.$storage = $localStorage.$default({
            server: 'localhost',
            port: '8080'
        });

        vm.saveServerPort = function(){
            $localStorage.server = vm.$storage.server;
            $localStorage.port = vm.$storage.port;

        };
    }
]);
