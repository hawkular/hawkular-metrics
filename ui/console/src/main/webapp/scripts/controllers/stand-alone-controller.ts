/**
 * Created by mtho11 on 14/11/14.
 */

/// <reference path="../../vendor/vendor.d.ts" />


module Controllers {
    'use strict';

    export class StandAloneController {
        public static  $inject = ['$scope', '$rootScope', '$log'];

        constructor(private $scope:ng.IScope, private $rootScope:ng.IRootScopeService, private $log:ng.ILogService ) {
            $scope.vm = this;

        }

        doSomething(){

        }


    }

    angular.module('chartingApp')
        .controller('StandAloneController', StandAloneController);
}
