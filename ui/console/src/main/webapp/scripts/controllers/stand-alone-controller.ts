/**
 * Created by mtho11 on 14/11/14.
 */

/// <reference path="../../vendor/vendor.d.ts" />


module Controllers {
    'use strict';

    /**
     * This is just a dummy controller for use with the sample.html standalone chart tag to use.
     * We just need a controller; nothing else, for the standalone chart tag to live in
     */
    export class StandAloneController {
        public static  $inject = ['$scope'];

        constructor(private $scope:ng.IScope) {
            $scope.vm = this;

        }
    }

    angular.module('chartingApp')
        .controller('StandAloneController', StandAloneController);
}
