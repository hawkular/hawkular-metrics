/**
* Created by mtho11 on 14/11/14.
*/
/// <reference path="../../vendor/vendor.d.ts" />
var Controllers;
(function (Controllers) {
    'use strict';

    /**
    * This is just a dummy controller for use with the sample.html standalone chart tag to use.
    * We just need a controller; nothing else, for the standalone chart tag to live in
    */
    var StandAloneController = (function () {
        function StandAloneController($scope) {
            this.$scope = $scope;
            $scope.vm = this;
        }
        StandAloneController.$inject = ['$scope'];
        return StandAloneController;
    })();
    Controllers.StandAloneController = StandAloneController;

    angular.module('chartingApp').controller('StandAloneController', StandAloneController);
})(Controllers || (Controllers = {}));
//# sourceMappingURL=stand-alone-controller.js.map
