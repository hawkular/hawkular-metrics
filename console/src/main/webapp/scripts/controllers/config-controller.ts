/// <reference path="../../vendor/vendor.d.ts" />

'use strict';

module Controllers {

    /**
     * @ngdoc controller
     * @name ConfigController
     * @description This controller allows the configuration settings dialog to open up.
     * @param $scope
     * @param $modal
     * @param $log
     */
    export class ConfigController {
        public static  $inject = ['$scope', '$modal', '$log' ];

        constructor(private $scope:ng.IScope, private $modal, private $log:ng.ILogService) {
            $scope.vm = this;
        }

        openConfig(size):void {

            var configModalInstance = this.$modal.open({
                templateUrl: 'configModal.inline',
                controller: ConfigInstanceController,
                size: size
            });

            configModalInstance.result.then(function () {
            }, function () {
                //$log.info('Config Modal dismissed at: ' + new Date());
            });
        }
    }

    /**
     *
     * @ngdoc controller
     * @name ConfigInstanceController
     * @description This controller controls the configuration setup modal dialog instance
     * @param $scope
     * @param $modalInstance
     * @param $rootScope
     * @param $localStorage
     */
    export class ConfigInstanceController {

        public static  $inject = ['$scope', '$modalInstance', '$rootScope', '$localStorage' ];

        constructor(private $scope, private $modalInstance, private $rootScope, private $localStorage) {
            $scope.vm = this;
        }

        // NOTE: the $rootScope.$storage.server is setup in app.js run module;
        // $rootScope is needed here because it is accessible to run module as controller scopes are not
        ok():void {
            this.$localStorage.server = this.$rootScope.$storage.server;
            this.$localStorage.port = this.$rootScope.$storage.port;
            this.$modalInstance.close('ok');
        }

        cancel():void {
            this.$modalInstance.dismiss('cancel');
        }
    }
    angular.module('chartingApp').controller('ConfigController', ConfigController);
    angular.module('chartingApp').controller('ConfigInstanceController', ConfigInstanceController);
}
