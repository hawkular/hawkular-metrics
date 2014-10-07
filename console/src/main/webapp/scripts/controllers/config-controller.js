/// <reference path="../../vendor/vendor.d.ts" />
var Controllers;
(function (Controllers) {
    'use strict';

    /**
    * @ngdoc controller
    * @name ConfigController
    * @description This controller allows the configuration settings dialog to open up.
    * @param $scope
    * @param $modal
    * @param $log
    */
    var ConfigController = (function () {
        function ConfigController($scope, $modal, $log) {
            this.$scope = $scope;
            this.$modal = $modal;
            this.$log = $log;
            $scope.vm = this;
        }
        ConfigController.prototype.openConfig = function (size) {
            var configModalInstance = this.$modal.open({
                templateUrl: 'configModal.inline',
                controller: ConfigInstanceController,
                size: size
            });

            configModalInstance.result.then(function () {
            }, function () {
                //$log.info('Config Modal dismissed at: ' + new Date());
            });
        };
        ConfigController.$inject = ['$scope', '$modal', '$log'];
        return ConfigController;
    })();
    Controllers.ConfigController = ConfigController;

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
    var ConfigInstanceController = (function () {
        function ConfigInstanceController($scope, $modalInstance, $rootScope, $localStorage) {
            this.$scope = $scope;
            this.$modalInstance = $modalInstance;
            this.$rootScope = $rootScope;
            this.$localStorage = $localStorage;
            $scope.vm = this;
        }
        // NOTE: the $rootScope.$storage.server is setup in app.js run module;
        // $rootScope is needed here because it is accessible to run module as controller scopes are not
        ConfigInstanceController.prototype.ok = function () {
            this.$localStorage.server = this.$rootScope.$storage.server;
            this.$localStorage.port = this.$rootScope.$storage.port;
            this.$modalInstance.close('ok');
        };

        ConfigInstanceController.prototype.cancel = function () {
            this.$modalInstance.dismiss('cancel');
        };
        ConfigInstanceController.$inject = ['$scope', '$modalInstance', '$rootScope', '$localStorage'];
        return ConfigInstanceController;
    })();
    Controllers.ConfigInstanceController = ConfigInstanceController;
    angular.module('chartingApp').controller('ConfigController', ConfigController);
    angular.module('chartingApp').controller('ConfigInstanceController', ConfigInstanceController);
})(Controllers || (Controllers = {}));
//# sourceMappingURL=config-controller.js.map
