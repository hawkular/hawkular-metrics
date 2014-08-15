'use strict';

/**
 * @ngdoc controller
 * @name ConfigController
 * @description This controller allows the configuration settings dialog to open up.
 * @param $scope
 * @param $modal
 * @param $log
 */
var ConfigController = function ($scope, $modal, $log) {

    $scope.openConfig = function (size) {

        var configModalInstance = $modal.open({
            templateUrl: 'configModal.inline',
            controller: ConfigInstanceController,
            size: size
        });

        configModalInstance.result.then(function () {
        }, function () {
            //$log.info('Config Modal dismissed at: ' + new Date());
        });
    };
};

/**
 *
 * @ngdoc controller
 * @name ConfigInstanceController
 * @description This controller controls the configurationetup modal dialog instance
 * @param $scope
 * @param $modalInstance
 * @param $rootScope
 * @param $localStorage
 */
var ConfigInstanceController = function ($scope, $modalInstance, $rootScope, $localStorage) {
    // NOTE: the $rootScope.$storage.server is setup in app.js run module;
    // $rootScope is needed here because it is accessible to run module as controller scopes are not
    $scope.ok = function () {
        $localStorage.server = $rootScope.$storage.server;
        $localStorage.port = $rootScope.$storage.port;
        $modalInstance.close('ok');
    };

    $scope.cancel = function () {
        $modalInstance.dismiss('cancel');
    };
};