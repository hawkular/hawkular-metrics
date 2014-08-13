'use strict';

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