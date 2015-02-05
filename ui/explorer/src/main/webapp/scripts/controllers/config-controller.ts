///
/// Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
/// and other contributors as indicated by the @author tags.
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///    http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///

/// <reference path="../../vendor/vendor.d.ts" />
module Controllers {
    'use strict';

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
