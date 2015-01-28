/*
 * Copyright 2014 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/// <reference path="../../vendor/vendor.d.ts" />

module Controllers {

    'use strict';
    export class SelectRealmController {
        public static $inject = ['$modal', '$log', 'Auth'];
        constructor(private $modal:any, private $log:ng.ILogService, private Auth:Services.AuthService) {
            if (Auth.isAuthenticated()) return;

            var modalInstance = $modal.open({
                templateUrl: 'views/select-realm-modal.html',
                controller: 'SelectRealmModalInstanceController as modalInstance',
                keyboard: false,
                backdrop: 'static'
            });

            modalInstance.result.then(function (selectedRealm) {
                $log.info('Realm to authenticate against: ' + selectedRealm);
            });
        }

    }

    export class SelectRealmModalInstanceController {
        public static $inject = ['$modalInstance', 'Auth', '$http'];
        constructor(private $modalInstance:any, private Auth:Services.AuthService, private $http:ng.IHttpService) {
            this.$http.get('realms.json').success((results) => {
                this.realms = results["realms"]
            });
        }

        realms = [];


        selected = {
            realm: this.realms[0]
        };

        ok = function () {
            this.Auth.realm(this.selected.realm);
            this.$modalInstance.close(this.selected.realm);
        }

    }

    angular.module('chartingApp').controller('SelectRealmController', SelectRealmController);
    angular.module('chartingApp').controller('SelectRealmModalInstanceController', SelectRealmModalInstanceController);
}