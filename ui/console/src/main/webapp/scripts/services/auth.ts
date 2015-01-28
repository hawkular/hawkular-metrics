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

module Services {
    'use strict';

    export class AuthService {
        public static $inject = ['$rootScope', '$window'];
        constructor(private $rootScope:ng.IRootScopeService, private $window:ng.IWindowService) {
            var keycloak = $window['keycloak'];
            if (keycloak) {
                if (!keycloak.hasResourceRole('user', 'metrics-console')) {
                    alert('There\'s something wrong with your credentials. Contact support.');
                    keycloak.logout();
                }
                $rootScope['username'] = keycloak.idToken.name;
            }
        }

        private keycloak():any {
            return this.$window['keycloak'];
        }

        realm(realm?:string):string {
            if (realm === undefined) {
                return localStorage['realm'];
            }
            localStorage.setItem('realm', realm);
            window.location.reload();
        }

        logout():void {
            if (!this.keycloak()) return;
            localStorage.removeItem('realm');
            return this.keycloak().logout();
        }

        updateToken(periodicity:number):any {
            if (!this.keycloak()) return;
            return this.keycloak().updateToken(periodicity);
        }

        token():string {
            if (!this.keycloak()) return '';
            return this.keycloak().token;
        }

        isAuthenticated():boolean {
            if (!this.keycloak()) return false;
            return this.keycloak() && this.keycloak().authenticated;
        }
    }

    angular.module('chartingApp').service('Auth', AuthService);
}