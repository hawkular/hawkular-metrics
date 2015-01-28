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

    export class AuthInterceptorService {
        public static $inject = ['$q', 'Auth', 'BASE_URL'];

        public static Factory($q:ng.IQService, Auth:Services.AuthService, BASE_URL:string) {
            return new AuthInterceptorService($q, Auth, BASE_URL);
        }

        constructor(private $q:ng.IQService, private Auth:Services.AuthService, private BASE_URL:string) {
        }

        request = (request) => {
            console.debug('Intercepting request');
            var addBearer, deferred;
            if (request.url.indexOf(this.BASE_URL) === -1) {
                console.debug('The requested URL is not part of the base URL. Base URL: ' + this.BASE_URL + ', requested URL: ' + request.url);
                return request;
            }
            addBearer = () => {
                return this.Auth.updateToken(5).success(() => {
                    var token = this.Auth.token();
                    console.debug('Adding bearer token to the request: ' + token);
                    request.headers.Authorization = 'Bearer ' + token;
                    request.headers['X-RHQ-Realm'] = this.Auth.realm();
                    deferred.notify();
                    return deferred.resolve(request);
                });
            };
            deferred = this.$q.defer();
            if (this.Auth.isAuthenticated()) {
                console.log('User is authenticated, add bearer token');
                addBearer();
            }
            return this.$q.when(deferred.promise);
        };

        responseError = (rejection) => {
            console.debug('Intercepting error response');
            if (rejection.status === 401) {
                toastr.error('Your session has expired. Please, login again.');
                this.Auth.logout();
            }
            return this.$q.reject(rejection);
        };
    }

    angular.module('chartingApp').config(function($httpProvider) {
        console.debug('Adding AuthInterceptor');
        return $httpProvider.interceptors.push(Services.AuthInterceptorService.Factory);
    });
}