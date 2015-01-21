/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
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
/**
 * Created by mtho11 on 14/11/14.
 */

/// <reference path="../../vendor/vendor.d.ts" />

/// Copyright 2014 Red Hat, Inc.
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
