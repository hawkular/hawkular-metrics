/// <reference path="../../vendor/vendor.d.ts" />

module Services {
    'use strict';


    export class MetricDataService {

        public static  $inject = ['$q', '$rootScope', '$http',  'BASE_URL' ];

        constructor(private $q:ng.IQService, private $rootScope:ng.IRootScopeService, private $http:ng.IHttpService,  private BASE_URL:string) {

        }

        private makeBaseUrl():string {
            var baseUrl = 'http://' + this.$rootScope.$storage.server + ':' + this.$rootScope.$storage.port + this.BASE_URL;
            return baseUrl;
        }

        getMetricsForTimeRange(id:string, startDate:Date, endDate:Date, buckets:number):any {
            console.info("-- Retrieving metrics data for id: " + id);
            console.info("-- Date Range: " + startDate + " - " + endDate);
            var numBuckets = buckets || 60,
                base = this.makeBaseUrl(),
                deferred = this.$q.defer(),
                searchParams =
                {
                    params: {
                        start: startDate.getTime(),
                        end: endDate.getTime(),
                        buckets: numBuckets
                    }
                };

            if (startDate >= endDate) {
                console.warn("Start date was after end date");
                deferred.reject("Start date was after end date");
            }

            this.$http.get(base + '/' + id, searchParams).success(function (data) {
                deferred.resolve(data);
            }).error(function (reason, status) {
                console.error('Error Loading Chart Data:' + status + ", " + reason);
                deferred.reject(status + " - " + reason);
            });

            return deferred.promise;
        }

        insertSinglePayload(id, jsonPayload):any {
            var url = this.makeBaseUrl(),
                deferred = this.$q.defer();
            this.$http.post(url + '/' + id, jsonPayload
            ).success(function () {
                    deferred.resolve("Success");
                }).error(function (response, status) {
                    console.error("Error: " + status + " --> " + response);
                    deferred.reject(status);
                });
            return deferred.promise;
        }

        insertMultiplePayload(jsonPayload):any {
            var url = this.makeBaseUrl(),
                deferred = this.$q.defer();
            this.$http.post(url + '/', jsonPayload
            ).success(function () {
                    deferred.resolve("Success");
                }).error(function (response, status) {
                    console.error("Error: " + status + " --> " + response);
                    deferred.reject(status);
                });
            return deferred.promise;
        }
    }


    angular.module('rhqm.services')
        .service('metricDataService', MetricDataService);
}
