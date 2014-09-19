/// <reference path="../../vendor/vendor.d.ts" />
'use strict';

module Services {

    export class MetricDataService {

        public static  $inject = ['$q', '$rootScope', '$http', '$log', '$localStorage', 'BASE_URL' ];

        constructor(private $q:ng.IQService, private $rootScope:ng.IRootScopeService, private $http:ng.IHttpService, private $log:ng.ILogService, public $localStorage:any, private BASE_URL:string) {

        }

        private makeBaseUrl():string {
            var baseUrl = 'http://' + this.$rootScope.$storage.server + ':' + this.$rootScope.$storage.port + this.BASE_URL;
            return baseUrl;
        }

        getMetricsForTimeRange(id:string, startDate:Date, endDate:Date, buckets:number):any {
            this.$log.info("-- Retrieving metrics data for id: " + id);
            this.$log.info("-- Date Range: " + startDate + " - " + endDate);
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
                this.$log.warn("Start date was after end date");
                deferred.reject("Start date was after end date");
            }

            this.$http.get(base + '/' + id, searchParams).success(function (data) {
                deferred.resolve(data);
            }).error(function (reason, status) {
                this.$log.error('Error Loading Chart Data:' + status + ", " + reason);
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
