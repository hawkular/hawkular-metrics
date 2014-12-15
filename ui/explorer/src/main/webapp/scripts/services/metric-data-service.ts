/// <reference path="../../vendor/vendor.d.ts" />

module Services {
    'use strict';


    export class MetricDataService {

        public static  $inject = ['$q', '$rootScope', '$http', '$localStorage', 'BASE_URL', 'TENANT_ID'];

        constructor(private $q:ng.IQService, private $rootScope:ng.IRootScopeService, private $http:ng.IHttpService,  public $localStorage:any, private BASE_URL:string, private TENANT_ID:string) {

        }

        getBaseUrl():string {
            var baseUrl = 'http://' + this.$rootScope.$storage.server.replace(/['"]+/g, '') + ':' + this.$rootScope.$storage.port + this.BASE_URL + '/'+this.TENANT_ID;
            return baseUrl;
        }

        getAllMetrics() {
            console.info('-- Retrieving all metrics');
            var base = this.getBaseUrl()+'/metrics/?type=num',
                deferred = this.$q.defer();


            this.$http.get(base).success((data) => {
                deferred.resolve(data);
            }).error((reason, status) => {
                console.error('Error Retrieving all metrics :' + status + ", " + reason);
                toastr.warning('No Metrics retrieved.');
                deferred.reject(status + " - " + reason);
            });

            return deferred.promise;
        }



        getMetricsForTimeRange(id:string, startDate:Date, endDate:Date, buckets:number):any {
            console.info('-- Retrieving metrics data for id: ' + id);
            console.info('-- Date Range: ' + startDate + ' - ' + endDate);
            var numBuckets = buckets || 60,
                base = this.getBaseUrl(),
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

            this.$http.get(base + '/metrics/numeric/' + id+'/data', searchParams).success((data) => {
                deferred.resolve(data);
            }).error((reason, status) => {
                console.error('Error Loading Chart Data:' + status + ", " + reason);
                deferred.reject(status + " - " + reason);
            });

            return deferred.promise;
        }

        insertSinglePayload(id, jsonPayload):any {
            var url = this.getBaseUrl(),
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
            var url = this.getBaseUrl(),
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
