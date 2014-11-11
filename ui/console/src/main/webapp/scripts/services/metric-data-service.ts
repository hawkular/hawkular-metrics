/// <reference path="../../vendor/vendor.d.ts" />

module Services {
    'use strict';


    export class MetricDataService {

        public static  $inject = ['$q', '$rootScope', '$http', '$log', '$localStorage', 'BASE_URL' ];

        constructor(private $q:ng.IQService, private $rootScope:ng.IRootScopeService, private $http:ng.IHttpService, private $log:ng.ILogService, public $localStorage:any, private BASE_URL:string) {

        }

        getBaseUrl():string {
            var baseUrl = 'http://' + this.$rootScope.$storage.server.replace(/['"]+/g, '') + ':' + this.$rootScope.$storage.port + this.BASE_URL;
            return baseUrl;
        }

        getAllMetrics() {

            this.$log.info('-- Retrieving all metrics');
            var base = this.getBaseUrl(),
                that = this,
                deferred = this.$q.defer();


            this.$http.get(base).success((data) => {
                deferred.resolve(data);
            }).error((reason, status) => {
                that.$log.error('Error Retrieving all metrics :' + status + ", " + reason);
                toastr.warning('No Metrics retrieved.');
                deferred.reject(status + " - " + reason);
            });

            return deferred.promise;
        }


        getMetricsForTimeRange(id:string, startDate:Date, endDate:Date, buckets:number):any {
            this.$log.info('-- Retrieving metrics data for id: ' + id);
            this.$log.info('-- Date Range: ' + startDate + ' - ' + endDate);
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
                this.$log.warn("Start date was after end date");
                deferred.reject("Start date was after end date");
            }

            this.$http.get(base + '/' + id, searchParams).success((data) => {
                deferred.resolve(data);
            }).error((reason, status) => {
                //this.$log.error('Error Loading Chart Data:' + status + ", " + reason);
                deferred.reject(status + " - " + reason);
            });

            return deferred.promise;
        }

        insertSinglePayload(id, jsonPayload):any {
            var url = this.getBaseUrl(),
                deferred = this.$q.defer();
            this.$http.post(url + '/' + id, jsonPayload
            ).success(()=> {
                    deferred.resolve("Success");
                }).error((response, status) => {
                    console.error("Error: " + status + " --> " + response);
                    deferred.reject(status);
                });
            return deferred.promise;
        }

        insertMultiplePayload(jsonPayload):any {
            var url = this.getBaseUrl(),
                deferred = this.$q.defer();
            this.$http.post(url + '/', jsonPayload
            ).success(() => {
                    deferred.resolve("Success");
                }).error((response, status) => {
                    console.error("Error: " + status + " --> " + response);
                    deferred.reject(status);
                });
            return deferred.promise;
        }
    }


    angular.module('rhqm.services')
        .service('metricDataService', MetricDataService);
}
