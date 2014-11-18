/// <reference path="../../vendor/vendor.d.ts" />

module Controllers {
    'use strict';

    export interface IContextChartDataPoint {
        timestamp: number;
        value: number;
        avg: number;
        empty: boolean;
    }

    export interface IChartDataPoint extends IContextChartDataPoint {
        date: Date;
        min: number;
        max: number;
    }

    export interface IChartType {
        chartType: string;
        icon:string;
        previousRangeData: boolean;
        enabled:boolean;
    }

    export interface IMetricGroup {
        groupName:string;
        metrics:string[];
    }


    export class TimeRange {

        constructor(public startTimeStamp:number, public endTimeStamp:number) {

        }

        getIntervalInSeconds() {
            return this.endTimeStamp - this.startTimeStamp;
        }

        moveToNextTimePeriod() {
            var next = this.calculateNextTimeRange();
            this.startTimeStamp = next.startTimeStamp;
            this.endTimeStamp = next.endTimeStamp;
        }

        moveToPreviousTimePeriod() {
            var previous = this.calculatePreviousTimeRange();
            this.startTimeStamp = previous.startTimeStamp;
            this.endTimeStamp = previous.endTimeStamp;
        }

        calculateNextTimeRange():TimeRange {
            return new TimeRange(this.endTimeStamp, this.endTimeStamp + this.getIntervalInSeconds());
        }

        calculatePreviousTimeRange():TimeRange {
            return new TimeRange(this.startTimeStamp - this.getIntervalInSeconds(), this.startTimeStamp);
        }

        getRelativeHumanTimeRange():string {
            return moment(this.startTimeStamp).from(moment(this.endTimeStamp));

        }
    }

    /**
     * @ngdoc controller
     * @name DashboardController
     * @description This controller is responsible for handling activity related to the console Dashboard
     * @param $scope
     * @param $rootScope
     * @param $interval
     * @param $log
     * @param metricDataService
     */
    export class DashboardController {
        public static  $inject = ['$scope', '$rootScope', '$interval', '$localStorage', '$log', 'metricDataService'];

        private updateLastTimeStampToNowPromise:ng.IPromise<number>;
        private chartData = {};
        private bucketedDataPoints:IChartDataPoint[] = [];

        // this promise needs to be exposed so the page can render busy wait spinning icons
        metricDataPromise;

        selectedMetrics:string[] = [];
        searchId = '';
        updateEndTimeStampToNow = false;
        showAutoRefreshCancel = false;
        chartTypes:IChartType[] = [
            {chartType: 'bar', icon: 'fa fa-bar-chart', enabled: true, previousRangeData: false},
            {chartType: 'line', icon: 'fa fa-line-chart', enabled: true, previousRangeData: false},
            {chartType: 'area', icon: 'fa fa-area-chart', enabled: true, previousRangeData: false},
            {chartType: 'scatterline', icon: 'fa fa-circle-thin', enabled: true, previousRangeData: false}
        ];
        selectedChart = {
            chartType : 'bar'
        };

        selectedGroup:string;
        groupNames:string[] = [];
        defaultGroupName = 'Default Group';

        currentTimeRange:TimeRange;


        constructor(private $scope:ng.IScope, private $rootScope:ng.IRootScopeService, private $interval:ng.IIntervalService, private $localStorage, private $log:ng.ILogService, private metricDataService, public dateRange:string) {
            $scope.vm = this;
            this.currentTimeRange = new TimeRange(_.now() - (24 * 60 * 60), _.now()); // default to 24 hours

            $scope.$on('GraphTimeRangeChangedEvent', (event, timeRange) => {
                this.currentTimeRange.startTimeStamp = timeRange[0];
                this.currentTimeRange.endTimeStamp = timeRange[1];
                this.dateRange = this.currentTimeRange.getRelativeHumanTimeRange();
                this.refreshAllChartsDataForTimeRange(this.currentTimeRange);
            });

            $rootScope.$on('NewChartEvent', (event, metricId) => {
                if (_.contains(this.selectedMetrics, metricId)) {
                    toastr.warning(metricId + ' is already selected');
                } else {
                    this.selectedMetrics.push(metricId);
                    this.searchId = metricId;
                    this.refreshHistoricalChartData(metricId, this.currentTimeRange);
                    toastr.success(metricId + ' Added to Dashboard!');
                }
            });

            $rootScope.$on('RemoveChartEvent', (event, metricId) => {
                if (_.contains(this.selectedMetrics, metricId)) {
                    var pos = _.indexOf(this.selectedMetrics, metricId);
                    this.selectedMetrics.splice(pos, 1);
                    this.searchId = metricId;
                    toastr.info('Removed: ' + metricId + ' from Dashboard!');
                    this.refreshAllChartsDataForTimeRange(this.currentTimeRange);
                }
            });

            $rootScope.$on('SidebarRefreshedEvent', () => {
                this.selectedMetrics = [];
                this.selectedGroup = '';
                this.loadAllChartGroupNames();
                this.loadSelectedChartGroup(this.defaultGroupName);
            });


            $rootScope.$on('LoadInitialChartGroup', () => {
                // due to timing issues we need to pause for a few seconds to allow the app to start
                // and must be greater than 1 sec delay in app.ts
                var startIntervalPromise = $interval(() => {
                    this.selectedGroup = 'Default Group';
                    this.loadSelectedChartGroup(this.defaultGroupName);
                    $interval.cancel(startIntervalPromise);
                }, 1300);
            });

            this.$scope.$watch(() => this.selectedGroup,
                (newValue:string) => {
                    this.loadSelectedChartGroup(newValue);
                });

            this.$scope.$watchCollection(() => this.selectedMetrics,
                () => {
                    if(this.selectedMetrics.length > 0){
                        this.saveChartGroup(this.defaultGroupName);
                        this.$rootScope.$broadcast('SelectedMetricsChangedEvent', this.selectedMetrics);
                    }
                });

        }


        private noDataFoundForId(id:string):void {
            this.$log.warn('No Data found for id: ' + id);
            toastr.warning('No Data found for id: ' + id);
        }


        getChartHtmlTextToCopy(metricId:string):string {
            return '<rhqm-chart chart-type="bar" metric-id="'+metricId+'" metric-url="'+this.metricDataService.getBaseUrl()+'" time-range-in-seconds="'+this.currentTimeRange.getIntervalInSeconds()+'" refresh-interval-in-seconds="30"  chart-height="250" ></rhqm-chart>';
        }

        clickChartHtmlCopy():void {
            toastr.info("Copied Chart to Clipboard");
        }

        deleteChart(metricId:string):void {
            var pos = _.indexOf(this.selectedMetrics, metricId);
            this.selectedMetrics.splice(pos, 1);
            this.$rootScope.$broadcast('RemoveSelectedMetricEvent', metricId);
        }


        cancelAutoRefresh():void {
            this.showAutoRefreshCancel = !this.showAutoRefreshCancel;
            this.$interval.cancel(this.updateLastTimeStampToNowPromise);
            toastr.info('Canceling Auto Refresh');
        }

        autoRefresh(intervalInSeconds:number):void {
            toastr.info('Auto Refresh Mode started');
            this.updateEndTimeStampToNow = !this.updateEndTimeStampToNow;
            this.showAutoRefreshCancel = true;
            if (this.updateEndTimeStampToNow) {
                this.showAutoRefreshCancel = true;
                this.updateLastTimeStampToNowPromise = this.$interval(() => {
                    this.updateTimestampsToNow();
                    this.refreshAllChartsDataForTimeRange(this.currentTimeRange);
                }, intervalInSeconds * 1000);

            } else {
                this.$interval.cancel(this.updateLastTimeStampToNowPromise);
            }

            this.$scope.$on('$destroy', () => {
                this.$interval.cancel(this.updateLastTimeStampToNowPromise);
            });
        }

        private updateTimestampsToNow():void {
            var interval:number = this.currentTimeRange.getIntervalInSeconds();
            this.currentTimeRange.endTimeStamp = _.now();
            this.currentTimeRange.startTimeStamp = this.currentTimeRange.endTimeStamp - interval;
        }

        refreshChartDataNow():void {
            this.updateTimestampsToNow();
            this.refreshAllChartsDataForTimeRange(this.currentTimeRange);
        }

        refreshHistoricalChartData(metricId:string, timeRange:TimeRange):void {
            this.refreshHistoricalChartDataForTimestamp(metricId, timeRange.startTimeStamp, timeRange.endTimeStamp);
        }

        refreshHistoricalChartDataForTimestamp(metricId:string, startTime?:number, endTime?:number):void {
            // calling refreshChartData without params use the model values
            if (angular.isUndefined(endTime)) {
                endTime = this.currentTimeRange.endTimeStamp;
            }
            if (angular.isUndefined(startTime)) {
                startTime = this.currentTimeRange.startTimeStamp;
            }

            if (startTime >= endTime) {
                this.$log.warn('Start Date was >= End Date');
                toastr.warning('Start Date was after End Date');
                return;
            }

            if (metricId !== '') {

                this.metricDataPromise = this.metricDataService.getMetricsForTimeRange(metricId, new Date(startTime), new Date(endTime))
                    .then((response) => {
                        // we want to isolate the response from the data we are feeding to the chart
                        this.bucketedDataPoints = this.formatBucketedChartOutput(response);

                        if (this.bucketedDataPoints.length !== 0) {
                            this.$log.info("Retrieving data for metricId: " + metricId);
                            // this is basically the DTO for the chart
                            this.chartData[metricId] = {
                                id: metricId,
                                startTimeStamp: this.currentTimeRange.startTimeStamp,
                                endTimeStamp: this.currentTimeRange.endTimeStamp,
                                dataPoints: this.bucketedDataPoints
                            };

                        } else {
                            this.noDataFoundForId(metricId);
                        }

                    }, (error) => {
                        toastr.error('Error Loading Chart Data: ' + error);
                    });
            }

        }

        refreshAllChartsDataForTimeRange(timeRange:TimeRange):void {

            _.each(this.selectedMetrics, (aMetric) => {
                this.$log.info("Reloading Metric Chart Data for: " + aMetric);
                this.refreshHistoricalChartDataForTimestamp(aMetric, timeRange.startTimeStamp, timeRange.endTimeStamp);
            });

        }

        getChartDataFor(metricId:string):IChartDataPoint[] {
            if(angular.isUndefined(this.chartData[metricId])){
                return;
            }else {
                return this.chartData[metricId].dataPoints;
            }
        }

        refreshPreviousRangeDataForTimestamp(metricId:string, previousRangeStartTime:number, previousRangeEndTime:number):void {

            if (previousRangeStartTime >= previousRangeEndTime) {
                this.$log.warn('Previous Range Start Date was >= Previous Range End Date');
                toastr.warning('Previous Range Start Date was after Previous Range End Date');
                return;
            }

            if (metricId !== '') {

                this.metricDataService.getMetricsForTimeRange(metricId, new Date(previousRangeStartTime), new Date(previousRangeEndTime))
                    .then((response) => {
                        // we want to isolate the response from the data we are feeding to the chart
                        this.bucketedDataPoints = this.formatBucketedChartOutput(response);

                        if (this.bucketedDataPoints.length !== 0) {
                            this.$log.info("Retrieving previous range data for metricId: " + metricId);
                            this.chartData[metricId].previousStartTimeStamp = previousRangeStartTime;
                            this.chartData[metricId].previousEndTimeStamp = previousRangeEndTime;
                            this.chartData[metricId].previousDataPoints = this.bucketedDataPoints;

                        } else {
                            this.noDataFoundForId(metricId);
                        }

                    }, (error) => {
                        toastr.error('Error Loading Chart Data: ' + error);
                    });
            }

        }


        getPreviousRangeDataFor(metricId:string):IChartDataPoint[] {

            return this.chartData[metricId].previousDataPoints;

        }

        private formatBucketedChartOutput(response):IChartDataPoint[] {
            //  The schema is different for bucketed output
            return _.map(response, (point:IChartDataPoint) => {
                return {
                    timestamp: point.timestamp,
                    date: new Date(point.timestamp),
                    value: !angular.isNumber(point.value) ? 0 : point.value,
                    avg: (point.empty) ? 0 : point.avg,
                    min: !angular.isNumber(point.min) ? 0 : point.min,
                    max: !angular.isNumber(point.max) ? 0 : point.max,
                    empty: point.empty
                };
            });
        }


        showPreviousTimeRange():void {
            this.currentTimeRange.moveToPreviousTimePeriod();
            this.refreshAllChartsDataForTimeRange(this.currentTimeRange);
            this.dateRange = moment(this.currentTimeRange.startTimeStamp).from(moment(_.now()));

        }

        showNextTimeRange():void {
            this.currentTimeRange.moveToNextTimePeriod();
            this.refreshAllChartsDataForTimeRange(this.currentTimeRange);
            this.dateRange = moment(this.currentTimeRange.startTimeStamp).from(moment(_.now()));
        }


        hasNext():boolean {
            var nextTimeRange = this.currentTimeRange.calculateNextTimeRange();
            // unsophisticated test to see if there is a next; without actually querying.
            //@fixme: pay the price, do the query!?
            return nextTimeRange.endTimeStamp < _.now();
        }


        saveChartGroup(groupName:string) {
            console.debug("Saving GroupName: " + groupName);
            var savedGroups:IMetricGroup[] = [];
            var previousGroups = localStorage.getItem('groups');
            var aGroupName = angular.isUndefined(groupName) ? this.selectedGroup : groupName;

            if (previousGroups !== null) {
                _.each(angular.fromJson(previousGroups), (item:IMetricGroup) => {
                    if (item.groupName !== this.defaultGroupName) {
                        savedGroups.push({'groupName': item.groupName, 'metrics': item.metrics});
                    }
                });
            }

            // Add the 'Default Group'
            var defaultGroupEntry:IMetricGroup = {'groupName': this.defaultGroupName, 'metrics': this.selectedMetrics};
            if(this.selectedMetrics.length > 0){
                savedGroups.push(defaultGroupEntry);
            }

            // Add the new group name
            var newEntry:IMetricGroup = {'groupName': aGroupName, 'metrics': this.selectedMetrics};
            if(aGroupName !== this.defaultGroupName){
                savedGroups.push(newEntry);
            }

            localStorage.setItem('groups', angular.toJson(savedGroups));
            this.loadAllChartGroupNames();
            this.selectedGroup = groupName;
        }

        loadAllChartGroupNames() {
            var existingGroups = localStorage.getItem('groups');
            var groups = angular.fromJson(existingGroups);
            this.groupNames = [];
            _.each(groups, (item:IMetricGroup) => {
                this.groupNames.push(item.groupName);
            });
        }

        loadSelectedChartGroup(selectedGroup:string) {
            var groups = angular.fromJson(localStorage.getItem('groups'));

            if (angular.isDefined(groups)) {
                _.each(groups, (item:IMetricGroup) => {
                    if (item.groupName === selectedGroup) {
                        this.selectedMetrics = item.metrics;
                        this.refreshAllChartsDataForTimeRange(this.currentTimeRange);
                    }
                });
            }
        }
    }


    angular.module('chartingApp')
        .controller('DashboardController', DashboardController);
}
