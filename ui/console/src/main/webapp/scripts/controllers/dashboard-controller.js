/// <reference path="../../vendor/vendor.d.ts" />
var Controllers;
(function (Controllers) {
    'use strict';

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
    var DashboardController = (function () {
        function DashboardController($scope, $rootScope, $interval, $localStorage, $log, metricDataService, startTimeStamp, endTimeStamp, dateRange) {
            this.$scope = $scope;
            this.$rootScope = $rootScope;
            this.$interval = $interval;
            this.$localStorage = $localStorage;
            this.$log = $log;
            this.metricDataService = metricDataService;
            this.startTimeStamp = startTimeStamp;
            this.endTimeStamp = endTimeStamp;
            this.dateRange = dateRange;
            this.chartData = {};
            this.bucketedDataPoints = [];
            this.selectedMetrics = [];
            this.searchId = '';
            this.updateEndTimeStampToNow = false;
            this.showAutoRefreshCancel = false;
            this.chartTypes = [
                { chartType: 'bar', icon: 'fa fa-bar-chart', enabled: true, previousRangeData: false },
                { chartType: 'line', icon: 'fa fa-line-chart', enabled: true, previousRangeData: false },
                { chartType: 'area', icon: 'fa fa-area-chart', enabled: true, previousRangeData: false },
                { chartType: 'scatterline', icon: 'fa fa-circle-thin', enabled: true, previousRangeData: false }
            ];
            this.chartType = this.chartTypes[0].chartType;
            this.groupNames = [];
            $scope.vm = this;

            $scope.$on('GraphTimeRangeChangedEvent', function (event, timeRange) {
                $scope.vm.startTimeStamp = timeRange[0];
                $scope.vm.endTimeStamp = timeRange[1];
                $scope.vm.dateRange = moment(timeRange[0]).from(moment(timeRange[1]));
                $scope.vm.refreshAllChartsDataForTimestamp($scope.vm.startTimeStamp, $scope.vm.endTimeStamp);
            });

            $rootScope.$on('NewChartEvent', function (event, metricId) {
                if (_.contains($scope.vm.selectedMetrics, metricId)) {
                    toastr.warning(metricId + ' is already selected');
                } else {
                    $scope.vm.selectedMetrics.push(metricId);
                    $scope.vm.searchId = metricId;
                    $scope.vm.refreshHistoricalChartData(metricId, $scope.vm.startTimeStamp, $scope.vm.endTimeStamp);
                    toastr.success(metricId + ' Added to Dashboard!');
                }
            });

            $rootScope.$on('RefreshSidebarEvent', function () {
                $scope.vm.selectedMetrics = [];
                $scope.vm.selectedGroup = '';
                $scope.vm.loadAllGraphGroupNames();
            });

            $rootScope.$on('RemoveChartEvent', function (event, metricId) {
                if (_.contains($scope.vm.selectedMetrics, metricId)) {
                    var pos = _.indexOf($scope.vm.selectedMetrics, metricId);
                    $scope.vm.selectedMetrics.splice(pos, 1);
                    $scope.vm.searchId = metricId;
                    toastr.info('Removed: ' + metricId + ' from Dashboard!');
                    $scope.vm.refreshAllChartsDataForTimestamp($scope.vm.startTimeStamp, $scope.vm.endTimeStamp);
                }
            });

            this.$scope.$watch(function () {
                return $scope.vm.selectedGroup;
            }, function (newValue) {
                $scope.vm.loadSelectedGraphGroup(newValue);
            });
        }
        DashboardController.prototype.noDataFoundForId = function (id) {
            this.$log.warn('No Data found for id: ' + id);
            toastr.warning('No Data found for id: ' + id);
        };

        DashboardController.prototype.deleteChart = function (metricId) {
            var pos = _.indexOf(this.selectedMetrics, metricId);
            this.selectedMetrics.splice(pos, 1);
            this.$rootScope.$broadcast('RemoveSelectedMetricEvent', metricId);
        };

        DashboardController.prototype.cancelAutoRefresh = function () {
            this.showAutoRefreshCancel = !this.showAutoRefreshCancel;
            this.$interval.cancel(this.updateLastTimeStampToNowPromise);
            toastr.info('Canceling Auto Refresh');
        };

        DashboardController.prototype.autoRefresh = function (intervalInSeconds) {
            var _this = this;
            toastr.info('Auto Refresh Mode started');
            this.updateEndTimeStampToNow = !this.updateEndTimeStampToNow;
            this.showAutoRefreshCancel = true;
            if (this.updateEndTimeStampToNow) {
                this.showAutoRefreshCancel = true;
                this.updateLastTimeStampToNowPromise = this.$interval(function () {
                    _this.updateTimestampsToNow();
                    _this.refreshAllChartsDataForTimestamp();
                }, intervalInSeconds * 1000);
            } else {
                this.$interval.cancel(this.updateLastTimeStampToNowPromise);
            }

            this.$scope.$on('$destroy', function () {
                _this.$interval.cancel(_this.updateLastTimeStampToNowPromise);
            });
        };

        DashboardController.prototype.updateTimestampsToNow = function () {
            var interval = this.$scope.vm.endTimeStamp - this.$scope.vm.startTimeStamp;
            this.$scope.vm.endTimeStamp = new Date().getTime();
            this.$scope.vm.startTimeStamp = this.$scope.vm.endTimeStamp - interval;
        };

        DashboardController.prototype.refreshChartDataNow = function () {
            this.updateTimestampsToNow();
            this.refreshAllChartsDataForTimestamp(this.$scope.vm.startTimeStamp, this.$scope.vm.endTimeStamp);
        };

        DashboardController.prototype.refreshHistoricalChartData = function (metricId, startDate, endDate) {
            this.refreshHistoricalChartDataForTimestamp(metricId, startDate.getTime(), endDate.getTime());
        };

        DashboardController.prototype.refreshHistoricalChartDataForTimestamp = function (metricId, startTime, endTime) {
            var _this = this;
            // calling refreshChartData without params use the model values
            if (angular.isUndefined(endTime)) {
                endTime = this.$scope.vm.endTimeStamp;
            }
            if (angular.isUndefined(startTime)) {
                startTime = this.$scope.vm.startTimeStamp;
            }

            if (startTime >= endTime) {
                this.$log.warn('Start Date was >= End Date');
                toastr.warning('Start Date was after End Date');
                return;
            }

            if (metricId !== '') {
                this.metricDataService.getMetricsForTimeRange(metricId, new Date(startTime), new Date(endTime)).then(function (response) {
                    // we want to isolate the response from the data we are feeding to the chart
                    _this.bucketedDataPoints = _this.formatBucketedChartOutput(response);

                    if (_this.bucketedDataPoints.length !== 0) {
                        _this.$log.info("Retrieving data for metricId: " + metricId);

                        // this is basically the DTO for the chart
                        _this.chartData[metricId] = {
                            id: metricId,
                            startTimeStamp: _this.startTimeStamp,
                            endTimeStamp: _this.endTimeStamp,
                            dataPoints: _this.bucketedDataPoints
                        };
                    } else {
                        _this.noDataFoundForId(metricId);
                    }
                }, function (error) {
                    toastr.error('Error Loading Chart Data: ' + error);
                });
            }
        };

        DashboardController.prototype.refreshAllChartsDataForTimestamp = function (startTime, endTime) {
            var _this = this;
            _.each(this.selectedMetrics, function (aMetric) {
                _this.$log.info("Reloading Metric Chart Data for: " + aMetric);
                _this.refreshHistoricalChartDataForTimestamp(aMetric, startTime, endTime);
            });
        };

        DashboardController.prototype.getChartDataFor = function (metricId) {
            return this.chartData[metricId].dataPoints;
        };

        DashboardController.prototype.refreshPreviousRangeDataForTimestamp = function (metricId, previousRangeStartTime, previousRangeEndTime) {
            var _this = this;
            if (previousRangeStartTime >= previousRangeEndTime) {
                this.$log.warn('Previous Range Start Date was >= Previous Range End Date');
                toastr.warning('Previous Range Start Date was after Previous Range End Date');
                return;
            }

            if (metricId !== '') {
                this.metricDataService.getMetricsForTimeRange(metricId, new Date(previousRangeStartTime), new Date(previousRangeEndTime)).then(function (response) {
                    // we want to isolate the response from the data we are feeding to the chart
                    _this.bucketedDataPoints = _this.formatBucketedChartOutput(response);

                    if (_this.bucketedDataPoints.length !== 0) {
                        _this.$log.info("Retrieving previous range data for metricId: " + metricId);
                        _this.chartData[metricId].previousStartTimeStamp = previousRangeStartTime;
                        _this.chartData[metricId].previousEndTimeStamp = previousRangeEndTime;
                        _this.chartData[metricId].previousDataPoints = _this.bucketedDataPoints;
                    } else {
                        _this.noDataFoundForId(metricId);
                    }
                }, function (error) {
                    toastr.error('Error Loading Chart Data: ' + error);
                });
            }
        };

        DashboardController.prototype.getPreviousRangeDataFor = function (metricId) {
            return this.chartData[metricId].previousDataPoints;
        };

        DashboardController.prototype.formatBucketedChartOutput = function (response) {
            //  The schema is different for bucketed output
            return _.map(response, function (point) {
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
        };

        DashboardController.prototype.showPreviousTimeRange = function () {
            var previousTimeRange = TimeRange.calculatePreviousTimeRange(this.startTimeStamp, this.endTimeStamp);

            this.startTimeStamp = previousTimeRange[0];
            this.endTimeStamp = previousTimeRange[1];
            this.refreshAllChartsDataForTimestamp(this.startTimeStamp.getTime(), this.endTimeStamp.getTime());
        };

        DashboardController.prototype.showNextTimeRange = function () {
            var nextTimeRange = TimeRange.calculateNextTimeRange(this.startTimeStamp, this.endTimeStamp);

            this.startTimeStamp = nextTimeRange[0];
            this.endTimeStamp = nextTimeRange[1];
            this.refreshAllChartsDataForTimestamp(this.startTimeStamp.getTime(), this.endTimeStamp.getTime());
        };

        DashboardController.prototype.hasNext = function () {
            var nextTimeRange = TimeRange.calculateNextTimeRange(this.startTimeStamp, this.endTimeStamp);

            // unsophisticated test to see if there is a next; without actually querying.
            //@fixme: pay the price, do the query!
            return nextTimeRange[1].getTime() < _.now();
        };

        DashboardController.prototype.saveGraphsAsGroup = function (groupName) {
            console.debug("Saving GroupName: " + groupName);
            var savedGroups = [];
            var previousGroups = localStorage.getItem('groups');
            var aGroupName = angular.isUndefined(groupName) ? this.selectedGroup : groupName;

            if (previousGroups !== null) {
                _.each(angular.fromJson(previousGroups), function (item) {
                    savedGroups.push({ 'groupName': item.groupName, 'metrics': item.metrics });
                });
            }

            var newEntry = { 'groupName': aGroupName, 'metrics': this.selectedMetrics };
            savedGroups.push(newEntry);

            localStorage.setItem('groups', angular.toJson(savedGroups));
            this.loadAllGraphGroupNames();
            this.selectedGroup = groupName;
        };

        DashboardController.prototype.loadAllGraphGroupNames = function () {
            var _this = this;
            var existingGroups = localStorage.getItem('groups');
            var groups = angular.fromJson(existingGroups);
            this.groupNames = [];
            _.each(groups, function (item) {
                _this.groupNames.push(item.groupName);
            });
        };

        DashboardController.prototype.loadSelectedGraphGroup = function (selectedGroup) {
            var _this = this;
            var groups = angular.fromJson(localStorage.getItem('groups'));

            if (angular.isDefined(groups)) {
                _.each(groups, function (item) {
                    if (item.groupName === selectedGroup) {
                        _this.selectedMetrics = item.metrics;
                        _this.refreshAllChartsDataForTimestamp(_this.startTimeStamp.getTime(), _this.endTimeStamp.getTime());
                    }
                });
            }
        };
        DashboardController.$inject = ['$scope', '$rootScope', '$interval', '$localStorage', '$log', 'metricDataService'];
        return DashboardController;
    })();
    Controllers.DashboardController = DashboardController;

    var TimeRange = (function () {
        function TimeRange() {
        }
        TimeRange.calculateNextTimeRange = function (startDate, endDate) {
            var nextTimeRange = [];
            var intervalInMillis = endDate.getTime() - startDate.getTime();

            nextTimeRange.push(endDate);
            nextTimeRange.push(new Date(endDate.getTime() + intervalInMillis));
            return nextTimeRange;
        };

        TimeRange.calculatePreviousTimeRange = function (startDate, endDate) {
            var previousTimeRange = [];
            var intervalInMillis = endDate.getTime() - startDate.getTime();

            previousTimeRange.push(new Date(startDate.getTime() - intervalInMillis));
            previousTimeRange.push(startDate);
            return previousTimeRange;
        };
        return TimeRange;
    })();

    angular.module('chartingApp').controller('DashboardController', DashboardController);
})(Controllers || (Controllers = {}));
//# sourceMappingURL=dashboard-controller.js.map
