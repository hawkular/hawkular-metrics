/// <reference path="../../vendor/vendor.d.ts" />
var Controllers;
(function (Controllers) {
    'use strict';

    var TimeRange = (function () {
        function TimeRange(startTimeStamp, endTimeStamp) {
            this.startTimeStamp = startTimeStamp;
            this.endTimeStamp = endTimeStamp;
        }
        TimeRange.prototype.getIntervalInSeconds = function () {
            return this.endTimeStamp - this.startTimeStamp;
        };

        TimeRange.prototype.moveToNextTimePeriod = function () {
            var next = this.calculateNextTimeRange();
            this.startTimeStamp = next.startTimeStamp;
            this.endTimeStamp = next.endTimeStamp;
        };

        TimeRange.prototype.moveToPreviousTimePeriod = function () {
            var previous = this.calculatePreviousTimeRange();
            this.startTimeStamp = previous.startTimeStamp;
            this.endTimeStamp = previous.endTimeStamp;
        };

        TimeRange.prototype.calculateNextTimeRange = function () {
            return new TimeRange(this.endTimeStamp, this.endTimeStamp + this.getIntervalInSeconds());
        };

        TimeRange.prototype.calculatePreviousTimeRange = function () {
            return new TimeRange(this.startTimeStamp - this.getIntervalInSeconds(), this.startTimeStamp);
        };

        TimeRange.prototype.getRelativeHumanTimeRange = function () {
            return moment(this.startTimeStamp).from(moment(this.endTimeStamp));
        };
        return TimeRange;
    })();
    Controllers.TimeRange = TimeRange;

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
        function DashboardController($scope, $rootScope, $interval, $localStorage, $log, metricDataService, dateRange) {
            var _this = this;
            this.$scope = $scope;
            this.$rootScope = $rootScope;
            this.$interval = $interval;
            this.$localStorage = $localStorage;
            this.$log = $log;
            this.metricDataService = metricDataService;
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
            this.selectedChart = {
                chartType: 'bar'
            };
            this.groupNames = [];
            this.defaultGroupName = 'Default Group';
            $scope.vm = this;
            this.currentTimeRange = new TimeRange(_.now() - (24 * 60 * 60), _.now()); // default to 24 hours

            $scope.$on('GraphTimeRangeChangedEvent', function (event, timeRange) {
                _this.currentTimeRange.startTimeStamp = timeRange[0];
                _this.currentTimeRange.endTimeStamp = timeRange[1];
                _this.dateRange = _this.currentTimeRange.getRelativeHumanTimeRange();
                _this.refreshAllChartsDataForTimeRange(_this.currentTimeRange);
            });

            $rootScope.$on('NewChartEvent', function (event, metricId) {
                if (_.contains(_this.selectedMetrics, metricId)) {
                    toastr.warning(metricId + ' is already selected');
                } else {
                    _this.selectedMetrics.push(metricId);
                    _this.searchId = metricId;
                    _this.refreshHistoricalChartData(metricId, _this.currentTimeRange);
                    toastr.success(metricId + ' Added to Dashboard!');
                }
            });

            $rootScope.$on('RemoveChartEvent', function (event, metricId) {
                if (_.contains(_this.selectedMetrics, metricId)) {
                    var pos = _.indexOf(_this.selectedMetrics, metricId);
                    _this.selectedMetrics.splice(pos, 1);
                    _this.searchId = metricId;
                    toastr.info('Removed: ' + metricId + ' from Dashboard!');
                    _this.refreshAllChartsDataForTimeRange(_this.currentTimeRange);
                }
            });

            $rootScope.$on('SidebarRefreshedEvent', function () {
                _this.selectedMetrics = [];
                _this.selectedGroup = '';
                _this.loadAllChartGroupNames();
                _this.loadSelectedChartGroup(_this.defaultGroupName);
            });

            $rootScope.$on('LoadInitialChartGroup', function () {
                // due to timing issues we need to pause for a few seconds to allow the app to start
                // and must be greater than 1 sec delay in app.ts
                var startIntervalPromise = $interval(function () {
                    _this.selectedGroup = 'Default Group';
                    _this.loadSelectedChartGroup(_this.defaultGroupName);
                    $interval.cancel(startIntervalPromise);
                }, 1300);
            });

            this.$scope.$watch(function () {
                return _this.selectedGroup;
            }, function (newValue) {
                _this.loadSelectedChartGroup(newValue);
            });

            this.$scope.$watchCollection(function () {
                return _this.selectedMetrics;
            }, function () {
                if (_this.selectedMetrics.length > 0) {
                    _this.saveChartGroup(_this.defaultGroupName);
                    _this.$rootScope.$broadcast('SelectedMetricsChangedEvent', _this.selectedMetrics);
                }
            });
        }
        DashboardController.prototype.noDataFoundForId = function (id) {
            this.$log.warn('No Data found for id: ' + id);
            toastr.warning('No Data found for id: ' + id);
        };

        DashboardController.prototype.getChartHtmlTextToCopy = function (metricId) {
            return '<rhqm-chart chart-type="bar" metric-id="' + metricId + '" metric-url="' + this.metricDataService.getBaseUrl() + '" time-range-in-seconds="' + this.currentTimeRange.getIntervalInSeconds() + '" refresh-interval-in-seconds="30"  chart-height="250" ></rhqm-chart>';
        };

        DashboardController.prototype.clickChartHtmlCopy = function () {
            toastr.info("Copied Chart to Clipboard");
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
                    _this.refreshAllChartsDataForTimeRange(_this.currentTimeRange);
                }, intervalInSeconds * 1000);
            } else {
                this.$interval.cancel(this.updateLastTimeStampToNowPromise);
            }

            this.$scope.$on('$destroy', function () {
                _this.$interval.cancel(_this.updateLastTimeStampToNowPromise);
            });
        };

        DashboardController.prototype.updateTimestampsToNow = function () {
            var interval = this.currentTimeRange.getIntervalInSeconds();
            this.currentTimeRange.endTimeStamp = _.now();
            this.currentTimeRange.startTimeStamp = this.currentTimeRange.endTimeStamp - interval;
        };

        DashboardController.prototype.refreshChartDataNow = function () {
            this.updateTimestampsToNow();
            this.refreshAllChartsDataForTimeRange(this.currentTimeRange);
        };

        DashboardController.prototype.refreshHistoricalChartData = function (metricId, timeRange) {
            this.refreshHistoricalChartDataForTimestamp(metricId, timeRange.startTimeStamp, timeRange.endTimeStamp);
        };

        DashboardController.prototype.refreshHistoricalChartDataForTimestamp = function (metricId, startTime, endTime) {
            var _this = this;
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
                this.metricDataPromise = this.metricDataService.getMetricsForTimeRange(metricId, new Date(startTime), new Date(endTime)).then(function (response) {
                    // we want to isolate the response from the data we are feeding to the chart
                    _this.bucketedDataPoints = _this.formatBucketedChartOutput(response);

                    if (_this.bucketedDataPoints.length !== 0) {
                        _this.$log.info("Retrieving data for metricId: " + metricId);

                        // this is basically the DTO for the chart
                        _this.chartData[metricId] = {
                            id: metricId,
                            startTimeStamp: _this.currentTimeRange.startTimeStamp,
                            endTimeStamp: _this.currentTimeRange.endTimeStamp,
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

        DashboardController.prototype.refreshAllChartsDataForTimeRange = function (timeRange) {
            var _this = this;
            _.each(this.selectedMetrics, function (aMetric) {
                _this.$log.info("Reloading Metric Chart Data for: " + aMetric);
                _this.refreshHistoricalChartDataForTimestamp(aMetric, timeRange.startTimeStamp, timeRange.endTimeStamp);
            });
        };

        DashboardController.prototype.getChartDataFor = function (metricId) {
            if (angular.isUndefined(this.chartData[metricId])) {
                return;
            } else {
                return this.chartData[metricId].dataPoints;
            }
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
            this.currentTimeRange.moveToPreviousTimePeriod();
            this.refreshAllChartsDataForTimeRange(this.currentTimeRange);
            this.dateRange = moment(this.currentTimeRange.startTimeStamp).from(moment(_.now()));
        };

        DashboardController.prototype.showNextTimeRange = function () {
            this.currentTimeRange.moveToNextTimePeriod();
            this.refreshAllChartsDataForTimeRange(this.currentTimeRange);
            this.dateRange = moment(this.currentTimeRange.startTimeStamp).from(moment(_.now()));
        };

        DashboardController.prototype.hasNext = function () {
            var nextTimeRange = this.currentTimeRange.calculateNextTimeRange();

            // unsophisticated test to see if there is a next; without actually querying.
            //@fixme: pay the price, do the query!?
            return nextTimeRange.endTimeStamp < _.now();
        };

        DashboardController.prototype.saveChartGroup = function (groupName) {
            var _this = this;
            console.debug("Saving GroupName: " + groupName);
            var savedGroups = [];
            var previousGroups = localStorage.getItem('groups');
            var aGroupName = angular.isUndefined(groupName) ? this.selectedGroup : groupName;

            if (previousGroups !== null) {
                _.each(angular.fromJson(previousGroups), function (item) {
                    if (item.groupName !== _this.defaultGroupName) {
                        savedGroups.push({ 'groupName': item.groupName, 'metrics': item.metrics });
                    }
                });
            }

            // Add the 'Default Group'
            var defaultGroupEntry = { 'groupName': this.defaultGroupName, 'metrics': this.selectedMetrics };
            if (this.selectedMetrics.length > 0) {
                savedGroups.push(defaultGroupEntry);
            }

            // Add the new group name
            var newEntry = { 'groupName': aGroupName, 'metrics': this.selectedMetrics };
            if (aGroupName !== this.defaultGroupName) {
                savedGroups.push(newEntry);
            }

            localStorage.setItem('groups', angular.toJson(savedGroups));
            this.loadAllChartGroupNames();
            this.selectedGroup = groupName;
        };

        DashboardController.prototype.loadAllChartGroupNames = function () {
            var _this = this;
            var existingGroups = localStorage.getItem('groups');
            var groups = angular.fromJson(existingGroups);
            this.groupNames = [];
            _.each(groups, function (item) {
                _this.groupNames.push(item.groupName);
            });
        };

        DashboardController.prototype.loadSelectedChartGroup = function (selectedGroup) {
            var _this = this;
            var groups = angular.fromJson(localStorage.getItem('groups'));

            if (angular.isDefined(groups)) {
                _.each(groups, function (item) {
                    if (item.groupName === selectedGroup) {
                        _this.selectedMetrics = item.metrics;
                        _this.refreshAllChartsDataForTimeRange(_this.currentTimeRange);
                    }
                });
            }
        };
        DashboardController.$inject = ['$scope', '$rootScope', '$interval', '$localStorage', '$log', 'metricDataService'];
        return DashboardController;
    })();
    Controllers.DashboardController = DashboardController;

    angular.module('chartingApp').controller('DashboardController', DashboardController);
})(Controllers || (Controllers = {}));
//# sourceMappingURL=dashboard-controller.js.map
