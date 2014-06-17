'use strict';


/**
 * @ngdoc controller
 * @name StreamingController
 * @param {expression} StreamingController
 */
angular.module('chartingApp')
    .controller('StreamingController', ['$scope', '$http', '$interval', 'BASE_URL', function ($scope, $http, $interval, BASE_URL) {
        var intervalPromise   ,
            randomIntFromInterval = function(min,max)
        {
            return Math.floor(Math.random()*(max-min+1)+min);
        };

        $scope.streamingInsertData = {
            timeStamp: moment().valueOf(),
            id: "",
            jsonPayload: "",
            count: 1,
            startNumber: 1,
            endNumber: 100,
            refreshTimerValue : 30,
            lastStreamedValue: 2,
            selectedRefreshInterval : $scope.timeIntervalInMinutes[0]
        };

        $scope.startStreaming = function () {
            console.info("Start Streaming Inserts");
            intervalPromise = $interval(function () {
                console.log("Timer has Run! for seconds: " + $scope.streamingInsertData.refreshTimerValue);
                $scope.streamingInsertData.count = $scope.streamingInsertData.count + 1;
                $scope.streamingInsertData.lastStreamedValue = randomIntFromInterval($scope.streamingInsertData.startNumber, $scope.streamingInsertData.endNumber);
                toastr.success('Streamed Value: '+$scope.streamingInsertData.lastStreamedValue);
                //$scope.$emit('refreshIntervalChangedEvent', $scope.selectedRefreshInterval);
                // no need to do anything as all we want to do change the interval
            }, $scope.streamingInsertData.refreshTimerValue * 1000);
            $scope.$on('$destroy', function () {
                console.info('Destroying intervalPromise');
                $interval.cancel(intervalPromise);
            });

        };

        $scope.stopStreaming = function () {
            toastr.info('Stopping Streaming Data.');
            console.info('Stop Streaming Data.');
            $interval.cancel(intervalPromise);
        };


    }]);
