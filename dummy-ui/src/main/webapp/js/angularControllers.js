
function MetricsController($scope, metricSearchService) {

//     use strict;

    $scope.init = function() {
        metricSearchService.loadMetrics(function (metrics) {
            $scope.metrics = metrics;
        })
    };

    $scope.submitSearch = function() {

        metricSearchService.findMetrics($scope.searchKeyword, function(metrics) {
            $scope.metrics = metrics;

        });

    }



    $scope.graph = function(schedule,divId) {

        $scope.scheduleName = schedule.title;
        // show actual graph
        rhq.drawGraph(schedule,divId);
    }




}