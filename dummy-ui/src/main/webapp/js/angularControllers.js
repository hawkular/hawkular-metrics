
function MetricsController($scope, metricSearchService) {

//     use strict;

    $scope.displayMode = "Raw";
    $scope.$watch('displayMode', function() {

        // redraw if a metric was already selected
        if ($scope.schedule) {
            $scope.graph($scope.schedule,$scope.divId);
        }
    });

    function loadMetricIds() {
        metricSearchService.loadMetrics(function (metrics) {
            $scope.metrics = metrics;
        });
    }

    $scope.init = function(divId) {

        metricSearchService.populateDummy(function(data) {
            loadMetricIds();
        });

        loadMetricIds();
        $scope.divId = divId;
    };

    $scope.submitSearch = function() {

        metricSearchService.findMetrics($scope.searchKeyword, function(metrics) {
            $scope.metrics = metrics;

        });

    }

    $scope.reload = function() {
        if ($scope.schedule) {
            $scope.graph($scope.schedule,$scope.divId);
        }
        loadMetricIds();
    };

    $scope.graph = function(schedule) {

        $scope.scheduleName = schedule.title;
        $scope.schedule = schedule;
        // show actual graph

        switch ($scope.displayMode) {
        case "Raw":
            rhq.drawRawGraph(schedule, $scope.divId, false);

            break;

        case "Bucket60":
            rhq.drawWhisker(schedule, $scope.divId);

            break;

        case "BucketDay":
            rhq.drawWhisker2(schedule, $scope.divId);

            break;
        case "ClusterDay":
            rhq.drawRawGraph(schedule, $scope.divId, true);

            break;
        default:
            console.log("Unknown selector " + $scope.displayMode);

            break;
        }
    }




}