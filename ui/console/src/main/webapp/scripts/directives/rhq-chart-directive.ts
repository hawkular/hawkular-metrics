/// <reference path="../../vendor/vendor.d.ts" />

module Directives {
    'use strict';

    declare var d3:any;
    declare var numeral:any;
    declare var console:any;

    /**
     * @ngdoc directive
     * @name rhqmChart
     * @description A d3 based charting direction to provide charting using various styles of charts like: bar, area, line, scatter.
     *
     */
    angular.module('rhqm.directives', [])
        .directive('rhqmChart', ['$rootScope', '$http', '$interval','$log', 'BASE_URL', function ($rootScope:ng.IRootScopeService, $http:ng.IHttpService, $interval:ng.IIntervalService,  $log:ng.ILogService, BASE_URL):ng.IDirective {

            function link(scope, element, attrs) {

                var dataPoints:any[] = [],
                    dataUrl = attrs.dataUrl || 'http://10.3.10.81:8080/rhq-metrics/metrics/',
                    metricId = attrs.metricId || '',
                    timeRangeInSeconds = +attrs.timeRangeInSeconds || 43200,
                    refreshIntervalInSeconds = +attrs.refreshIntervalInSeconds || 3600,
                    endTimestamp =  _.now(),
                    startTimestamp =  endTimestamp - timeRangeInSeconds,
                    previousRangeDataPoints = [],
                    annotationData = [],
                    contextData = [],
                    multiChartOverlayData = [],
                    chartHeight = +attrs.chartHeight || 250,
                    chartType = attrs.chartType || 'bar',
                    timeLabel = attrs.timeLabel || 'Time',
                    dateLabel = attrs.dateLabel || 'Date',
                    singleValueLabel = attrs.singleValueLabel || 'Raw Value',
                    noDataLabel = attrs.noDataLabel || 'No Data',
                    aggregateLabel = attrs.aggregateLabel || 'Aggregate',
                    startLabel = attrs.startLabel || 'Start',
                    endLabel = attrs.endLabel || 'End',
                    durationLabel = attrs.durationLabel || 'Bar Duration',
                    minLabel = attrs.minLabel || 'Min',
                    maxLabel = attrs.maxLabel || 'Max',
                    avgLabel = attrs.avgLabel || 'Avg',
                    timestampLabel = attrs.timestampLabel || 'Timestamp',
                    highBarColor = attrs.highBarColor || '#1794bc',
                    lowBarColor = attrs.lowBarColor || '#70c4e2',
                    leaderBarColor = attrs.leaderBarColor || '#d3d3d6',
                    rawValueBarColor = attrs.rawValueBarColor || '#50505a',
                    avgLineColor = attrs.avgLineColor || '#2e376a',
                    showAvgLine = true,
                    hideHighLowValues = false,
                    chartHoverDateFormat = attrs.chartHoverDateFormat || '%m/%d/%y',
                    chartHoverTimeFormat = attrs.chartHoverTimeFormat || '%I:%M:%S %p',
                    buttonBarDateTimeFormat = attrs.buttonbarDatetimeFormat || 'MM/DD/YYYY h:mm a';

                // chart specific vars
                var margin = {top: 10, right: 5, bottom: 5, left: 90},
                    contextMargin = {top: 150, right: 5, bottom: 5, left: 90},
                    xAxisContextMargin = {top: 190, right: 5, bottom: 5, left: 90},
                    width = 750 - margin.left - margin.right,
                    adjustedChartHeight = chartHeight - 50,
                    height = adjustedChartHeight - margin.top - margin.bottom,
                    smallChartThresholdInPixels = 600,
                    titleHeight = 30, titleSpace = 10,
                    innerChartHeight = height + margin.top - titleHeight - titleSpace + margin.bottom,
                    adjustedChartHeight2 = +titleHeight + titleSpace + margin.top,
                    barOffset = 2,
                    chartData,
                    calcBarWidth,
                    yScale,
                    timeScale,
                    yAxis,
                    xAxis,
                    tip,
                    brush,
                    brushGroup,
                    timeScaleForBrush,
                    timeScaleForContext,
                    chart,
                    chartParent,
                    context,
                    contextArea,
                    svg,
                    lowBound,
                    highBound,
                    avg,
                    peak,
                    min,
                    processedNewData,
                    processedPreviousRangeData;

                dataPoints = attrs.data;
                previousRangeDataPoints = attrs.previousRangeData;
                multiChartOverlayData = attrs.multiChartOverlayData;
                annotationData = attrs.annotationData;
                contextData = attrs.contextData;

                function xStartPosition(d) {
                    return timeScale(d.timestamp) + (calcBarWidth() / 2);
                }

                function getChartWidth():number {
                    //return angular.element("#" + chartContext.chartHandle).width();
                    return 760;
                }

                function useSmallCharts():boolean {
                    return getChartWidth() <= smallChartThresholdInPixels;
                }


                function oneTimeChartSetup():void {
                    console.log("OneTimeChartSetup");
                    // destroy any previous charts
                    if (angular.isDefined(chart)) {
                        chartParent.selectAll('*').remove();
                    }
                    chartParent = d3.select(element[0]);
                    chart = chartParent.append("svg");

                    createSvgDefs(chart);

                    tip = d3.tip()
                        .attr('class', 'd3-tip')
                        .offset([-10, 0])
                        .html(function (d, i) {
                            return buildHover(d, i);
                        });

                    svg = chart.append("g")
                        .attr("width", width + margin.left + margin.right)
                        .attr("height", innerChartHeight)
                        .attr("transform", "translate(" + margin.left + "," + (adjustedChartHeight2) + ")");


                    svg.call(tip);

                }


                function setupFilteredData(dataPoints):void {
                    function determineMultiMetricMinMax() {
                        var currentMax, currentMin, seriesMax, seriesMin, maxList = [], minList = [];
                        angular.forEach(multiChartOverlayData, function (series) {
                            console.debug("Series: " + series.length);
                            currentMax = d3.max(series.map(function (d) {
                                return !d.empty ? d.avg : 0;
                            }));
                            maxList.push(currentMax);
                            currentMin = d3.min(series.map(function (d) {
                                return !d.empty ? d.avg : Number.MAX_VALUE;
                            }));
                            minList.push(currentMin);

                        });
                        seriesMax = d3.max(maxList);
                        seriesMin = d3.min(minList);
                        return [seriesMin, seriesMax];
                    }

                    avg = d3.mean(dataPoints.map(function (d) {
                        return !d.empty ? d.avg : 0;
                    }));

                    if (angular.isDefined(multiChartOverlayData)) {
                        var minMax = determineMultiMetricMinMax();
                        peak = minMax[1];
                        min = minMax[0];
                    }

                    peak = d3.max(dataPoints.map(function (d) {
                        return !d.empty ? d.max : 0;
                    }));

                    min = d3.min(dataPoints.map(function (d) {
                        return !d.empty ? d.min : undefined;
                    }));

                    lowBound = min - (min * 0.1);
                    highBound = peak + ((peak - min) * 0.1);
                }

                function determineScale(dataPoints) {
                    var xTicks, xTickSubDivide, numberOfBarsForSmallGraph = 20;

                    if (dataPoints.length > 0) {

                        // if window is too small server up small chart
                        if (useSmallCharts()) {
                            width = 250;
                            xTicks = 3;
                            xTickSubDivide = 2;
                            chartData = dataPoints.slice(dataPoints.length - numberOfBarsForSmallGraph, dataPoints.length);
                        }
                        else {
                            //  we use the width already defined above
                            xTicks = 8;
                            xTickSubDivide = 5;
                            chartData = dataPoints;
                        }

                        setupFilteredData(dataPoints);

                        calcBarWidth = function () {
                            return (width / chartData.length - barOffset  );
                        };

                        yScale = d3.scale.linear()
                            .clamp(true)
                            .rangeRound([height, 0])
                            .domain([lowBound, highBound]);

                        yAxis = d3.svg.axis()
                            .scale(yScale)
                            .tickSubdivide(1)
                            .ticks(5)
                            .tickSize(4, 4, 0)
                            .orient("left");


                        timeScale = d3.time.scale()
                            .range([0, width])
                            .domain(d3.extent(chartData, function (d:any) {
                                return d.timestamp;
                            }));

                        if (isListDefinedAndHasValues(contextData)) {
                            timeScaleForContext = d3.time.scale()
                                .range([0, width])
                                .domain(d3.extent(contextData, function (d:any) {
                                    return d.timestamp;
                                }));
                        } else {
                            timeScaleForBrush = d3.time.scale()
                                .range([0, width])
                                .domain(d3.extent(chartData, function (d:any) {
                                    return d.timestamp;
                                }));


                        }

                        xAxis = d3.svg.axis()
                            .scale(timeScale)
                            .ticks(xTicks)
                            .tickSubdivide(xTickSubDivide)
                            .tickSize(4, 4, 0)
                            .orient("bottom");

                    }
                }


                function getBaseUrl():string {
                    var baseUrl;
                        if(angular.isUndefined(dataUrl) || dataUrl === ''){
                            baseUrl = 'http://' + $rootScope.$storage.server.replace(/['"]+/g, '') + ':' + $rootScope.$storage.port + BASE_URL;
                        }else {
                            baseUrl = dataUrl;
                        }
                    return baseUrl;
                }


                function loadMetricsForTimeRange(url, metricId, startTimestamp, endTimestamp, buckets) {
                    $log.info('-- Retrieving metrics data for urlData: ' + metricId);
                    $log.info('-- Date Range: ' + new Date(startTimestamp) + ' - ' + new Date(endTimestamp));
                    var numBuckets = buckets || 60,
                        searchParams =
                        {
                            params: {
                                start: startTimestamp,
                                end: endTimestamp,
                                buckets: numBuckets
                            }
                        };

                    if (startTimestamp >= endTimestamp) {
                        $log.warn('Start date was after end date');
                    }

                    $http.get(url+ metricId, searchParams).success(function (response) {

                        processedNewData = formatBucketedChartOutput(response);
                        console.info("DataPoints from standalone URL:");
                        //console.table(processedNewData);
                        scope.render(processedNewData, processedPreviousRangeData);

                    }).error(function (reason, status) {
                        $log.error('Error Loading Chart Data:' + status + ", " + reason);
                    });

                }

                function formatBucketedChartOutput(response) {
                    //  The schema is different for bucketed output
                    return _.map(response, function (point:any) {
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


                function isEmptyDataBar(d) {
                    return d.empty;
                }

                function isRawMetric(d) {
                    return d.value;
                }


                function buildHover(d, i) {
                    var hover,
                        prevTimestamp,
                        currentTimestamp = d.timestamp,
                        barDuration,
                        formattedDateTime = moment(d.timestamp).format(buttonBarDateTimeFormat);

                    if (i > 0) {
                        prevTimestamp = chartData[i - 1].timestamp;
                        barDuration = moment(currentTimestamp).from(moment(prevTimestamp), true);
                    }

                    if (isEmptyDataBar(d)) {
                        // nodata
                        hover = "<div class='chartHover'><small class='chartHoverLabel'>" + noDataLabel + "</small>" +
                        "<div><small><span class='chartHoverLabel'>" + durationLabel + "</span><span>: </span><span class='chartHoverValue'>" + barDuration + "</span></small> </div>" +
                        "<hr/>" +
                        "<div><small><span class='chartHoverLabel'>" + timestampLabel + "</span><span>: </span><span class='chartHoverValue'>" + formattedDateTime + "</span></small></div></div>";
                    } else {
                        if (isRawMetric(d)) {
                            // raw single value from raw table
                            hover = "<div class='chartHover'><div><small><span class='chartHoverLabel'>" + timestampLabel + "</span><span>: </span><span class='chartHoverValue'>" + formattedDateTime + "</span></small></div>" +
                            "<div><small><span class='chartHoverLabel'>" + durationLabel + "</span><span>: </span><span class='chartHoverValue'>" + barDuration + "</span></small> </div>" +
                            "<hr/>" +
                            "<div><small><span class='chartHoverLabel'>" + singleValueLabel + "</span><span>: </span><span class='chartHoverValue'>" + numeral(d.value).format('0,0.0') + "</span></small> </div></div> ";
                        } else {
                            // aggregate with min/avg/max
                            hover = "<div class='chartHover'><div><small><span class='chartHoverLabel'>" + timestampLabel + "</span><span>: </span><span class='chartHoverValue'>" + formattedDateTime + "</span></small></div>" +
                            "<div><small><span class='chartHoverLabel'>" + durationLabel + "</span><span>: </span><span class='chartHoverValue'>" + barDuration + "</span></small> </div>" +
                            "<hr/>" +
                            "<div><small><span class='chartHoverLabel'>" + maxLabel + "</span><span>: </span><span class='chartHoverValue'>" + numeral(d.max).format('0,0.0') + "</span></small> </div> " +
                            "<div><small><span class='chartHoverLabel'>" + avgLabel + "</span><span>: </span><span class='chartHoverValue'>" + numeral(d.avg).format('0,0.0') + "</span></small> </div> " +
                            "<div><small><span class='chartHoverLabel'>" + minLabel + "</span><span>: </span><span class='chartHoverValue'>" + numeral(d.min).format('0,0.0') + "</span></small> </div></div> ";
                        }
                    }
                    return hover;

                }

                function createHeader(titleName) {
                    var title = chart.append("g").append("rect")
                        .attr("class", "title")
                        .attr("x", 30)
                        .attr("y", margin.top)
                        .attr("height", titleHeight)
                        .attr("width", width + 30 + margin.left)
                        .attr("fill", "none");

                    chart.append("text")
                        .attr("class", "titleName")
                        .attr("x", 40)
                        .attr("y", 37)
                        .text(titleName);

                    return title;

                }

                function createSvgDefs(chart) {

                    var defs = chart.append("defs");

                    defs.append("pattern")
                        .attr("id", "noDataStripes")
                        .attr("patternUnits", "userSpaceOnUse")
                        .attr("x", "0")
                        .attr("y", "0")
                        .attr("width", "6")
                        .attr("height", "3")
                        .append("path")
                        .attr("d", "M 0 0 6 0")
                        .attr("style", "stroke:#CCCCCC; fill:none;");

                    defs.append("pattern")
                        .attr("id", "unknownStripes")
                        .attr("patternUnits", "userSpaceOnUse")
                        .attr("x", "0")
                        .attr("y", "0")
                        .attr("width", "6")
                        .attr("height", "3")
                        .attr("style", "stroke:#2E9EC2; fill:none;")
                        .append("path").attr("d", "M 0 0 6 0");

                    defs.append("pattern")
                        .attr("id", "downStripes")
                        .attr("patternUnits", "userSpaceOnUse")
                        .attr("x", "0")
                        .attr("y", "0")
                        .attr("width", "6")
                        .attr("height", "3")
                        .attr("style", "stroke:#ff8a9a; fill:none;")
                        .append("path").attr("d", "M 0 0 6 0");

                }


                function createStackedBars(lowBound, highBound) {

                    // The gray bars at the bottom leading up
                    svg.selectAll("rect.leaderBar")
                        .data(chartData)
                        .enter().append("rect")
                        .attr("class", "leaderBar")
                        .attr("x", function (d) {
                            return timeScale(d.timestamp);
                        })
                        .attr("y", function (d) {
                            if (!isEmptyDataBar(d)) {
                                return yScale(d.min);
                            }
                            else {
                                return 0;
                            }
                        })
                        .attr("height", function (d) {
                            if (isEmptyDataBar(d)) {
                                return height - yScale(highBound);
                            }
                            else {
                                return height - yScale(d.min);
                            }
                        })
                        .attr("width", function () {
                            return calcBarWidth();
                        })

                        .attr("opacity", ".6")
                        .attr("fill", function (d) {
                            if (isEmptyDataBar(d)) {
                                return "url(#noDataStripes)";
                            }
                            else {
                                return leaderBarColor;
                            }
                        }).on("mouseover", function (d, i) {
                            tip.show(d, i);
                        }).on("mouseout", function () {
                            tip.hide();
                        });


                    // upper portion representing avg to high
                    svg.selectAll("rect.high")
                        .data(chartData)
                        .enter().append("rect")
                        .attr("class", "high")
                        .attr("x", function (d) {
                            return timeScale(d.timestamp);
                        })
                        .attr("y", function (d) {
                            return isNaN(d.max) ? yScale(lowBound) : yScale(d.max);
                        })
                        .attr("height", function (d) {
                            if (isEmptyDataBar(d)) {
                                return 0;
                            }
                            else {
                                return yScale(d.avg) - yScale(d.max);
                            }
                        })
                        .attr("width", function () {
                            return calcBarWidth();
                        })
                        .attr("data-rhq-value", function (d) {
                            return d.max;
                        })
                        .attr("opacity", 0.9)
                        .on("mouseover", function (d, i) {
                            tip.show(d, i);
                        }).on("mouseout", function () {
                            tip.hide();
                        });


                    // lower portion representing avg to low
                    svg.selectAll("rect.low")
                        .data(chartData)
                        .enter().append("rect")
                        .attr("class", "low")
                        .attr("x", function (d) {
                            return timeScale(d.timestamp);
                        })
                        .attr("y", function (d) {
                            return isNaN(d.avg) ? height : yScale(d.avg);
                        })
                        .attr("height", function (d) {
                            if (isEmptyDataBar(d)) {
                                return 0;
                            }
                            else {
                                return yScale(d.min) - yScale(d.avg);
                            }
                        })
                        .attr("width", function () {
                            return calcBarWidth();
                        })
                        .attr("opacity", 0.9)
                        .attr("data-rhq-value", function (d) {
                            return d.min;
                        })
                        .on("mouseover", function (d, i) {
                            tip.show(d, i);
                        }).on("mouseout", function () {
                            tip.hide();
                        });

                    // if high == low put a "cap" on the bar to show raw value, non-aggregated bar
                    svg.selectAll("rect.singleValue")
                        .data(chartData)
                        .enter().append("rect")
                        .attr("class", "singleValue")
                        .attr("x", function (d) {
                            return timeScale(d.timestamp);
                        })
                        .attr("y", function (d) {
                            return isNaN(d.value) ? height : yScale(d.value) - 2;
                        })
                        .attr("height", function (d) {
                            if (isEmptyDataBar(d)) {
                                return 0;
                            }
                            else {
                                if (d.min === d.max) {
                                    return yScale(d.min) - yScale(d.value) + 2;
                                }
                                else {
                                    return 0;
                                }
                            }
                        })
                        .attr("width", function () {
                            return calcBarWidth();
                        })
                        .attr("opacity", 0.9)
                        .attr("data-rhq-value", function (d) {
                            return d.value;
                        })
                        .attr("fill", function (d) {
                            if (d.min === d.max) {
                                return rawValueBarColor;
                            }
                            else {
                                return "#70c4e2";
                            }
                        }).on("mouseover", function (d, i) {
                            tip.show(d, i);
                        }).on("mouseout", function () {
                            tip.hide();
                        });
                }

                function createCandleStickChart() {

                    // upper portion representing avg to high
                    svg.selectAll("rect.candlestick.up")
                        .data(chartData)
                        .enter().append("rect")
                        .attr("class", "candleStickUp")
                        .attr("x", function (d) {
                            return timeScale(d.timestamp);
                        })
                        .attr("y", function (d) {
                            return isNaN(d.max) ? yScale(lowBound) : yScale(d.max);
                        })
                        .attr("height", function (d) {
                            if (isEmptyDataBar(d)) {
                                return 0;
                            }
                            else {
                                return yScale(d.avg) - yScale(d.max);
                            }
                        })
                        .attr("width", function () {
                            return calcBarWidth();
                        })
                        .attr("data-rhq-value", function (d) {
                            return d.max;
                        })
                        .style("fill", function (d, i) {
                            return fillCandleChart(d, i);
                        })

                        .on("mouseover", function (d, i) {
                            tip.show(d, i);
                        }).on("mouseout", function () {
                            tip.hide();
                        });


                    // lower portion representing avg to low
                    svg.selectAll("rect.candlestick.down")
                        .data(chartData)
                        .enter().append("rect")
                        .attr("class", "candleStickDown")
                        .attr("x", function (d) {
                            return timeScale(d.timestamp);
                        })
                        .attr("y", function (d) {
                            return isNaN(d.avg) ? height : yScale(d.avg);
                        })
                        .attr("height", function (d) {
                            if (isEmptyDataBar(d)) {
                                return 0;
                            }
                            else {
                                return yScale(d.min) - yScale(d.avg);
                            }
                        })
                        .attr("width", function () {
                            return calcBarWidth();
                        })
                        .attr("data-rhq-value", function (d) {
                            return d.min;
                        })
                        .style("fill", function (d, i) {
                            return fillCandleChart(d, i);
                        })
                        .on("mouseover", function (d, i) {
                            tip.show(d, i);
                        }).on("mouseout", function () {
                            tip.hide();
                        });

                    function fillCandleChart(d, i) {
                        if (i > 0 && chartData[i].avg > chartData[i - 1].avg) {
                            return "green";
                        } else if (i === 0) {
                            return "none";
                        } else {
                            return "#ff0705";
                        }
                    }

                }

                function createHistogramChart() {
                    var strokeOpacity = "0.6";

                    // upper portion representing avg to high
                    svg.selectAll("rect.histogram")
                        .data(chartData)
                        .enter().append("rect")
                        .attr("class", "histogram")
                        .attr("x", function (d) {
                            return timeScale(d.timestamp);
                        })
                        .attr("width", function () {
                            return calcBarWidth();
                        })
                        .attr("y", function (d) {
                            if (!isEmptyDataBar(d)) {
                                return yScale(d.avg);
                            }
                            else {
                                return 0;
                            }
                        })
                        .attr("height", function (d) {
                            if (isEmptyDataBar(d)) {
                                return height - yScale(highBound);
                            }
                            else {
                                return height - yScale(d.avg);
                            }
                        })
                        .attr("fill", function (d, i) {
                            if (isEmptyDataBar(d)) {
                                return 'url(#noDataStripes)';
                            }
                            else if (i % 5 === 0) {
                                return '#989898';
                            }
                            else {
                                return '#C0C0C0';
                            }
                        })
                        .attr("stroke", function (d) {
                            return '#777';
                        })
                        .attr("stroke-width", function (d) {
                            if (isEmptyDataBar(d)) {
                                return '0';
                            }
                            else {
                                return '0';
                            }
                        })
                        .attr("data-rhq-value", function (d) {
                            return d.avg;
                        }).on("mouseover", function (d, i) {
                            tip.show(d, i);
                        }).on("mouseout", function () {
                            tip.hide();
                        });

                    if (hideHighLowValues === false) {

                        svg.selectAll(".histogram.top.stem")
                            .data(chartData)
                            .enter().append("line")
                            .attr("class", "histogramTopStem")
                            .attr("x1", function (d) {
                                return xStartPosition(d);
                            })
                            .attr("x2", function (d) {
                                return xStartPosition(d);
                            })
                            .attr("y1", function (d) {
                                return yScale(d.max);
                            })
                            .attr("y2", function (d) {
                                return yScale(d.avg);
                            })
                            .attr("stroke", function (d) {
                                return "red";
                            })
                            .attr("stroke-opacity", function (d) {
                                return strokeOpacity;
                            });

                        svg.selectAll(".histogram.bottom.stem")
                            .data(chartData)
                            .enter().append("line")
                            .attr("class", "histogramBottomStem")
                            .attr("x1", function (d) {
                                return xStartPosition(d);
                            })
                            .attr("x2", function (d) {
                                return xStartPosition(d);
                            })
                            .attr("y1", function (d) {
                                return yScale(d.avg);
                            })
                            .attr("y2", function (d) {
                                return yScale(d.min);
                            })
                            .attr("stroke", function (d) {
                                return "red";
                            }).attr("stroke-opacity", function (d) {
                                return strokeOpacity;
                            });

                        svg.selectAll(".histogram.top.cross")
                            .data(chartData)
                            .enter().append("line")
                            .attr("class", "histogramTopCross")
                            .attr("x1", function (d) {
                                return xStartPosition(d) - 3;
                            })
                            .attr("x2", function (d) {
                                return xStartPosition(d) + 3;
                            })
                            .attr("y1", function (d) {
                                return yScale(d.max);
                            })
                            .attr("y2", function (d) {
                                return yScale(d.max);
                            })
                            .attr("stroke", function (d) {
                                return "red";
                            })
                            .attr("stroke-width", function (d) {
                                return "0.5";
                            })
                            .attr("stroke-opacity", function (d) {
                                return strokeOpacity;
                            });

                        svg.selectAll(".histogram.bottom.cross")
                            .data(chartData)
                            .enter().append("line")
                            .attr("class", "histogramBottomCross")
                            .attr("x1", function (d) {
                                return xStartPosition(d) - 3;
                            })
                            .attr("x2", function (d) {
                                return xStartPosition(d) + 3;
                            })
                            .attr("y1", function (d) {
                                return yScale(d.min);
                            })
                            .attr("y2", function (d) {
                                return yScale(d.min);
                            })
                            .attr("stroke", function (d) {
                                return "red";
                            })
                            .attr("stroke-width", function (d) {
                                return "0.5";
                            })
                            .attr("stroke-opacity", function (d) {
                                return strokeOpacity;
                            });

                    }

                }

                function createLineChart() {
                    var avgLine = d3.svg.line()
                            .interpolate("linear")
                            .defined(function (d) {
                                return !d.empty;
                            })
                            .x(function (d) {
                                return xStartPosition(d);
                            })
                            .y(function (d) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.avg);
                            }),
                        highLine = d3.svg.line()
                            .interpolate("linear")
                            .defined(function (d) {
                                return !d.empty;
                            })
                            .x(function (d) {
                                return xStartPosition(d);
                            })
                            .y(function (d) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.max);
                            }),
                        lowLine = d3.svg.line()
                            .interpolate("linear")
                            .defined(function (d) {
                                return !d.empty;
                            })
                            .x(function (d) {
                                return xStartPosition(d);
                            })
                            .y(function (d) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.min);
                            });

                    // Bar avg line
                    svg.append("path")
                        .datum(chartData)
                        .attr("class", "avgLine")
                        .attr("d", avgLine);


                    if (hideHighLowValues === false) {

                        svg.append("path")
                            .datum(chartData)
                            .attr("class", "highLine")
                            .attr("d", highLine);

                        svg.append("path")
                            .datum(chartData)
                            .attr("class", "lowLine")
                            .attr("d", lowLine);
                    }

                }

                function createAreaChart() {
                    var highArea = d3.svg.area()
                            .interpolate("step-before")
                            .defined(function (d) {
                                return !d.empty;
                            })
                            .x(function (d) {
                                return xStartPosition(d);
                            })
                            .y(function (d) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.max);
                            })
                            .y0(function (d) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.avg);
                            }),

                        avgArea = d3.svg.area()
                            .interpolate("step-before")
                            .defined(function (d) {
                                return !d.empty;
                            })
                            .x(function (d) {
                                return xStartPosition(d);
                            })
                            .y(function (d) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.avg);
                            }).
                            y0(function (d) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.min);
                            }),

                        lowArea = d3.svg.area()
                            .interpolate("step-before")
                            .defined(function (d) {
                                return !d.empty;
                            })
                            .x(function (d) {
                                return xStartPosition(d);
                            })
                            .y(function (d) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.min);
                            })
                            .y0(function () {
                                return height;
                            });


                    if (hideHighLowValues === false) {

                        svg.append("path")
                            .datum(chartData)
                            .attr("class", "highArea")
                            .attr("d", highArea);

                        svg.append("path")
                            .datum(chartData)
                            .attr("class", "lowArea")
                            .attr("d", lowArea);
                    }

                    svg.append("path")
                        .datum(chartData)
                        .attr("class", "avgArea")
                        .attr("d", avgArea);

                }

                function createScatterChart() {
                    if (hideHighLowValues === false) {

                        svg.selectAll(".highDot")
                            .data(chartData)
                            .enter().append("circle")
                            .attr("class", "highDot")
                            .attr("r", 3)
                            .attr("cx", function (d) {
                                return xStartPosition(d);
                            })
                            .attr("cy", function (d) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.max);
                            })
                            .style("fill", function () {
                                return "#ff1a13";
                            }).on("mouseover", function (d, i) {
                                tip.show(d, i);
                            }).on("mouseout", function () {
                                tip.hide();
                            });


                        svg.selectAll(".lowDot")
                            .data(chartData)
                            .enter().append("circle")
                            .attr("class", "lowDot")
                            .attr("r", 3)
                            .attr("cx", function (d) {
                                return xStartPosition(d);
                            })
                            .attr("cy", function (d) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.min);
                            })
                            .style("fill", function () {
                                return "#70c4e2";
                            }).on("mouseover", function (d, i) {
                                tip.show(d, i);
                            }).on("mouseout", function () {
                                tip.hide();
                            });
                    }

                    svg.selectAll(".avgDot")
                        .data(chartData)
                        .enter().append("circle")
                        .attr("class", "avgDot")
                        .attr("r", 3)
                        .attr("cx", function (d) {
                            return xStartPosition(d);
                        })
                        .attr("cy", function (d) {
                            return isRawMetric(d) ? yScale(d.value) : yScale(d.avg);
                        })
                        .style("fill", function () {
                            return "#FFF";
                        }).on("mouseover", function (d, i) {
                            tip.show(d, i);
                        }).on("mouseout", function () {
                            tip.hide();
                        });
                }

                function createScatterLineChart() {


                    svg.selectAll(".scatterline.top.stem")
                        .data(chartData)
                        .enter().append("line")
                        .attr("class", "scatterLineTopStem")
                        .attr("x1", function (d) {
                            return xStartPosition(d);
                        })
                        .attr("x2", function (d) {
                            return xStartPosition(d);
                        })
                        .attr("y1", function (d) {
                            return yScale(d.max);
                        })
                        .attr("y2", function (d) {
                            return yScale(d.avg);
                        })
                        .attr("stroke", function (d) {
                            return "#000";
                        });

                    svg.selectAll(".scatterline.bottom.stem")
                        .data(chartData)
                        .enter().append("line")
                        .attr("class", "scatterLineBottomStem")
                        .attr("x1", function (d) {
                            return xStartPosition(d);
                        })
                        .attr("x2", function (d) {
                            return xStartPosition(d);
                        })
                        .attr("y1", function (d) {
                            return yScale(d.avg);
                        })
                        .attr("y2", function (d) {
                            return yScale(d.min);
                        })
                        .attr("stroke", function (d) {
                            return "#000";
                        });

                    svg.selectAll(".scatterline.top.cross")
                        .data(chartData)
                        .enter().append("line")
                        .attr("class", "scatterLineTopCross")
                        .attr("x1", function (d) {
                            return xStartPosition(d) - 3;
                        })
                        .attr("x2", function (d) {
                            return xStartPosition(d) + 3;
                        })
                        .attr("y1", function (d) {
                            return yScale(d.max);
                        })
                        .attr("y2", function (d) {
                            return yScale(d.max);
                        })
                        .attr("stroke", function (d) {
                            return "#000";
                        })
                        .attr("stroke-width", function (d) {
                            return "0.5";
                        });

                    svg.selectAll(".scatterline.bottom.cross")
                        .data(chartData)
                        .enter().append("line")
                        .attr("class", "scatterLineBottomCross")
                        .attr("x1", function (d) {
                            return xStartPosition(d) - 3;
                        })
                        .attr("x2", function (d) {
                            return xStartPosition(d) + 3;
                        })
                        .attr("y1", function (d) {
                            return yScale(d.min);
                        })
                        .attr("y2", function (d) {
                            return yScale(d.min);
                        })
                        .attr("stroke", function (d) {
                            return "#000";
                        })
                        .attr("stroke-width", function (d) {
                            return "0.5";
                        });

                    svg.selectAll(".scatterDot")
                        .data(chartData)
                        .enter().append("circle")
                        .attr("class", "avgDot")
                        .attr("r", 3)
                        .attr("cx", function (d) {
                            return xStartPosition(d);
                        })
                        .attr("cy", function (d) {
                            return isRawMetric(d) ? yScale(d.value) : yScale(d.avg);
                        })
                        .style("fill", function () {
                            return "#70c4e2";
                        })
                        .style("opacity", function () {
                            return "1";
                        }).on("mouseover", function (d, i) {
                            tip.show(d, i);
                        }).on("mouseout", function () {
                            tip.hide();
                        });


                }


                function createYAxisGridLines() {
                    // create the y axis grid lines
                    svg.append("g").classed("grid y_grid", true)
                        .call(d3.svg.axis()
                            .scale(yScale)
                            .orient("left")
                            .ticks(10)
                            .tickSize(-width, 0, 0)
                            .tickFormat("")
                    );
                }

                function createXandYAxes() {
                    var xAxisGroup;

                    svg.selectAll('g.axis').remove();


                    // create x-axis
                    xAxisGroup = svg.append("g")
                        .attr("class", "x axis")
                        .attr("transform", "translate(0," + height + ")")
                        .call(xAxis);

                    xAxisGroup.append("g")
                        .attr("class", "x brush")
                        .call(brush)
                        .selectAll("rect")
                        .attr("y", -6)
                        .attr("height", 30);

                    // create y-axis
                    svg.append("g")
                        .attr("class", "y axis")
                        .call(yAxis)
                        .append("text")
                        .attr("transform", "rotate(-90),translate( -70,-40)")
                        .attr("y", -30)
                        .style("text-anchor", "end")
                        .text(attrs.yAxisUnits === "NONE" ? "" : attrs.yAxisUnits);

                }

                function createCenteredLine(newInterpolation) {
                    var interpolate = newInterpolation || 'monotone',
                        line = d3.svg.line()
                            .interpolate(interpolate)
                            .defined(function (d) {
                                return !d.empty;
                            })
                            .x(function (d) {
                                return timeScale(d.timestamp) + (calcBarWidth() / 2)
                            })
                            .y(function (d) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.avg);
                            });

                    return line;
                }

                function createAvgLines() {
                    svg.append("path")
                        .datum(chartData)
                        .attr("class", "barAvgLine")
                        .attr("d", createCenteredLine("monotone"));
                }


                function createXAxisBrush() {

                    brush = d3.svg.brush()
                        .x(timeScaleForBrush)
                        .on("brushstart", brushStart)
                        .on("brush", brushMove)
                        .on("brushend", brushEnd);

                    //brushGroup = svg.append("g")
                    //    .attr("class", "brush")
                    //    .call(brush);
                    //
                    //brushGroup.selectAll(".resize").append("path");
                    //
                    //brushGroup.selectAll("rect")
                    //    .attr("height", height);

                    function brushStart() {
                        svg.classed("selecting", true);
                    }

                    function brushMove() {
                        //useful for showing the daterange change dynamically while selecting
                        var extent = brush.extent();
                        scope.$emit('DateRangeMove', extent);
                    }

                    function brushEnd() {
                        var extent = brush.extent(),
                            startTime = Math.round(extent[0].getTime()),
                            endTime = Math.round(extent[1].getTime()),
                            dragSelectionDelta = endTime - startTime >= 60000;

                        svg.classed("selecting", !d3.event.target.empty());
                        // ignore range selections less than 1 minute
                        if (dragSelectionDelta) {
                            scope.$emit('DateRangeChanged', extent);
                        }
                    }

                }

                function createPreviousRangeOverlay(prevRangeData) {
                    if (isListDefinedAndHasValues(prevRangeData)) {
                        $log.debug("Running PreviousRangeOverlay");
                        svg.append("path")
                            .datum(prevRangeData)
                            .attr("class", "prevRangeAvgLine")
                            .style("stroke-dasharray", ("9,3"))
                            .attr("d", createCenteredLine("linear"));
                    }

                }

                function createMultiMetricOverlay() {
                    var multiLine,
                        g = 0,
                        colorScale = d3.scale.category20();


                    if (isListDefinedAndHasValues(multiChartOverlayData)) {
                        $log.warn("Running MultiChartOverlay for %i metrics", multiChartOverlayData.length);

                        angular.forEach(multiChartOverlayData, function (singleChartData) {

                            svg.append("path")
                                .datum(singleChartData)
                                .attr("class", "multiLine")
                                .attr("fill", function (d, i) {
                                    return colorScale(i);
                                })
                                .attr("stroke", function (d, i) {
                                    return colorScale(i);
                                })
                                .attr("stroke-width", "1")
                                .attr("stroke-opacity", ".8")
                                .attr("d", createCenteredLine("linear"));
                        });
                        g++;
                    }

                }

                function annotateChart(annotationData) {
                    if (isListDefinedAndHasValues(annotationData)) {
                        svg.selectAll(".annotationDot")
                            .data(annotationData)
                            .enter().append("circle")
                            .attr("class", "annotationDot")
                            .attr("r", 5)
                            .attr("cx", function (d) {
                                return timeScale(d.timestamp);
                            })
                            .attr("cy", function () {
                                return height - yScale(highBound);
                            })
                            .style("fill", function (d) {
                                if (d.severity === '1') {
                                    return "red";
                                } else if (d.severity === '2') {
                                    return "yellow";
                                } else {
                                    return "white";
                                }
                            });
                    }
                }

                function isListDefinedAndHasValues(list) {
                    return angular.isDefined(list) && list.length > 0;
                }

                scope.$watch('data', (newData) => {
                    if (isListDefinedAndHasValues(newData)) {
                        $log.debug('Data Changed');
                        processedNewData = angular.fromJson(newData);
                        scope.render(processedNewData, processedPreviousRangeData);
                    }
                }, true);

                scope.$watch('previousRangeData', (newPreviousRangeValues) =>{
                    if (isListDefinedAndHasValues(newPreviousRangeValues)) {
                        $log.debug("Previous Range data changed");
                        processedPreviousRangeData = angular.fromJson(newPreviousRangeValues);
                        scope.render(processedNewData, processedPreviousRangeData);
                    }
                }, true);

                scope.$watch('annotationData', (newAnnotationData) => {
                    if (isListDefinedAndHasValues(newAnnotationData)) {
                        annotationData = angular.fromJson(newAnnotationData);
                        scope.render(processedNewData, processedPreviousRangeData);
                    }
                }, true);


                scope.$watch('contextData', (newContextData) => {
                    if (isListDefinedAndHasValues(newContextData)) {
                        contextData = angular.fromJson(newContextData);
                        scope.render(processedNewData, processedPreviousRangeData);
                    }
                }, true);

                scope.$on('MultiChartOverlayDataChanged', (event, newMultiChartData) =>{
                    $log.log('Handling MultiChartOverlayDataChanged in Chart Directive');
                    if (angular.isUndefined(newMultiChartData)) {
                        // same event is sent with no data to clear it
                        multiChartOverlayData = [];
                    } else {
                        multiChartOverlayData = angular.fromJson(newMultiChartData);
                    }
                    scope.render(processedNewData, processedPreviousRangeData);
                });


                scope.$watch('chartType', (newChartType) => {
                    if (angular.isDefined(newChartType)) {
                        chartType = newChartType;
                        scope.render(processedNewData, processedPreviousRangeData);
                    }
                });

                function loadMetricsTimeRangeFromNow(){
                    endTimestamp = _.now();
                    startTimestamp = moment().subtract('seconds', timeRangeInSeconds).valueOf();
                    loadMetricsForTimeRange(getBaseUrl(), metricId, startTimestamp, endTimestamp, 60);
                }

                scope.$watch('dataUrl', (newUrlData) => {
                    if (angular.isDefined(newUrlData)) {
                        console.log('dataUrl has changed: '+newUrlData);
                        dataUrl = newUrlData;
                    }
                });

                scope.$watch('metricId', (newMetricId) =>{
                    if (angular.isDefined(newMetricId)) {
                        console.log('metricId has changed: '+newMetricId);
                        metricId = newMetricId;
                        loadMetricsTimeRangeFromNow();
                    }
                });

                scope.$watch('refreshIntervalInSeconds', (newRefreshInterval) => {
                    if (angular.isDefined(newRefreshInterval)) {
                        refreshIntervalInSeconds = +newRefreshInterval;
                        var startIntervalPromise = $interval(() => {
                            loadMetricsTimeRangeFromNow();
                        }, refreshIntervalInSeconds * 1000);
                    }
                });

                scope.$watch('timeRangeInSeconds', (newTimeRange) => {
                    if (angular.isDefined(newTimeRange)) {
                        console.log("timeRangeInSeconds changed.")
                        timeRangeInSeconds = newTimeRange;
                    }
                });




                scope.$watch('showAvgLine', (newShowAvgLine) =>{
                    if (angular.isDefined(newShowAvgLine)) {
                        showAvgLine = newShowAvgLine;
                        scope.render(processedNewData, processedPreviousRangeData);
                    }
                });

                scope.$watch('avgLineColor', (newAvgLineColor) => {
                    if (angular.isDefined(newAvgLineColor)) {
                        avgLineColor = newAvgLineColor;
                        scope.render(processedNewData, processedPreviousRangeData);
                    }
                });

                scope.$watch('hideHighLowValues', (newHideHighLowValues) =>{
                    if (angular.isDefined(newHideHighLowValues)) {
                        hideHighLowValues = newHideHighLowValues;
                        scope.render(processedNewData, processedPreviousRangeData);
                    }
                });

                scope.$on('DateRangeDragChanged', (event, extent) => {
                    $log.debug('Handling DateRangeDragChanged Fired Chart Directive: ' + extent[0] + ' --> ' + extent[1]);
                    scope.$emit('GraphTimeRangeChangedEvent', extent);
                });


                scope.render = (dataPoints, previousRangeDataPoints) => {
                    if (isListDefinedAndHasValues(dataPoints)) {
                        console.group('Render Chart');
                        console.time('chartRender');
                        //NOTE: layering order is important!
                        oneTimeChartSetup();
                        determineScale(dataPoints);
                        createHeader(attrs.chartTitle);
                        createYAxisGridLines();
                        createXAxisBrush();

                        if (chartType === 'bar') {
                            createStackedBars(lowBound, highBound);
                        } else if (chartType === 'histogram') {
                            createHistogramChart();
                        } else if (chartType === 'line') {
                            createLineChart();
                        } else if (chartType === 'area') {
                            createAreaChart();
                        } else if (chartType === 'scatter') {
                            createScatterChart();
                        } else if (chartType === 'scatterline') {
                            createScatterLineChart();
                        } else if (chartType === 'candlestick') {
                            createCandleStickChart();
                        } else {
                            $log.warn('chart-type is not valid. Must be in [bar,area,line,scatter,candlestick,histogram]');
                        }
                        createPreviousRangeOverlay(previousRangeDataPoints);
                        createMultiMetricOverlay();
                        createXandYAxes();
                        showAvgLine = (chartType === 'bar' || chartType === 'scatterline') ? true : false;
                        if (showAvgLine === true) {
                            createAvgLines();
                        }
                        annotateChart(annotationData);
                        console.timeEnd('chartRender');
                        console.groupEnd('Render Chart');
                    }
                };
            }

            return {
                link: link,
                restrict: 'EA',
                replace: true,
                scope: {
                    data: '@',
                    dataUrl: '@',
                    metricId: '@',
                    startTimestamp: '@',
                    endTimestamp: '@',
                    timeRangeInSeconds: '@',
                    refreshIntervalInSeconds: '@',
                    previousRangeData: '@',
                    annotationData: '@',
                    contextData: '@',
                    multiChartOverlayData: '@',
                    chartHeight: '@',
                    chartType: '@',
                    yAxisUnits: '@',
                    buttonbarDatetimeFormat: '@',
                    timeLabel: '@',
                    dateLabel: '@',
                    chartHoverDateFormat: '@',
                    chartHoverTimeFormat: '@',
                    singleValueLabel: '@',
                    noDataLabel: '@',
                    aggregateLabel: '@',
                    startLabel: '@',
                    endLabel: '@',
                    durationLabel: '@',
                    minLabel: '@',
                    maxLabel: '@',
                    avgLabel: '@',
                    timestampLabel: '@',
                    highBarColor: '@',
                    lowBarColor: '@',
                    leaderBarColor: '@',
                    rawValueBarColor: '@',
                    avgLineColor: '@',
                    showAvgLine: '@',
                    hideHighLowValues: '@',
                    chartTitle: '@'
                }
            };
        }]
    );
}
