'use strict';


/**
 * @ngdoc directive
 * @name rhqmStackedBarChart
 * @description A d3 stacked bar chart representing time intervals.
 *
 */
angular.module('rhqm.directives')
    .directive('rhqmStackedBarChart', function () {
        var chartTypes = ['bar', 'line', 'area'];

        function link(scope, element, attributes) {

            var dataPoints,
                previousRangeDataPoints,
                chartHeight = +attributes.chartHeight || 250,
                chartType = attributes.chartType || "bar",
                timeLabel = attributes.timeLabel || "Time",
                dateLabel = attributes.dateLabel || "Date",
                singleValueLabel = attributes.singleValueLabel || "Raw Value",
                noDataLabel = attributes.noDataLabel || "No Data",
                aggregateLabel = attributes.aggregateLabel || "Aggregate",
                startLabel = attributes.startLabel || "Start",
                endLabel = attributes.endLabel || "End",
                durationLabel = attributes.durationLabel || "Duration",
                minLabel = attributes.minLabel || "Min",
                maxLabel = attributes.maxLabel || "Max",
                avgLabel = attributes.avgLabel || "Avg",
                timestampLabel = attributes.timestampLabel || "Timestamp",
                highBarColor = attributes.highBarColor || "#1794bc",
                lowBarColor = attributes.lowBarColor || "#70c4e2",
                leaderBarColor = attributes.leaderBarColor || "#d3d3d6",
                rawValueBarColor = attributes.rawValueBarColor || "#50505a",
                avgLineColor = attributes.avgLineColor || "#2e376a",
                chartHoverDateFormat = attributes.chartHoverDateFormat || "%m/%d/%y",
                chartHoverTimeFormat = attributes.chartHoverTimeFormat || "%I:%M:%S %p",
                allowDragDateSelections = attributes.allowDragDateSelections || true,
                buttonBarDateTimeFormat = attributes.buttonbarDatetimeFormat || "MM/DD/YYYY h:mm a";

            // chart specific vars
            var margin = {top: 10, right: 5, bottom: 5, left: 90},
                contextMargin = {top: 150, right: 5, bottom: 5, left: 90},
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
                chart,
                chartParent,
                context,
                svg,
                lowBound,
                highBound,
                avg,
                peak,
                min,
                processedNewData,
                processedPreviousRangeData;

            dataPoints = attributes.data;
            previousRangeDataPoints = attributes.previousRangeData;


            function getChartWidth() {
                //return angular.element("#" + chartContext.chartHandle).width();
                return 760;
            }

            function useSmallCharts() {
                return  getChartWidth() <= smallChartThresholdInPixels;
            }


            function oneTimeChartSetup() {
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
                    .html(function (d) {
                        return buildHover(d);
                    });

                svg = chart.append("g")
                    .attr("width", width + margin.left + margin.right)
                    .attr("height", innerChartHeight)
                    .attr("transform", "translate(" + margin.left + "," + (adjustedChartHeight2) + ")");

                context = svg.append("g")
                    .attr("class", "context")
                    .attr("width", width + margin.left + margin.right)
                    .attr("height", chartHeight)
                    .attr("transform", "translate(" + contextMargin.left + "," + (adjustedChartHeight2 + 90) + ")");

                svg.call(tip);

            }


            function setupFilteredData(dataPoints) {
                avg = d3.mean(dataPoints.map(function (d) {
                    return !d.empty ? d.avg : 0;
                }));

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
                        .domain(d3.extent(chartData, function (d) {
                            return d.timestamp;
                        }));

                    timeScaleForBrush = d3.time.scale()
                        .range([0, width])
                        .domain(d3.extent(chartData, function (d) {
                            return d.timestamp;
                        }));

                    xAxis = d3.svg.axis()
                        .scale(timeScale)
                        .ticks(xTicks)
                        .tickSubdivide(xTickSubDivide)
                        .tickSize(4, 4, 0)
                        .orient("bottom");

                }

            }

            function isEmptyDataBar(d) {
                return  d.empty;
            }

            function isRawMetric(d) {
                return  d.value;
            }


            function buildHover(d) {
                var hover,
                    formattedDateTime = moment(d.timestamp).format(buttonBarDateTimeFormat);

                if (isEmptyDataBar(d)) {
                    // nodata
                    hover = "<div class='chartHover'><small class='chartHoverLabel'>" + noDataLabel + "</small><hr/>" +
                        "<div><small><span class='chartHoverLabel'>Timestamp: </span><span class='chartHoverValue'>" + formattedDateTime + "</span></small></div></div>";
                } else {
                    if (isRawMetric(d)) {
                        // raw single value from raw table
                        hover = "<div class='chartHover'><div><small><span class='chartHoverLabel'>Timestamp: </span><span class='chartHoverValue'>" + formattedDateTime + "</span></small></div><hr/>" +
                            "<div><small><span class='chartHoverLabel'>" + singleValueLabel + "</span><span>: </span><span class='chartHoverValue'>" + d.value + "</span></small> </div></div> ";
                    } else {
                        // aggregate with min/avg/max
                        hover = "<div class='chartHover'><div><small><span class='chartHoverLabel'>Timestamp: </span><span class='chartHoverValue'>" + formattedDateTime + "</span></small></div><hr/>" +
                            "<div><small><span class='chartHoverLabel'>" + maxLabel + "</span><span>: </span><span class='chartHoverValue'>" + d.max + "</span></small> </div> " +
                            "<div><small><span class='chartHoverLabel'>" + avgLabel + "</span><span>: </span><span class='chartHoverValue'>" + d.avg + "</span></small> </div> " +
                            "<div><small><span class='chartHoverLabel'>" + minLabel + "</span><span>: </span><span class='chartHoverValue'>" + d.min + "</span></small> </div></div> ";
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
                    .text(titleName)

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
                var pixelsOffHeight = 0;

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
                            return height - yScale(highBound) - pixelsOffHeight;
                        }
                        else {
                            return height - yScale(d.min) - pixelsOffHeight;
                        }
                    })
                    .attr("width", function () {
                        return  calcBarWidth();
                    })

                    .attr("opacity", ".6")
                    .attr("fill", function (d) {
                        if (isEmptyDataBar(d)) {
                            return  "url(#noDataStripes)";
                        }
                        else {
                            return  leaderBarColor;
                        }
                    }).on("mouseover", function (d) {
                        tip.show(d);
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
                            return  yScale(d.avg) - yScale(d.max);
                        }
                    })
                    .attr("width", function () {
                        return  calcBarWidth();
                    })
                    .attr("data-rhq-value", function (d) {
                        return d.avg;
                    })
                    .attr("opacity", 0.9)
                    .on("mouseover", function (d) {
                        tip.show(d);
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
                            return  yScale(d.min) - yScale(d.avg);
                        }
                    })
                    .attr("width", function () {
                        return  calcBarWidth();
                    })
                    .attr("opacity", 0.9)
                    .on("mouseover", function (d) {
                        tip.show(d);
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
                                return  yScale(d.min) - yScale(d.value) + 2;
                            }
                            else {
                                return  0;
                            }
                        }
                    })
                    .attr("width", function () {
                        return  calcBarWidth();
                    })
                    .attr("opacity", 0.9)
                    .attr("fill", function (d) {
                        if (d.min === d.max) {
                            return  rawValueBarColor;
                        }
                        else {
                            return  "#70c4e2";
                        }
                    }).on("mouseover", function (d) {
                        tip.show(d);
                    }).on("mouseout", function () {
                        tip.hide();
                    });
            }

            function createLineChart() {
                var avgLine = d3.svg.line()
                        .interpolate("linear")
                        .defined(function (d) {
                            return !d.empty;
                        })
                        .x(function (d) {
                            return timeScale(d.timestamp) + (calcBarWidth() / 2);
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
                            return timeScale(d.timestamp) + (calcBarWidth() / 2);
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
                            return timeScale(d.timestamp) + (calcBarWidth() / 2);
                        })
                        .y(function (d) {
                            return isRawMetric(d) ? yScale(d.value) : yScale(d.min);
                        });

                // Bar avg line
                svg.append("path")
                    .datum(chartData)
                    .attr("class", "avgLine")
                    .attr("d", avgLine);

                svg.append("path")
                    .datum(chartData)
                    .attr("class", "highLine")
                    .attr("d", highLine);

                svg.append("path")
                    .datum(chartData)
                    .attr("class", "lowLine")
                    .attr("d", lowLine);

            }

            function createAreaChart() {
                var highArea = d3.svg.area()
                        .interpolate("linear")
                        .defined(function (d) {
                            return !d.empty;
                        })
                        .x(function (d) {
                            return timeScale(d.timestamp) + (calcBarWidth() / 2);
                        })
                        .y(function (d) {
                            return isRawMetric(d) ? yScale(d.value) : yScale(d.max);
                        })
                        .y0(function (d) {
                            return isRawMetric(d) ? yScale(d.value) : yScale(d.avg);
                        }),

                    avgArea = d3.svg.area()
                        .interpolate("linear")
                        .defined(function (d) {
                            return !d.empty;
                        })
                        .x(function (d) {
                            return timeScale(d.timestamp) + (calcBarWidth() / 2);
                        })
                        .y(function (d) {
                            return isRawMetric(d) ? yScale(d.value) : yScale(d.avg);
                        }).
                        y0(function (d) {
                            return isRawMetric(d) ? yScale(d.value) : yScale(d.min);
                        }),

                    lowArea = d3.svg.area()
                        .interpolate("linear")
                        .defined(function (d) {
                            return !d.empty;
                        })
                        .x(function (d) {
                            return timeScale(d.timestamp) + (calcBarWidth() / 2);
                        })
                        .y(function (d) {
                            return isRawMetric(d) ? yScale(d.value) : yScale(d.min);
                        })
                        .y0(function (d) {
                            return height;
                        });

                // Bar avg line
                svg.append("path")
                    .datum(chartData)
                    .attr("class", "avgArea")
                    .attr("d", avgArea);

                svg.append("path")
                    .datum(chartData)
                    .attr("class", "highArea")
                    .attr("d", highArea);

                svg.append("path")
                    .datum(chartData)
                    .attr("class", "lowArea")
                    .attr("d", lowArea);

            }

            function createScatterChart() {
                svg.selectAll(".highDot")
                    .data(chartData)
                    .enter().append("circle")
                    .attr("class", "highDot")
                    .attr("r", 3)
                    .attr("cx", function (d) {
                        return timeScale(d.timestamp) + (calcBarWidth() / 2);
                    })
                    .attr("cy", function (d) {
                        return isRawMetric(d) ? yScale(d.value) : yScale(d.max);
                    })
                    .style("fill", function (d) {
                        return "#ff1a13";
                    });

                svg.selectAll(".avgDot")
                    .data(chartData)
                    .enter().append("circle")
                    .attr("class", "avgDot")
                    .attr("r", 3)
                    .attr("cx", function (d) {
                        return timeScale(d.timestamp) + (calcBarWidth() / 2);
                    })
                    .attr("cy", function (d) {
                        return isRawMetric(d) ? yScale(d.value) : yScale(d.avg);
                    })
                    .style("fill", function (d) {
                        return "#FFF";
                    });

                svg.selectAll(".lowDot")
                    .data(chartData)
                    .enter().append("circle")
                    .attr("class", "lowDot")
                    .attr("r", 3)
                    .attr("cx", function (d) {
                        return timeScale(d.timestamp) + (calcBarWidth() / 2);
                    })
                    .attr("cy", function (d) {
                        return isRawMetric(d) ? yScale(d.value) : yScale(d.min);
                    })
                    .style("fill", function (d) {
                        return "#70c4e2";
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
                    .text(attributes.yAxisUnits === "NONE" ? "" : attributes.yAxisUnits);

            }

            function createAvgLines() {
                var showBarAvgTrendline = true,
                    barAvgLine = d3.svg.line()
                        .interpolate("linear")
                        .defined(function (d) {
                            return !d.empty;
                        })
                        .x(function (d) {
                            return timeScale(d.timestamp) + (calcBarWidth() / 2);
                        })
                        .y(function (d) {
                            if (showBarAvgTrendline) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.avg);
                            }
                            else {
                                return NaN;
                            }
                        });

                // Bar avg line
                svg.append("path")
                    .datum(chartData)
                    .attr("class", "barAvgLine")
                    .attr("d", barAvgLine);

            }


            function createXAxisBrush() {

                brush = d3.svg.brush()
                    .x(timeScaleForBrush)
                    .on("brushstart", brushStart)
                    .on("brush", brushMove)
                    .on("brushend", brushEnd);

                brushGroup = svg.append("g")
                    .attr("class", "brush")
                    .call(brush);

                brushGroup.selectAll(".resize").append("path");

                brushGroup.selectAll("rect")
                    .attr("height", height);

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
                console.debug("Running PreviousRangeOverlay");
                var showBarAvgTrendline = true,
                    prevRangeLine = d3.svg.line()
                        .interpolate("linear")
                        .defined(function (d) {
                            return !d.empty;
                        })
                        .x(function (d) {
                            return timeScale(d.timestamp) + (calcBarWidth() / 2);
                        })
                        .y(function (d) {
                            if (showBarAvgTrendline) {
                                return isRawMetric(d) ? yScale(d.value) : yScale(d.avg);
                            }
                            else {
                                return NaN;
                            }
                        });

                // Bar avg line
                svg.append("path")
                    .datum(prevRangeData)
                    .attr("class", "prevRangeAvgLine")
                    .style("stroke-dasharray", ("9,3"))
                    .attr("d", prevRangeLine);

            }

            scope.$watch('data', function (newData) {
                if (angular.isDefined(newData) && newData.length > 0) {
                    processedNewData = angular.fromJson(newData);
                    return scope.render(processedNewData, processedPreviousRangeData);
                }
            }, true);

            scope.$watch('previousRangeData', function (newPreviousRangeValues) {
                console.debug("Previous Range data changed");
                if (angular.isDefined(newPreviousRangeValues) && newPreviousRangeValues.length > 0) {
                    processedPreviousRangeData = angular.fromJson(newPreviousRangeValues);
                    console.debug("ReRender with Prev Range data");
                    scope.render(processedNewData, processedPreviousRangeData);
                }
            }, true);

            scope.$watch('chartType', function (newData) {
                if (angular.isDefined(newData) && newData.length > 0) {
                    chartType = newData;
                    scope.render(processedNewData, processedPreviousRangeData);
                }
            });

            scope.$on("DateRangeChanged", function (event, extent) {
                console.debug("Handling DateRangeChanged Fired Chart Directive: " + extent[0] + " --> " + extent[1]);
                var dataSubset = [], dataPoints = angular.fromJson(attributes.data);
                angular.forEach(dataPoints, function (value) {
                    if (value.timestamp >= extent[0].getTime() && value.timestamp <= extent[1].getTime()) {
                        dataSubset.push(value);
                    }
                });
                scope.render(dataSubset, null);
            });


            scope.render = function (dataPoints, previousRangeDataPoints) {
                console.debug("Render");
                if (angular.isDefined(dataPoints)) {
                    oneTimeChartSetup();
                    determineScale(dataPoints);
                    createHeader(attributes.chartTitle);
                    createYAxisGridLines();
                    if (allowDragDateSelections) {
                        console.debug("Allowing DragDateSelections");
                        createXAxisBrush();
                    }

                    if (chartType === 'bar') {
                        createStackedBars(lowBound, highBound);
                    } else if (chartType === 'line') {
                        createLineChart();
                    } else if (chartType === 'area') {
                        createAreaChart();
                    } else if (chartType === 'scatter') {
                        createScatterChart();
                    } else {
                        console.warn("chart-type is not valid. Must be in [bar,area,line]");
                    }

                    if (angular.isDefined(previousRangeDataPoints)) {
                        createPreviousRangeOverlay(previousRangeDataPoints);
                    }
                    createXandYAxes();
                    createAvgLines();
                }
            };
        }

        return {
            link: link,
            restrict: 'EA',
            replace: true,
            scope: {
                data: '@',
                previousRangeData: '@',
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
                allowDragDateSelections: '@',
                chartTitle: '@'}
        };
    }
)
;
