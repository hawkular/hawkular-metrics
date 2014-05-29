var rhq = {

drawRawGraph: function (metric, divId, clusterValues) {
    clusterValues = clusterValues || false;

    var url = metric.href + '.json';
    if (clusterValues) {
        url += "?buckets=60&bucketWidthSeconds=60&skipEmpty=true&bucketCluster=false"
    }

    d3.json(url,
            function (jsondata) {
                var svg = d3.select("body").select("#" + divId).select("svg");
                var w = svg.attr("width");
                var h = svg.attr("height");

                var points = jsondata.sort(function (a, b) {
                    return b.timestamp - a.timestamp
                });
                var minVal = d3.min(jsondata, function (d) {
                    return d.value
                });
                var maxVal = d3.max(jsondata, function (d) {
                    return d.value
                });
                var minTs = d3.min(jsondata, function (d) {
                    return d.timestamp
                });
                var maxTs = d3.max(jsondata, function (d) {
                    return d.timestamp
                });

                var minTsD = new Date(minTs);
                var maxTsD = new Date(maxTs);

                // X axis goes from lowest to highest timestamp
                var x = d3.time.scale().domain([minTsD, maxTsD]).range([80, w - 5]);
                // Y axis goes from lowest to highest value
                var y = d3.scale.linear().domain([minVal, maxVal]).rangeRound([5, h - 20]);
                var yAxisRange = d3.scale.linear().domain([maxVal, minVal]).rangeRound([5, h - 20]);

                var dataFormat = d3.format(".2r");
                var dateFormat;
                if (!clusterValues) {
                    dateFormat = d3.time.format("%X"); // only h:m:s at the moment
                } else {
                    dateFormat = d3.time.format("%M"); // only minutes
                }

                // Remove old stuff -- we should perhaps animate that
                svg.selectAll("g").remove();
                svg.selectAll("path").remove();
                svg.selectAll("line").remove();

                // Prepare axes
                var xAxis = d3.svg.axis()
                                .scale(x)
                                .tickSize(1, 1)
                                .tickSubdivide(true)
                                .orient("bottom")
                        ;

                var yAxis = d3.svg.axis()
                                .scale(yAxisRange)
                                .tickSize(1, 1)
                                .ticks(4)
                                .orient("right")
                                .tickFormat(d3.format(".2r"))
                                .tickSubdivide(true)
                        ;
                svg.append("g")
                        .attr("class", "xaxis")
                        .call(xAxis)

                ;
                svg.append("g")
                        .attr("class", "yaxis")
                        .call(yAxis)
                ;

                // data() loops over all entries in 'points' above for the remaining statements
                var bars = svg.selectAll("bars").data(points);

                var currX = function (d) {
                    return x(new Date(d.timestamp))
                };

                if (!clusterValues) {
                    var linesFunction = d3.svg.line()
                            .x(function (d, i) {
                                return x(new Date(d.timestamp));
                            })
                            .y(function (d) {
                                return  h - y(d.value);
                            })
                            .interpolate("linear");

                    svg.append("path")
                            .attr("d", linesFunction(points))
                            .attr("stroke", "lightblue")
                            .attr("stroke-width", ".5")
                            .attr("stroke-dasharray", "2,2")
                            .attr("stroke-linecap", "round")
                            .attr("fill", "none");
                }


                var group = bars.enter().append("svg:g");

                group.append("svg:circle")
                        .attr("cx", currX)
                        .attr("cy", function(d) {
                            return h - y(d.value)
                        })
                        .attr("r", 3)
                        .attr("stroke", "blue")
                        .attr("fill", "lightblue")
                        .append("title")
                        .text(function(d) {
                            return dataFormat(d.value) + " @ " + dateFormat(new Date(d.timestamp))
                        });

            });
},

drawWhisker :function(metric,divId) {

    d3.json(metric.href +'.json' + "?buckets=60&skipEmpty=true",
            function (jsondata) {
                var svg = d3.select("body").select("#"+divId).select("svg");
                var w = svg.attr("width");
                var h = svg.attr("height");

                var points = jsondata.sort(function(a,b) { return b.timestamp - a.timestamp});
                var minVal = d3.min(jsondata, function(d) {return d.min});
                var maxVal = d3.max(jsondata, function(d) {return d.max});
                var minTs = d3.min(jsondata, function(d) {return d.timestamp});
                var maxTs = d3.max(jsondata, function(d) {return d.timestamp});

                var minTsD = new Date(minTs);
                var maxTsD = new Date(maxTs);


                // X axis goes from lowest to highest timestamp
                var x = d3.time.scale().domain([minTsD,maxTsD]).range([80,w-5]);
                // Y axis goes from lowest to highest value
                var y = d3.scale.linear().domain([maxVal, minVal]).rangeRound([5,h-20]);


                // Remove old stuff -- we should perhaps animate that
                svg.selectAll("g").remove();
                svg.selectAll("path").remove();
                svg.selectAll("line").remove();


                // Prepare axes
                var xAxis = d3.svg.axis()
                        .scale(x)
                        .tickSize(1,1)
                        .tickSubdivide(true)
                        .orient("bottom")
                        ;

                var yAxis = d3.svg.axis()
                                .scale(y)
                                .tickSize(1,1)
                                .ticks(4)
                                .orient("right")
                                .tickFormat(d3.format(".2r"))
                                .tickSubdivide(true)
                ;
                svg.append("g")
                        .attr("class","xaxis")
                        .call(xAxis)

                ;
                svg.append("g")
                        .attr("class","yaxis")
                        .call(yAxis)
                ;



                // data() loops over all entries in 'points' above for the remaining statements
                var bars = svg.selectAll("bars").data(points);

                var currX = function(d) {
                    return x(new Date(d.timestamp))
                };

                var group = bars.enter().append("svg:g");

                var line = group.append("svg:line")
                        .attr("x1", currX)
                        .attr("x2", currX)
                        .attr("y1", function(d) {
                            return y(d.min)
                        })
                        .attr("y2", function(d) {
                            return y(d.max)
                        })
                        .attr("stroke-width",2)
                        .attr("stroke", "lightblue");

                group.append("svg:circle")
                        .attr("cx", currX)
                        .attr("cy", function(d) {
                            return y(d.min)
                        })
                        .attr("r", 2)
                        .attr("stroke", "blue")
                        .attr("fill", "lightblue");

                group.append("svg:circle")
                        .attr("cx", currX)
                        .attr("cy", function(d) {
                            return y(d.max)
                        })
                        .attr("r", 2)
                        .attr("stroke", "blue")
                        .attr("fill", "lightblue");

                group.append("svg:circle")
                        .attr("cx", currX)
                        .attr("cy", function(d) {
                            return y(d.avg)
                        })
                        .attr("r", 2.5)
                        .attr("stroke", "blue")
                        .attr("fill", "lightpink");

            });

},

    // This is similar to above, but the data is clustered by hour
    // so with 60 buckets and a bucket size of 60seconds
    // data from 8:01a and 9:01a end up in the same bucket
drawWhisker2 :function(metric,divId) {

    d3.json(metric.href +'.json' + "?buckets=60&bucketWidthSeconds=60&skipEmpty=true",
            function (jsondata) {
                var svg = d3.select("body").select("#"+divId).select("svg");
                var w = svg.attr("width");
                var h = svg.attr("height");

                var points = jsondata.sort(function(a,b) { return b.timestamp - a.timestamp});
                var minVal = d3.min(jsondata, function(d) {return d.min});
                var maxVal = d3.max(jsondata, function(d) {return d.max});
                var minTs = d3.min(jsondata, function(d) {return d.timestamp});
                var maxTs = d3.max(jsondata, function(d) {return d.timestamp});

                var minTsD = new Date(minTs);
                var maxTsD = new Date(maxTs);



                // X axis goes from lowest to highest timestamp
                var x = d3.time.scale().domain([minTsD,maxTsD]).range([80,w-5]);
                // Y axis goes from lowest to highest value
                var y = d3.scale.linear().domain([maxVal, minVal]).rangeRound([5,h-20]);


                // Remove old stuff -- we should perhaps animate that
                svg.selectAll("g").remove();
                svg.selectAll("path").remove();
                svg.selectAll("line").remove();


                // Prepare axes
                var xAxis = d3.svg.axis()
                        .scale(x)
                        .tickSize(1,1)
                        .tickSubdivide(true)
                        .orient("bottom")
                        ;

                var yAxis = d3.svg.axis()
                                .scale(y)
                                .tickSize(1,1)
                                .ticks(4)
                                .orient("right")
                                .tickFormat(d3.format(".2r"))
                                .tickSubdivide(true)
                ;
                svg.append("g")
                        .attr("class","xaxis")
                        .call(xAxis)

                ;
                svg.append("g")
                        .attr("class","yaxis")
                        .call(yAxis)
                ;



                // data() loops over all entries in 'points' above for the remaining statements
                var bars = svg.selectAll("bars").data(points);

                var currX = function(d) {
                    return x(new Date(d.timestamp))
                };

                var group = bars.enter().append("svg:g");

                var line = group.append("svg:line")
                        .attr("x1", currX)
                        .attr("x2", currX)
                        .attr("y1", function(d) {
                            return y(d.min)
                        })
                        .attr("y2", function(d) {
                            return y(d.max)
                        })
                        .attr("stroke-width",2)
                        .attr("stroke", "lightblue");

                group.append("svg:circle")
                        .attr("cx", currX)
                        .attr("cy", function(d) {
                            return y(d.min)
                        })
                        .attr("r", 2)
                        .attr("stroke", "blue")
                        .attr("fill", "lightblue");

                group.append("svg:circle")
                        .attr("cx", currX)
                        .attr("cy", function(d) {
                            return y(d.max)
                        })
                        .attr("r", 2)
                        .attr("stroke", "blue")
                        .attr("fill", "lightblue");

                group.append("svg:circle")
                        .attr("cx", currX)
                        .attr("cy", function(d) {
                            return y(d.avg)
                        })
                        .attr("r", 2.5)
                        .attr("stroke", "blue")
                        .attr("fill", "lightpink");

            });

}
};//var rhq
