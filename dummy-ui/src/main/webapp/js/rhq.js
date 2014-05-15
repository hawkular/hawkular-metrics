var rhq = {

drawGraph: function(metric,divId) {

    d3.json(metric.href +'.json',
            function (jsondata) {
                var svg = d3.select("body").select("#"+divId).select("svg");
                var w = svg.attr("width");
                var h = svg.attr("height");

                var points = jsondata;
                var minVal = d3.min(jsondata, function(d) {return d.value});
                var maxVal = d3.max(jsondata, function(d) {return d.value});
                var minTs = d3.min(jsondata, function(d) {return d.timestamp});
                var maxTs = d3.max(jsondata, function(d) {return d.timestamp});

                var minTsD = new Date(minTs);
                var maxTsD = new Date(maxTs);


                // X axis goes from lowest to highest timestamp
                var x = d3.time.scale().domain([minTsD,maxTsD]).range([80,w-3]);
                // Y axis goes from lowest to highest value
                var y = d3.scale.linear().domain([minVal, maxVal]).rangeRound([5,h-20]);
                var yAxisRange = d3.scale.linear().domain([maxVal, minVal]).rangeRound([5,h-20]);

                var dataFormat = d3.format(".2r");
                var dateFormat = d3.time.format("%X"); // only h:m:s at the moment

                // Remove old stuff -- we should perhaps animate that
                svg.selectAll("g").remove();


                // Prepare axes
                var xAxis = d3.svg.axis()
                        .scale(x)
                        .tickSize(1,1)
                        .tickSubdivide(true)
                        .orient("bottom")
                        ;

                var yAxis = d3.svg.axis()
                                .scale(yAxisRange)
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
} // whisker

};//var rhq
