function resourceTree(resourceId,anchor) {

var r = 960 / 2;

 var tree = d3.layout.tree()
 .size([360, r - 120])
 .separation(function(a, b) { return (a.parent == b.parent ? 1 : 2) / a.depth; });

 var diagonal = d3.svg.diagonal.radial()
 .projection(function(d) { return [d.y, d.x / 180 * Math.PI]; });

 var vis = d3.select("#"+anchor).append("svg:svg")
 .attr("width", r * 2)
 .attr("height", r * 2 - 150)
   .append("svg:g")
 .attr("transform", "translate(" + r + "," + r + ")");

// d3.json("/rest/resource/10702/hierarchy.json", function(json) {
 d3.json("/rest/resource/" + resourceId + "/hierarchy.json", function(json) {
   var nodes = tree.nodes(json);

   var link = vis.selectAll("path.link")
   .data(tree.links(nodes))
 .enter().append("svg:path")
   .attr("class", "link")
   .attr("d", diagonal);

   var node = vis.selectAll("g.node")
   .data(nodes)
 .enter().append("svg:g")
   .attr("class", "node")
   .attr("transform", function(d) { return "rotate(" + (d.x - 90) + ")translate(" + d.y + ")"; });

   node.append("svg:circle")
   .attr("r",  function(d) {
           if (d.depth==0) return 10 ;
           else if (d.depth==1) return 8;
           else return 5
       })
   .style("stroke", function(d){
           if (d.depth==0) return "darkblue";
           else if (d.depth==1) return "steelblue";
           else return "lightblue"
       })
   .style("stroke-width", function(d){
           if (d.depth==0) return 3;
           else if (d.depth==1) return 2;
           else return 1.5
       });

//   var hyperLink = node.append("a") // TODO works in adding to tree, but
//     .attr("xlink:href",function(d) { return "/rest/resource/" + d.id; });

   node.append("svg:text")
   .attr("dx", function(d) { return d.x < 180 ? 8 : -8; })
   .attr("dy", ".31em")
   .attr("text-anchor", function(d) { return d.x < 180 ? "start" : "end"; })
   .attr("transform", function(d) { return d.x < 180 ? null : "rotate(180)"; })
   .text(function(d) { return d.name; })
   .on("mousedown",function(d) { window.location= "/rest/resource/" + d.id; })
   .on("mouseover", function() {d3.select(this).style("fill","red").style("font-size","20px");})
   .on("mouseout", function() {d3.select(this).style("fill","black").style("font-size","10px");});

 });
}