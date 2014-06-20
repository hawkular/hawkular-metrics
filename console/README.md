# RHQ-Metrics UI Module

## What does it do?
The UI provides an easy UI way of inserting and viewing data in either rhq-metrics datastore: Cassandra or In-Memory.

## Purpose
This UI is a stripped down version of the forthcoming  metrics UI that will provide dashboards and insights into metric data patterns. This data explorer allows to easily insert and view data put into the rhq-metric datastore. The focus on simplicity provides an example to easily incorporate the angular directives into other applications without the complexity of a full blown management UI
(and understanding its many accompanying libraries). Although stripped down, it is still very functional at populating a datastore and viewing its results graphically as well as a learning tool for working with the *rhq-metrics*.

_NOTE: The graphical chart components used here will be the same ones used in other more advanced scenarios._

Stay tuned for future tutorials based on this data explorer.

## Install
The standard **mvn install** will work.

## Dev Install
This is a standard Grunt project generated from yeoman. 

1. Install [Node.js](http://nodejs.org)  (and npm) if not already installed
2. Install the node packages: **'npm install -g grunt-cli bower karma'**
3. Install npm packages for tooling: **'npm install'**
4. Install [Bower](http://bower.io) packages for UI: **'bower install'**
5. Start the [Grunt](http://gruntjs.com) Server: **'grunt serve'**
6. Browse [http://127.0.0.1:9000/](http://127.0.0.1:9000/) and explore...

Grunt will automatically launch a url for you in your browser and will reload any changes in real time to the browser. Feel Free to play around.

## Javascript API documentation

. To generate the jsdoc documentation for the project: **'grunt ngdocs'**

. To view documentation run **'grunt ngdocs:view'** and peruse documentation at [http://localhost:8000/](http://localhost:8000/)

