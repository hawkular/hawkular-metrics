# AngularJS directives for [PatternFly](https://www.patternfly.org)

This project will provide a set of common AngularJS directives for use with the PatternFly reference implementation.

## Getting started

You have to install required software before you're able to use grunt:

* Install Node.js - Find more information on [Node.js](http://nodejs.org/)
* Install npm - If npm is not already installed with Node.js, you have to install it manually. Find more information on [NPM](https://www.npmjs.org/)
* Install Bower globally - Find more information on [Bower](http://bower.io/)

        npm install -g bower
* Install Grunt globally - Find more information on [Grunt](http://gruntjs.com/)

        npm install -g grunt-cli
* Install npm dependencies with:

        npm install
* Install bower dependencies with:

        bower install

You should have your environment ready now.

Angular-PatternFly can now be built with:
```shell
grunt build
```

To see all the grunt tasks that are available:
```shell
grunt help
```

## API documentation

The API documentation can be built with:
```shell
grunt ngdocs
```

If you're interested in reading the docs right away, you can use special target, which will start a web server:
```shell
grunt ngdocs:view
```

After executing this tasks you'll be able to access the documentation at [http://localhost:8000/](http://localhost:8000/).

## Contributing

We're always interested in contributions from the community.

Please ensure that your PR provides the following:

* Detailed description of the proposed changes
* Follows the style rules for [javascript](.jshintrc) and [html](.htmlhintrc).
* Rebased onto the latest master commit
