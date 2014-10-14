'use strict';

// Based off the Red Hat, angular-patternfly stuff: https://github.com/patternfly/angular-patternfly

module.exports = function(grunt) {

  require('matchdep').filterDev('grunt-*').forEach(grunt.loadNpmTasks);

  function init() {

    grunt.initConfig({
      availabletasks: {
        tasks: {
          options: {
            descriptions: {
              'help': 'Task list helper for your Grunt enabled projects.',
              'clean': 'Deletes the content of the dist directory.',
              'build': 'Builds the project (including documentation) into the dist directory. You can specify modules to be built as arguments (' +
                'grunt build:buttons:notification) otherwise all available modules are built.',
              'test': 'Executes the karma testsuite.',
              'watch': 'Whenever js source files (from the src directory) change, the tasks executes jshint and documentation build.',
              'ngdocs': 'Builds documentation into dist/docs.',
              'ngdocs:view': 'Builds documentation into dist/docs and runs a web server. The docs can be accessed on http://localhost:8000/'
            },
            groups: {
              'Basic project tasks': ['help', 'clean', 'build', 'test'],
              'Documentation tasks': ['ngdocs', 'ngdocs:view']
            }
          }
        }
      },
      clean: {
        docs: ['dist/docs'],
        templates: ['templates/'],
        all: ['dist/*']
      },
      concat: {
        options: {
          separator: ';'
        },
        dist: {
          src: ['src/**/*.module.js', 'src/**/*.js', 'templates/*.js'],
          dest: 'dist/rhqm-charts.js'
        }
      },
      connect: {
        docs: {
          options: {
            base: 'dist/docs'
          }
        }
      },
      copy: {
        docdata: {
          cwd: 'lib/rhqm-charts/dist',
          src: ['fonts/*', 'img/*'],
          dest: 'dist/docs',
          expand: true
        },
        fa: {
          cwd: 'lib/rhqm-charts/',
          src: ['components/font-awesome/**'],
          dest: 'dist/docs',
          expand: true
        }
      },
      htmlhint: {
        html: {
          src: ['src/**/*.html'],
          options: {
            htmlhintrc: '.htmlhintrc'
          }
        }
      },
      jshint: {
        files: ['Gruntfile.js', 'src/**/*.js'],
        options: {
          jshintrc: '.jshintrc'
        },
        beforeconcat: {
          options: {
            force: true,
            ignores: ['**.min.js']
          },
          files: {
            src: 'src/**/*.js'
          }
        }
      },
      karma: {
        unit: {
          configFile: 'test/karma.conf.js',
          singleRun: true,
          browsers: ['PhantomJS']
        }
      },
//      ngdocs: {
//        options: {
//          title: 'RHQ Metrics Documentation',
//          dest: 'dist/docs',
//          scripts: ['lib/rhqm-charts/components/jquery/jquery.js',
//            'lib/rhqm-charts/components/bootstrap/dist/js/bootstrap.js',
//            'angular.js',
//            'dist/rhqm-charts.js',
//            'lib/rhqm-charts/dist/js/rhqm-charts.js'],
//          html5Mode: false,
//          styles: ['lib/rhqm-charts/dist/css/rhqm-charts.css']
//        },
//        all: ['src/**/*.js']
//      },
//      ngtemplates: {
//        options: {
//          htmlmin: {
//            collapseBooleanAttributes:      true,
//            collapseWhitespace:             true,
//            removeAttributeQuotes:          true,
//            removeComments:                 false,
//            removeEmptyAttributes:          true,
//            removeRedundantAttributes:      true,
//            removeScriptTypeAttributes:     true,
//            removeStyleLinkTypeAttributes:  true
//          }
//        }
//
      //},
      uglify: {
        options: {
          mangle: false
        },
        build: {
          files: {},
          src: 'dist/rhqm-charts.js',
          dest: 'dist/rhqm-charts.min.js'
        }
      },
      watch: {
        main: {
          files: ['Gruntfile.js'],
          tasks: ['jshint']
        },
        js: {
          files: ['Gruntfile.js', 'src/**/*.js'],
          tasks: ['build']
        }
      }
    });

    // You can specify which modules to build as arguments of the build task.
    grunt.registerTask('build', 'Create bootstrap build files', function() {
      var concatSrc = [];

      if (this.args.length) {
        this.args.forEach(function(file) {
          if (grunt.file.exists('./src/' + file)) {
            grunt.log.ok('Adding ' + file + ' to the build queue.');
            concatSrc.push('src/' + file + '/*.js');
          } else {
            grunt.fail.warn('Unable to build module \'' + file + '\'. The module doesn\'t exist.');
          }
        });

      } else {
        concatSrc = 'src/**/*.js';
      }

      //grunt.task.run(['clean', 'jshint:beforeconcat', 'lint', 'test', 'ngtemplates', 'concat', 'uglify:build', 'ngdocs', 'copy', 'clean:templates']);
        grunt.task.run(['clean',  'concat', 'uglify:build',  'copy', 'clean:templates']);
    });

    grunt.registerTask('default', ['build']);
    //grunt.registerTask('ngdocs:view', ['build', 'connect:docs', 'watch']);
    grunt.registerTask('lint', ['jshint', 'htmlhint']);
    grunt.registerTask('test', ['karma']);
    grunt.registerTask('help', ['availabletasks']);

  }

  init({});

};
