/*
 * Copyright 2014-2015 Red Hat, Inc. and/or its affiliates
 * and other contributors as indicated by the @author tags.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * Copyright 2014 Red Hat, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

module.exports = function (grunt) {
    'use strict';

    require('load-grunt-tasks')(grunt);
    require('time-grunt')(grunt);

    grunt.loadNpmTasks('grunt-typescript');


    // Define the configuration for all the tasks
    grunt.initConfig({

        // Project settings
        rhqMetrics: {
            // configurable paths
            app: './',
            dist: '../../../target/dist'
        },

        typescript: {
            base: {
                src: [ 'vendor/**/*.d.ts', 'scripts/**/*.ts' ],
                dest: '<%= rhqMetrics.dist %>',
                options: {
                    removeComments: true,
                    target: 'ES5',
                    declaration: false,
                    sourceMap: true,
                    ignoreError: true,
                    indentStep: 5,
                    references: ['vendor/**/*.d.ts'],
                    watch: false
                }
            },
            dev: {
                src: [ 'vendor/**/*.d.ts', 'scripts/**/*.ts'  ],
                options: {
                    removeComments: true,
                    target: 'ES5',
                    declaration: false,
                    sourceMap: true,
                    noEmitOnError: true,
                    indentStep: 5,
                    references: ['vendor/**/*.d.ts'],
                    watch: grunt.option('watch') ? {
                        path: 'scripts',
                        atBegin: true
                    } : false
                }
            }
        },

        // Watches files for changes and runs tasks based on the changed files
        watch: {
            js: {
                files: ['<%= rhqMetrics.app %>/scripts/{,*/}*.js'],
                //tasks: ['newer:jshint:all'],
                options: {
                    livereload: true
                }
            },
            jsTest: {
                files: ['test/spec/{,*/}*.js'],
                tasks: ['newer:jshint:test', 'karma']
            },
            styles: {
                files: ['<%= rhqMetrics.app %>/css/{,*/}*.css'],
                tasks: ['newer:copy:styles', 'autoprefixer']
            },
            gruntfile: {
                files: ['Gruntfile.js']
            },
            livereload: {
                options: {
                    livereload: '<%= connect.options.livereload %>'
                },
                files: [
                    '<%= rhqMetrics.app %>/{,*/}*.html',
                    '.tmp/css/{,*/}*.css',
                    '<%= rhqMetrics.app %>/images/{,*/}*.{png,jpg,jpeg,gif,webp,svg}'
                ]
            }
        },

        // The actual grunt server settings
        connect: {
            options: {
                port: 9000,
                // Change this to '0.0.0.0' to access the server from outside.
                hostname: 'localhost',
                livereload: 35729
            },

            livereload: {
                options: {
                    open: true,
                    base: [
                        '.tmp',
                        '<%= rhqMetrics.app %>'
                    ]
                }
            },
            test: {
                options: {
                    port: 9001,
                    base: [
                        '.tmp',
                        'test',
                        '<%= rhqMetrics.app %>'
                    ]
                }
            },
            dist: {
                options: {
                    base: '<%= rhqMetrics.dist %>'
                }
            }
        },

        // Make sure code styles are up to par and there are no obvious mistakes
        jshint: {
            options: {
                jshintrc: '.jshintrc',
                reporter: require('jshint-stylish')
            },
            all: [
                'Gruntfile.js'
            ],
            test: {
                options: {
                    jshintrc: 'test/.jshintrc'
                },
                src: ['test/spec/{,*/}*.js']
            }
        },

        // Empties folders to start fresh
        clean: {
            dist: {
                files: [
                    {
                        dot: true,
                        src: [
                            '.tmp',
                            '<%= rhqMetrics.dist %>/*',
                            '!<%= rhqMetrics.dist %>/.git*'
                        ]
                    }
                ],
                options: {
                    force: true
                }
            },
            server: '.tmp'
        },

        // Add vendor prefixed styles
        autoprefixer: {
            options: {
                browsers: ['last 1 version']
            },
            dist: {
                files: [
                    {
                        expand: true,
                        cwd: '.tmp/css/',
                        src: '{,*/}*.css',
                        dest: '.tmp/css/'
                    }
                ]
            }
        },

        // Automatically inject Bower components into the app
        'bower-install': {
            app: {
                html: '<%= rhqMetrics.app %>/index.html',
                ignorePath: '<%= rhqMetrics.app %>/'
            }
        },


        bower: {
            install: {
                options: {
                    targetDir: './bower_components'
                }
            }
        },

        // Renames files for browser caching purposes
        rev: {
            dist: {
                files: {
                    src: [
                        '<%= rhqMetrics.dist %>/scripts/{,*/}*.js',
                        '<%= rhqMetrics.dist %>/css/{,*/}*.css',
                        '<%= rhqMetrics.dist %>/images/{,*/}*.{png,jpg,jpeg,gif,webp,svg}',
                        '<%= rhqMetrics.dist %>/css/fonts/*'
                    ]
                }
            }
        },


        // Copies remaining files to places other tasks can use
        copy: {
            dist: {
                files: [
                    {
                        expand: true,
                        dot: true,
                        cwd: '<%= rhqMetrics.app %>',
                        dest: '<%= rhqMetrics.dist %>',
                        src: [
                            '*.{ico,png,txt}',
                            '.htaccess',
                            '*.html',
                            'views/{,*/}*.html',
                            'bower_components/**/*',
                            'images/{,*/}*.{webp}',
                            'fonts/*',
                            'scripts/**/*',
                            'css/**/*',
                            'img/**/*',
                            'WEB-INF/*',
                            '!**/.*'
                        ]
                    },
                    {
                        expand: true,
                        cwd: '.tmp/images',
                        dest: '<%= rhqMetrics.dist %>/images',
                        src: ['generated/*']
                    }
                ]
            },
            styles: {
                expand: true,
                cwd: '<%= rhqMetrics.app %>/css',
                dest: '.tmp/css/',
                src: '{,*/}*.css'
            }
        },

        // Run some tasks in parallel to speed up the build process
        concurrent: {
            server: [
                'copy:styles'
            ],
            test: [
                'copy:styles'
            ],
            dist: [
                'copy:styles',
            ]
        },


        // Test settings
        karma: {
            unit: {
                configFile: 'karma.conf.js',
                singleRun: true
            }
        }
    });


    grunt.registerTask('serve', function (target) {
        if (target === 'dist') {
            return grunt.task.run(['build', 'connect:dist:keepalive']);
        }

        grunt.task.run([
            'clean:server',
            'bower-install',
            'concurrent:server',
            'autoprefixer',
            'connect:livereload',
            'typescript:dev',
            'watch'
        ]);
    });

    grunt.registerTask('ts', ['typescript:base']);

    grunt.registerTask('test', [
        'clean:server',
        'concurrent:test',
        'autoprefixer',
        'connect:test',
        'karma'
    ]);

    grunt.registerTask('build', [
        'clean:dist',
        'bower:install',
        'typescript:base',
        'concurrent:dist',
        'autoprefixer',
        'copy:dist',
    ]);

    grunt.registerTask('default', [
        //'newer:jshint',
        //'test',
        'build'
    ]);
};
