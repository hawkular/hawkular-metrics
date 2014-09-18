'use strict';


module.exports = function (grunt) {


    grunt.loadNpmTasks("grunt-ts");

      // Configure grunt here
      grunt.initConfig({

          ts: {
              dev: {                          // a particular target
                  src: ['./scripts/**/*.ts'], // The source typescript files, http://gruntjs.com/configuring-tasks#files
                  reference: './scripts/reference.ts',  // If specified, generate this file that you can use for your reference management
                  watch: 'scripts',
                  options: {
                     target: 'es5'
                  }
              },
          }
      });


  grunt.registerTask("default", ["ts:dev"]);


};
