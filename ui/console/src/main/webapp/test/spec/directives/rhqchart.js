'use strict';

describe('Directive: rhqChart', function () {

  // load the directive's module
    beforeEach(module('chartingApp'));

  var element,
    scope;

  beforeEach(inject(function ($rootScope) {
    scope = $rootScope.$new();
  }));

    it('should have a chart element visible', inject(function ($compile) {
        element = angular.element('<rhqm-chart></rhqm-chart>');
    element = $compile(element)(scope);
        //expect(element.text()).toBeTruthy('<svg>');
  }));
});
