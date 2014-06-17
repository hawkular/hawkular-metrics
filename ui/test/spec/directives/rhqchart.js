'use strict';

describe('Directive: rhqChart', function () {

  // load the directive's module
  beforeEach(module('rhqApp'));

  var element,
    scope;

  beforeEach(inject(function ($rootScope) {
    scope = $rootScope.$new();
  }));

  it('should make hidden element visible', inject(function ($compile) {
    element = angular.element('<rhq-chart></rhq-chart>');
    element = $compile(element)(scope);
    expect(element.text()).toBe('this is the rhqChart directive');
  }));
});
