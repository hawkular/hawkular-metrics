'use strict';

describe('Controller: ChartControllerCtrl', function () {

    // load the controller's module
    beforeEach(module('chartingApp'));

    var ChartControllerCtrl,
        scope;

    // Initialize the controller and a mock scope
    beforeEach(inject(function ($controller, $rootScope) {
        scope = $rootScope.$new();
        ChartControllerCtrl = $controller('ChartControllerCtrl', {
            $scope: scope
        });
    }));

    it('should attach a list of awesomeThings to the scope', function () {
        expect(scope.awesomeThings.length).toBe(3);
    });
});
