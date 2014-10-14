'use strict';

describe('Controller: ChartControllerCtrl', function () {

    // load the controller's module
    beforeEach(module('chartingApp'));

    var ChartControllerCtrl,
        scope;

    // Initialize the controller and a mock scope
    beforeEach(inject(function ($controller, $rootScope) {
        scope = $rootScope.$new();
        ChartControllerCtrl = $controller('ChartController', {
            $scope: scope
        });
    }));

    it('should have this variable in scope', function () {
        expect(scope.vm.dateTimeRanges).toBeDefined();
    });
});
