var tablesApp = angular.module('tablesApp', [
    'ui.bootstrap',
    'ngRoute',
    'angularSpinner',
    'ajoslin.promise-tracker',
    'tablesControllers']);


tablesApp.config(['$routeProvider',
  function($routeProvider) {
    $routeProvider.
      when('/:tablespace', {
        templateUrl: 'partials/tablespace-tables.html',
        controller: 'TablesCtrl'
      }).
      when('/', {
        templateUrl: 'partials/tablespace-tables.html',
        controller: 'TablesCtrl'
      }).
      otherwise({
        redirectTo: function (routeParams, path, search) {
        return '/';
      }
      });
  }]);

  tablesApp.filter('array', function() {
    return function(items) {
      var filtered = [];
      angular.forEach(items, function(item) {
        filtered.push(item);
      });
     return filtered;
    };
  });