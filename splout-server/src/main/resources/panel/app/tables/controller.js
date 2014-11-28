var tablesControllers = angular.module('tablesControllers', [
    'angularSpinner',
    'ajoslin.promise-tracker'
]);

tablesControllers.controller('TablesCtrl', ['$scope', '$http', '$routeParams', '$location', 'promiseTracker',
    function ($scope, $http, $routeParams, $location, promiseTracker) {
        //Create a new tracker
        $scope.loadingTracker = promiseTracker();

        $http.get("/api/tablespaces", {tracker: $scope.loadingTracker}).success(function (data) {
            $scope.tablespaces = data;
        });

        $scope.$watch('tablespace', function (tablespace) {
            $location.path(tablespace);
        });
        $scope.$watch(function () {
            return $routeParams.tablespace;
        }, function (tablespace) {
            $scope.tablespace = tablespace;
        });

        tables = {};
        if ($routeParams.tablespace) {
            $http.get("/api/query/" +
                    encodeURIComponent($routeParams.tablespace) +
                    "?key=''&sql=" + encodeURIComponent("SELECT name, tbl_name, type, sql FROM sqlite_master WHERE type='table' ORDER BY tbl_name")
                , {tracker: $scope.loadingTracker}).success(function (data) {
                    angular.forEach(data.result, function (table, idx) {
                        table.indexes = {};
                        tables[table.tbl_name] = table;

                        $http.get("/api/query/" +
                                encodeURIComponent($routeParams.tablespace) +
                                "?key=''&sql=" + encodeURIComponent("PRAGMA table_info(" + table.tbl_name + ")"), {tracker: $scope.loadingTracker}
                        ).success(function (data) {
                                tables[table.tbl_name].tableInfo = data.result;
                            });
                    });
                });

            indexes = {};
            $http.get("/api/query/" +
                    encodeURIComponent($routeParams.tablespace) +
                    "?key=''&sql=" + encodeURIComponent("SELECT name, tbl_name, type, sql FROM sqlite_master WHERE type='index'")
                , {tracker: $scope.loadingTracker}).success(function (data) {
                    angular.forEach(data.result, function (index, idx) {
                        $http.get("/api/query/" +
                                encodeURIComponent($routeParams.tablespace) +
                                "?key=''&sql=" + encodeURIComponent("PRAGMA index_info(" + index.name + ")"),
                            {tracker: $scope.loadingTracker}).success(function (data) {
                                tables[index.tbl_name].indexes[index.name] = data.result;
                            });
                    });
                });


            $scope.tables = tables;
        }
    }]);