var pgLoader = require(__dirname + "/../lib/pg-loader.js"),
    async = require("async"),
    fs = require("fs"),
    pg = require("pg"),
    stream = require("stream");

// Database configuration parsing that is common across tests
// You must create this for the test to work!
var dbConfFile = __dirname + "/../.pgpass",
    dbUrl;
(function() {
    var config = (fs.readFileSync(dbConfFile, { encoding : "ascii" })).trim().split(":"),
    hostname = config[0],
    port = config[1],
    database = config[2],
    username = config[3],
    password = config[4];
    dbUrl = "postgres://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database;
})();

// Some helper functions:
var generateCreateSql = function(tableName) {
    return "CREATE TABLE " + tableName + " (a INTEGER, b INTEGER, PRIMARY KEY(a))";
};

var generateDropSql = function(tableName) {
    return "DROP TABLE " + tableName;
};

var generateInsertSql = function(tableName, data) {
    var dataStr = [];
    for(var i=0; i<data.length; i++) {
        dataStr.push("(" + data[i].join(",") + ")");
    }
    dataStr = dataStr.join(",");
    return "INSERT INTO " + tableName + " (a,b) VALUES " + dataStr;
};

var generateCompareSql = function(table1, table2) {
    return "(SELECT a,b FROM " + table1 + " EXCEPT SELECT a,b FROM " + table2 + ")" +
        " UNION ALL " +
        "(SELECT a,b FROM " + table2 + " EXCEPT SELECT a,b FROM " + table1 + ")";
};

var generatePgLoaderOptions = function(tableName, sqlMode) {
    return {
        "dbUrl" : dbUrl, 
        "tableName" : tableName,
        "insertFields" : ['a','b'],
        "keyFields" : ['a'],
        "sqlMode" : sqlMode
    };
};

// Disconnects upon completion of all tests
var numTests = 2,
    disconnectCount = 0,
    disconnect = function() {
        disconnectCount += 1;
        if( disconnectCount >= numTests) {
            pg.end();
        }
    };


exports.testRun = function(test) {
    test.expect(3); 
    pg.connect(dbUrl, function(err, client, done) {
        test.ifError(err);

        // Create reference tables
        var refTable = "test_" + ("0000000000" + Math.floor(10000000000*Math.random())).slice(-10),
            testTable = "test_" + ("0000000000" + Math.floor(10000000000*Math.random())).slice(-10); 

        // Query function that binds "this" to the current client
        var _query = function(sql, callback) {
            // console.log("Executing query: %s", sql); // Debugging
            client.query.call(client, sql, callback);
        };


        // Now line up the sequence of queries to establish the reference table, and the test table
        var tasks = {
            "create_ref" : async.apply(_query, generateCreateSql(refTable)),
            "insert_ref" : async.apply(_query, generateInsertSql(refTable, [[1,2],[2,3],[3,1]])),
            "create_test" : async.apply(_query, generateCreateSql(testTable)),
            "load_test_1" : async.apply(pgLoader.run, [{'b':3,'a':2},{'b':1,'a':1}], generatePgLoaderOptions(testTable)),
            "load_test_2" : async.apply(pgLoader.run, [{'b':4,'a':2},{'b':1,'a':3}], generatePgLoaderOptions(testTable, pgLoader.SQL_IGNORE)),
            "load_test_3" : async.apply(pgLoader.run, [{'b':2,'a':1}], generatePgLoaderOptions(testTable, pgLoader.SQL_REPLACE)),
            "compare" : async.apply(_query, generateCompareSql(refTable, testTable)),
            "drop_ref" : async.apply(_query, generateDropSql(refTable)),
            "drop_test" : async.apply(_query, generateDropSql(testTable))
        };

        async.series(tasks, function(err, result) {
            test.ifError(err);
            var compareResult = result.compare.rows;
            test.equal(compareResult.length, 0, "Received non-empty difference from reference, with " + compareResult.length + " rows.");
            done();
            test.done();
            disconnect();
        });
    });
};

// NOTE: Depends on the successful running of the parse method
exports.testStream = function(test) {
    test.expect(4);
    pg.connect(dbUrl, function(err, client, done) {
        test.ifError(err);

        // Create reference tables
        var refTable = "test_" + ("0000000000" + Math.floor(10000000000*Math.random())).slice(-10),
            testTable = "test_" + ("0000000000" + Math.floor(10000000000*Math.random())).slice(-10); 

        // Query function that binds "this" to the current client
        var _query = function(sql, callback) {
            // console.log("Executing query: %s", sql); // Debugging
            client.query.call(client, sql, callback);
        };


        // Now line up the sequence of queries to establish the reference table, and the test table
        var tasks = {
            "create_ref" : async.apply(_query, generateCreateSql(refTable)),
            "insert_ref" : async.apply(_query, generateInsertSql(refTable, [[1,2],[2,4],[3,1]])),
            "create_test" : async.apply(_query, generateCreateSql(testTable)),            
        };

        async.series(tasks, function(err, result) { // Now stream the required data to the test table
            test.ifError(err);

            // Define the read stream
            var rs = new stream.Readable({ objectMode : true }),
                streamData = [
                    [{'b':3,'a':2},{'b':1,'a':1}],
                    [{'b':4,'a':2},{'b':1,'a':3}],
                    [{'b':2,'a':1}]
                ];
            rs._read = function() {
                if( streamData.length > 0 ) {
                    this.push(streamData.shift());
                } else {
                    this.push(null);
                }
            };

            // Setup piping
            var sm = rs.pipe(pgLoader.createStream(generatePgLoaderOptions(testTable, pgLoader.SQL_REPLACE)));
            sm.on("write", function() {

            });
            sm.on("finish", function() {
                var endTasks = {
                    "compare" : async.apply(_query, generateCompareSql(refTable, testTable)),
                    "drop_ref" : async.apply(_query, generateDropSql(refTable)),
                    "drop_test" : async.apply(_query, generateDropSql(testTable))
                };

                async.series(endTasks, function(err, result) {
                    test.ifError(err);
                    var compareResult = result.compare.rows;
                    test.equal(compareResult.length, 0, "Received non-empty difference from reference, with " + compareResult.length + " rows.");
                    done();
                    test.done();
                    disconnect();
                });
            });
        });
    });
};