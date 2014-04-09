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
var numTests = 3,
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

// Does not depend on parser, but it should be, ultimately, correct
exports.testTradeStream = function(test) {
    test.expect(15);
    pg.connect(dbUrl, function(err, client, done) {
        test.ifError(err);
        //console.log("Error object is as follows: %o", err);

        var testTable = "test_" + ("0000000000" + Math.floor(10000000000*Math.random())).slice(-10); 

        //console.log("Client object is as follows: %o", client);

        async.series([
            async.apply(client.query.bind(client), "CREATE TABLE " + testTable + " " +
                "(exchange VARCHAR(50), currency CHAR(3), " +
                "time TIMESTAMP WITH TIME ZONE, price DECIMAL, volume DECIMAL, cnt INTEGER, " +
                "PRIMARY KEY(exchange, currency, time, price, volume))"),
            async.apply(client.query.bind(client), "INSERT INTO " + testTable + " " +
                "VALUES ('localbtc', 'PLN', '2013-07-16T00:34:50+00' ,'349.000000000000', '0.110000000000', 5)")
        ], function(createErr, result) {
            test.ifError(createErr);

            var loadOptions = {
                dbUrl : dbUrl,
                symbol : 'localbtcPLN',
                tableName : testTable,
                fileFields : ['time', 'price', 'volume'],
                symbolFields : ['exchange', 'currency'],
                countField : ['cnt']
            };

            pgLoader.createTradeStream(loadOptions, function(err, pgcfStream) {
                    //console.log("Error object is as follows: %o", createErr);
                    test.ifError(err);
                    fs.createReadStream(__dirname + "/read-files/localbtcPLN.csv").pipe(pgcfStream);            
                }, function(err) {
                    test.ifError(err);

                    async.series([
                        async.apply(client.query.bind(client),"SELECT exchange, currency, time, price, volume, cnt " + 
                            "FROM " + testTable + " ORDER BY 3 ASC LIMIT 1"),
                        async.apply(client.query.bind(client),"SELECT exchange, currency, time, price, volume, cnt " + 
                            "FROM " + testTable + " ORDER BY 3 DESC LIMIT 1"),
                        async.apply(client.query.bind(client),"SELECT COUNT(*) as \"count\" FROM " + testTable),
                        async.apply(client.query.bind(client),"SELECT SUM(cnt) as \"count_lines\" FROM " + testTable),
                        async.apply(client.query.bind(client),"DROP TABLE " + testTable)
                    ], function(err, result) {
                        test.ifError(err);
                        done();
                        test.equal(result[0].rows[0].time.toString(), (new Date("2013-06-24T16:56:22+00:00")).toString(), "Date on first line doesn't match expectations");
                        test.equal(result[0].rows[0].price, "371.170000000000", "Price on first line doesn't match expectations");
                        test.equal(result[0].rows[0].volume, "0.269400000000", "Volume on first line doesn't match expectations");
                        test.equal(result[0].rows[0].cnt, "3", "Count on first line doesn't match expectations");
                        test.equal(result[1].rows[0].time.toString(), (new Date("2014-03-18T08:42:20+00:00")).toString(), "Date on last line doesn't match expectations");
                        test.equal(result[1].rows[0].price, "1828.360000000000", "Price on last line doesn't match expectations");
                        test.equal(result[1].rows[0].volume, "1.367300000000", "Volume on last line doesn't match expectations");
                        test.equal(result[1].rows[0].cnt, "1", "Count on last line doesn't match expectations");
                        test.equal(result[2].rows[0].count, 478, "Count of total rows in tables doesn't match expectations.");
                        test.equal(result[3].rows[0].count_lines, 494, "Count of total lines in file doesn't match expectations.");
                        test.done();
                        disconnect();
                    });
                }
            );
        });
    });
};