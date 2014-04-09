// Requirements
var async = require("async"),
    debug = require("debug")("bitcoin-charts-pg-loader"),
    pg = require("pg"),
    pgCopyFrom = require("pg-copy-streams").from,
    sql = require("sql"),
    stream = require("stream"),
    util = require("util");

// Helper function for formatting javascript date-time object in postgres friendly format
var formatTime = function(time) {
    return time.toISOString();
};

// Constants indicating the insertion mode
exports.SQL_INSERT=0;
exports.SQL_IGNORE=1;
exports.SQL_REPLACE=2;

// Impelmentation of Java's hashcode function
// From: http://werxltd.com/wp/2010/05/13/javascript-implementation-of-javas-string-hashcode-method/
var hashCode = function(str){
    var hash = 0;
    if (str.length === 0) return hash;
    for (i = 0; i < str.length; i++) {
        char = str.charCodeAt(i);
        hash = ((hash<<5)-hash)+char;
        hash = hash & hash; // Convert to 32bit integer
    }
    return hash;
};

// Generate queries to load data in the database; Normally this should be used only for debugging purposes
exports.generateSQL = function(tableName, insertFields, keyFields, data, sqlMode) {
    var tempTableName = tableName + '_staging_' + hashCode(Math.floor((Math.random()*1000000)+1).toString()),
        tempTable = sql.define({ name : tempTableName, columns: insertFields}),
        table = sql.define({ name : tableName, columns: insertFields}),
        insertFieldsString = "\"" + insertFields.join("\", \"") + "\"",
        keyFieldsString = "\"" + keyFields.join("\", \"") + "\"",
        queries = [];

    queries.push("BEGIN TRANSACTION"); // 1. Start transaction
    queries.push("CREATE TABLE " + tempTableName + " AS SELECT " + insertFieldsString + // 2a. Create temp table
        " FROM " + tableName + " WHERE NULL");
    queries.push("ALTER TABLE " + tempTableName + " ADD PRIMARY KEY (" + keyFieldsString + ")"); // 2b. Add primary key to temp table
    queries.push(tempTable.insert(data).toString()); // 3. Format and insert data into temp table
    if( sqlMode == exports.SQL_REPLACE && insertFields.filter(function(i) {return (keyFields.indexOf(i) < 0);}).length > 0 ) { // 4. If replacing, update duplicate keys with the latest values
        queries.push((function() {
            var updateFields = insertFields.filter(function(i) {return (keyFields.indexOf(i) < 0);}), 
                updateAssignments = {},
                whereConditions = [],
                sqlQuery, i, j;
            
            // Establish assignments for SQL parser
            for (i=0; i < updateFields.length; i++) {
                updateAssignments[updateFields[i]] = tempTable[updateFields[i]];
            }

            // Establish where clause bindings for SQL parser
            for (j=0; j < keyFields.length; j++) {
                whereConditions.push(table[keyFields[j]].equals(tempTable[keyFields[j]]));
            }

            sqlQuery = table.update(updateAssignments)
                .from(tempTable);
            sqlQuery = sqlQuery.where.apply(sqlQuery, whereConditions);

            return sqlQuery.toString();
        })()); 
    }
    queries.push((function() { // 5. Execute Insert / Select with an appropriate clause for ignore option and execute
        var onConditions = table[keyFields[0]].equals(tempTable[keyFields[0]]),
            sqlQuery;

        // Establish on clause bindings for SQL parser
        for (var j=1; j < keyFields.length; j++) {
            onConditions = onConditions.and(table[keyFields[j]].equals(tempTable[keyFields[j]]));
        }

        // Build SQL Select Query for new insert values
        sqlQuery = tempTable.select()
            .from(tempTable.leftJoin(table).on(onConditions));
        if (sqlMode == exports.SQL_IGNORE || sqlMode == exports.SQL_REPLACE) {
            sqlQuery = sqlQuery.where(table[keyFields[0]].isNull());
        }

        // Return the query string
        return "INSERT INTO " + tableName + " (" + insertFieldsString + ") (" + sqlQuery.toString() + ")";
    })());
    queries.push("DROP TABLE " + tempTableName); // 6. Drop the temporary table
    queries.push("COMMIT"); // 7. Commit transaction

    return queries;
};

// Load data into the database (note that this will not create the table)
// Note: This supports MySQL style REPLACE and INSERT IGNORE options by first loading data into staging table and then into the final table
exports.run = function(data, options, callback) {

    var dbUrl = options.dbUrl,
        tableName = options.tableName,
        insertFields = options.insertFields,
        keyFields = options.keyFields,
        sqlMode = options.sqlMode;

    // Open postgres sql connection
    pg.connect(dbUrl, function(err, client, done) { // Connect!
        if(err) {
            console.log("Could not connect to database.");
            callback(err);
        }

        // Generate SQL
        sqlQueries = exports.generateSQL(tableName, insertFields, keyFields, data, sqlMode);
        
        // Query function that binds "this" to the current client
        var _query = function(sql, callback) {
            // console.log("Executing query: %s", sql); // Debugging
            client.query.call(client, sql, callback);
        };

        // Execute
        async.series(sqlQueries.map(function(sql){ return async.apply(_query, sql); }), function(err, results) {
            if( err ) {
                console.log("Encountered the following error: %s", err.message);
                console.log("Executing DB rollback...");
                client.query("ROLLBACK", function( anotherErr, result ) {
                    if( anotherErr ) {
                        console.log("Oh no, I couldn't rollback the transaction. You might want to check for open transactions.");
                        callback(anotherErr);
                    } else {
                        console.log("DB rollback complete.");
                        done(); // Return connection to the pool
                        callback(err);
                    }    
                });
            }
            else {
                done(); // Return connection to the pool
                callback(null, true);
            }
        });

    });
};

// Streaming version of run()
// Works on chunks of JSON stringified version input to run (of the sort returned by parser::TradeFormatter)
util.inherits(Stream, stream.Writable);
function Stream(dbLoadOptions) {
    stream.Writable.call(this, { objectMode : true });
    this._dbLoadOptions = dbLoadOptions;
    this._batchCount = 0; // Tacker for the number of batches loaded
}
Stream.prototype._write = function(chunk, encoding, callback) {
    var self = this;
    exports.run(chunk, self._dbLoadOptions, function(err, result) {
        self._batchCount += 1;
        if( err ) {
            console.log("Error loading batch %s into database, First line %s", self._batchCount, JSON.stringify(chunk[0]));
            // console.log("Length of chunk is %s", chunk.length);
            // console.log("Head of chunk is as follows: %o", chunk.slice(0,3));
            // console.log("Tail of chunk is as follows: %o", chunk.slice(-3));
            callback(err);
        }
        else {
            debug("Finished loading batch %s into database. First line %s", self._batchCount, JSON.stringify(chunk[0]));
            callback(null, true);
        }
    });
};
// Exported method for creation
exports.createStream = function(dbLoadOptions) {
    return new Stream(dbLoadOptions);
};

// Stream specific to trades file
exports.createTradeStream = function(loadOptions, callback, finishCallback) {

    var dbUrl = loadOptions.dbUrl,
        tableName = loadOptions.tableName,
        tempTableName = tableName + '_staging_' + hashCode(Math.floor((Math.random()*1000000)+1).toString()),
        symbol = loadOptions.symbol,
        exchange = symbol.slice(0,-3),
        currency = symbol.slice(-3), 
        fileFields = loadOptions.fileFields,
        fileFieldsStr = "\"" + fileFields.join("\",\"") + "\"",
        symbolFields = loadOptions.symbolFields,
        symbolFieldsStr = "\"" + symbolFields.join("\",\"") + "\"",
        countField = loadOptions.countField,
        countFieldStr = "\"" + countField + "\"",
        pgcf;

    // DB connection & Stream establishment
    pg.connect(dbUrl, function(err, client, done) { // Connect!
        if(err) {
            callback(err);
        }

        // Setup rollback handler
        var rollback = function(cb) {
            // Rollback, Walmart style
            console.log("Encountered an error while loading trades data into database.");
            console.log("Executing DB rollback...");
            client.query("ROLLBACK", function( anotherErr, result ) {
                if( anotherErr ) {
                    console.log("Oh no, I couldn't rollback the transaction. You might want to check for open transactions.");
                    if( cb ) { cb(anotherErr); }
                } else {
                    console.log("DB rollback complete.");
                    done(); // Return connection to the pool
                    if( cb ) { cb(err); }
                }    
            });
        };

        // Setup stream builder
        var createTradeStreamCore = function(cb) {
            pgcf = client.query(pgCopyFrom("COPY " + tempTableName + " FROM STDIN WITH (FORMAT csv)"));

            // Takedown queries
            pgcf.on("error", function(err) {
                console.log("Error encountered while streaming trades to database: %s", err.message);
                console.log("Full dump of error object is as follows: %o", err);
                rollback(finishCallback);
            });
            pgcf.on("finish", function() {
                debug("Finished streaming trades to database for symbol: %s", symbol);
                async.series([
                    async.apply(client.query.bind(client), "ALTER TABLE " + tempTableName + " ADD COLUMN time TIMESTAMP WITH TIME ZONE"),
                    async.apply(client.query.bind(client), "UPDATE " + tempTableName + " SET time = TO_TIMESTAMP(time_unix)"),
                    async.apply(client.query.bind(client), "CREATE INDEX ON " + tempTableName + " (time, volume, price)"),
                    async.apply(client.query.bind(client), "DELETE FROM " + tableName + " prm " +
                        "USING " + tempTableName + " \"stg\" " + 
                        "WHERE \""  + symbolFields[0] + "\"='" + exchange + "' AND \"" + symbolFields[1] + "\"='" + currency + "' " +  
                        "AND prm.\"" + fileFields[0] + "\"=stg.time AND prm.\"" + fileFields[1] + "\"=stg.price AND prm.\"" + fileFields[2] + "\"=stg.volume"),
                    async.apply(client.query.bind(client), "INSERT INTO " + tableName + " " + 
                        "(" + symbolFieldsStr + "," + fileFieldsStr + "," + countFieldStr + ") " +
                        "(SELECT '" + exchange + "', '" + currency + "', time, price, volume, COUNT(*) " +
                        "FROM " + tempTableName + " " +
                        "GROUP BY 3,4,5 " +
                        "ORDER BY 3,4,5)"),
                    async.apply(client.query.bind(client), "DROP TABLE " + tempTableName),
                    async.apply(client.query.bind(client), "COMMIT")
                ], function(err) {
                    if( err ) {
                        console.log("Received the following error while peforming post merge load actvities: %s", err.message);
                        rollback(finishCallback);
                    }
                    else {
                        debug("Successfully finished merge activities into database for symbol: %s", symbol);
                        done();
                        finishCallback(null,true);
                    }
                });
            });
            cb(null, true);
        };

        // Execute the necessary queries
        async.series([
            async.apply(client.query.bind(client),"BEGIN TRANSACTION"),
            async.apply(client.query.bind(client),"CREATE TABLE " + tempTableName + 
                "(time_unix INTEGER, price DECIMAL, volume DECIMAL)"),
            async.apply(createTradeStreamCore)
        ], function(err) {
            if( err ) {
                rollback(callback);
            } else {
                debug("Successfully constructed trade stream into database: %s", symbol);
                callback(null, pgcf);
            }
        });
    });
};