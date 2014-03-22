// Requirements
var async = require("async"),
    debug = require("debug")("bitcoin-charts-pg-loader"),
    pg = require("pg"),
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