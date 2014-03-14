// Requirements
var sql = require('sql'),
    async = require('async'),
    pg = require("pg"),
    fs = require("fs");

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
    queries.push("CREATE TEMPORARY TABLE " + tempTableName + " AS SELECT " + insertFieldsString + // 2a. Create temp table
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
exports.run = function(url, tableName, insertFields, keyFields, data, sqlMode) {

    // Open postgres sql connection
    pg.connect(url, function(err, client, done) { // Connect!
        if(err) {
            console.log("Could not connect to database.");
            return err;
        }
        
        // Define a query task generator for chaining of SQL query execution with async.series method
        var queryTaskGenerator = function(client, sql) {
            return function(cb) {
                //console.log("DB Query: %s;", sql);
                client.query(sql, function(err, result) {
                    cb(err, result);
                });
            };
        };

        // Generate SQL
        sqlQueries = exports.generateSQL(tableName, insertFields, keyFields, data, sqlMode);

        // Execute
        async.series(sqlQueries.map(function(sql){ return queryTaskGenerator(client, sql); }), function(err, results) {
            if( err ) {
                console.log("Encountered the following error: %s", err.message);
                console.log("Executing DB rollback...");
                client.query("ROLLBACK", function( anotherErr, result ) {
                    if( anotherErr ) {
                        console.log("Oh no, I couldn't rollback the transaction. You might want to check for open transactions.");
                        return anotherErr;
                    }
                    console.log("Done.");
                    done();
                    return err;    
                });
            }
            else {
                // console.log("SQL Insertion process succeeded.");
                done();
                return true;
            }
        });

    });

};