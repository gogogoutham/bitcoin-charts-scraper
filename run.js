// #!/usr/bin/node

// Requirements
var btcCharts = require(__dirname + "/lib/bitcoin-charts.js"),
    pgloader = require(__dirname + "/lib/pgloader.js"),
    util = require(__dirname + "/lib/util.js"),
    async = require("async"),
    debug = require('debug')('bitcoin-charts-scraper'),
    fs = require("fs"),
    pg = require("pg"),
    zlib = require("zlib");

// Configuration
var dbConfFile = __dirname + "/.pgpass", // postgres host connection specification
    dataDir = __dirname + "/data", // data storage directory 
    insertRowLengthCap = 20000, // max row size over which to begin data batching
    manifestTable = 'manifest', // table storing listing of available historical data
    tradeTable = 'trade'; // table storing actual listed trades

// Establish time
var runTime = new Date(),
    runTimeSql = runTime.toISOString().slice(0,-5).replace("T",' ') + "+00",
    runTimeUnix = runTime.getTime();

// Parse Postgres configuration
var dbUrl;
(function() {
    var config = (fs.readFileSync(dbConfFile, { encoding : "ascii" })).trim().split(":"),
    hostname = config[0],
    port = config[1],
    database = config[2],
    username = config[3],
    password = config[4];

    dbUrl = "postgres://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database;
})();

// Save manifest file to disk
var saveManifest = function(html, callback) {
    util.saveFile(
        dataDir + "/" + "manifest_on_" + runTimeUnix + ".html",
        html,
        function(err, result) {
            if(err) {
                console.log("Could not save manifest to disk.");
                callback(err);
            }
            debug("Saved manifest to disk.");
            callback(null, result);
        }
    );
};

// Saves trades from historical csv listing (manifest) to disk
var saveTradesFromManifestFile = function(symbol, csv, callback) {
    util.saveFile(
        dataDir + "/" + "trades_for_" + symbol.toLowerCase() + "_historical_on_" + runTimeUnix + ".csv",
        csv,
        function(err, result) {
            if(err) {
                console.log("Could not save historical trades for %s to disk.", symbol);
                callback(err);
            }
            debug("Successfully saved historical trades for %s to disk.", symbol);
            callback(null, result);
        }
    );
};

// Save trades gotten from API call to disk
var saveTradesFromApi = function(symbol, startTime, csv, callback) {
    util.saveFile(
        dataDir + "/" + "trades_for_" + symbol.toLowerCase() + "_api_start_" + startTime + "_on_" + runTimeUnix + ".csv",
        csv,
        function(err, result) {
            if(err) {
                console.log("Could not save api trades for %s with start of %s to disk.", symbol, startTime);
                callback(err);
            }
            debug("Successfully saved api trades for %s with start of %s to disk.", symbol, startTime);
            callback(null, result);
        }
    );
};

// Function to handle parsing and loading in sequence; this will batch data into blocks
var parseAndLoadTrades = function(symbol, csv, callback) {
    btcCharts.parseTrades(
        csv,
        function (parseErr, data) { // Callback (method to batch trades and load into sql)
            if( parseErr ) {
                console.log("Error parsing trades for symbol %s.", symbol);
                callback(parseErr);
            }
            debug("Finished parsing trades for symbol %s.", symbol);

            // Batch data according to rules specified
            var batchCount = 1;
            while( data.length > 0 ) {
                var batch = data.splice(0, insertRowLengthCap);
                var sqlErr = pgloader.run(dbUrl, 
                    tradeTable,
                    ['exchange', 'currency', 'time', 'price', 'volume', 'cnt'], 
                    ['exchange', 'currency', 'time', 'price', 'volume'], 
                    batch, 
                    pgloader.SQL_REPLACE);
                if( sqlErr instanceof Error ) {
                    console.log("Error loading batch %s of %s trades into database.", batchCount, batchCount + Math.ceil(data.length/insertRowLengthCap));
                    callback(sqlErr);
                }
                debug("Finished loading batch %s of %s of trades to DB for symbol %s.", batchCount, batchCount + Math.ceil(data.length/insertRowLengthCap), symbol);
                batchCount += 1;
            }
            debug("Finished loadiing trades to DB for symbol %s.", symbol);
            callback(null, true);
        },
        function(timeRaw) { // time formatter
            return btcCharts.parseTradeTime(timeRaw).toISOString();
        }, 
        btcCharts.parseSymbol(symbol), // pad data with the exchange+currency = symbol
        'cnt' // group the data and store the count of items in a group in 'grp'
    );
};

// Function to handle end-to-end scraping of trade data via API request (request, file storage, parsing, db storage)
var scrapeTradesFromApi = function(symbol, startTime, callback) {
    btcCharts.getTrades({ "symbol" : symbol, "start" : startTime}, function(err, csv) {
        if( err ) {
            console.log("Error requesting api trades for %s with start of %s", symbol, startTime);
            callback(err);
        }
        debug("Successfully requested api trades for %s with start of %s", symbol, startTime);

        //Save data to disk and load into database asynchronously and in parallel
        async.parallel([
                function(cb) { parseAndLoadTrades(symbol, csv, cb); },
                function(cb) { saveTradesFromApi(symbol, startTime, csv, cb); }
            ], 
            function(err, result) {
                if( err ) {
                    console.log("Problem parsing & saving api trades for %s with start of %s", symbol, startTime);
                }
                debug("Successfully parsed & saved api trades for %s with start of %s", symbol, startTime);
                callback(err, result);
            }
        );
    });
};

// Function to handle end-to-end scraping of trade data via manifest file request (request, file storage, parsing, db storage)
var scrapeTradesFromManifestFile = function(link, callback) {
    btcCharts.getManifestFile(link, null, function(err, gz) {
        var symbol = link.replace(".csv.gz","");
        if( err ) {
            console.log("Error requesting historical trades for %s", symbol);
            callback(err);
        }
        debug("Successfully requested historical trades for %s", symbol);

        // Gunzip the returned buffer and cast as a string
        zlib.gunzip( gz, function(err, csvObj) {
            if( err ) {
                console.log("Error gunzipping historical trades for: %s", symbol);
                callback(err);
            }
            debug("Successfully gunzipped historical trades for %s", symbol); 

            // Save data to disk and load into database asynchronously and in parallel
            async.parallel([
                    function(cb) { parseAndLoadTrades(symbol, csvObj.toString(), cb); },
                    function(cb) { saveTradesFromManifestFile(symbol, csvObj.toString(), cb); }
                ], 
                function(err, result) {
                    if( err ) {
                        console.log("Problem parsing & saving historical trades for %s", symbol);
                    }
                    debug("Done with parsing & saving historical trades for %s", symbol);
                    callback(err, result);
                }
            );          
        });
    });
};

// Function to dispatch trade scrapers based on the manifest
var dispatchTradeScrapers = function(manifestData, callback) {
    pg.connect(dbUrl, function(err, client, done) { // Connect to database!
        if(err) {
            debug("Could not connect to database to compare manifest to latest trades.");
            callback(err);
        }

        client.query('SELECT CONCAT(exchange, currency) as "symbol", MAX(time) as "latest" FROM trade GROUP BY 1', function(err, result) {
            if(err) {
                debug("Could not execute query to compare manifest to latest trades");
                callback(err);
            }

            // Release the connection back to the pool
            done();

            var lastRecord = {};
            for ( var i=0; i<result.rows.length; i++ ) { // Establish list of latest trade records in DB by symbol (= exchange + currency)
                lastRecord[result.rows[i].symbol] = result.rows[i].latest;
            }

            // Create individual scraping tasks by symbol based on weather or not a last trade record exists
            var scrapingTasks = manifestData.map( function(manifestRecord) {
                var manifestSymbol = manifestRecord.link.replace(".csv.gz", "");
                if( manifestSymbol in lastRecord ) { // If we have the history, get all trades for this symbol since the last recorded trade
                    debug("Scraping trades from API for symbol " + manifestSymbol + " starting on " + lastRecord[manifestSymbol]);
                    return function(cb) { scrapeTradesFromApi(manifestSymbol, lastRecord[manifestSymbol].getTime()/1000, cb); };
                } else { // Otherwise, pull the history
                    debug("No recorded trades for API symbol " + manifestSymbol + ". Pulling historical trades file.");
                    return function(cb) { scrapeTradesFromManifestFile(manifestRecord.link, cb); };
                }
            });

            // Execute scraping tasks in parallel and asynchronously
            async.parallel(scrapingTasks, function(err, result) {
                if( err ) {
                    console.log("Error scraping trades after processing manifest.");
                }
                debug("Done with scraper dispatch tasks.");
                callback(err, result);
            });

        });
    });
};

// Core scraping logic
var scrape = function(callback) {
    // Request manifest
    btcCharts.getManifest( function(err, html) {
        if( err ) {
            debug("Error requesting historical trade file manifest.");
            callback(err);
        }
        debug("Pulled HTML request for historical trade file manifest.");
        
        // Save data to disk and parse in parallel
        var tasks = [
            function(cback) { saveManifest(html, cback); }, // Save to disk
            function(cback) { // Parse manifest
                btcCharts.parseManifest(
                    html, 
                    function(err, data) {
                        if( err ) {
                            console.log("Error parsing historical trade file manifest.");
                            cback(err);
                        }
                        debug("Successfully parsed historical trade file manifest.");
                        
                        var tasks = [
                            function(cb) { dispatchTradeScrapers(data,cb); }, // Dispatch trade scrapers based on data received
                            function(cb) { // Load manifest into database
                                var result = pgloader.run(dbUrl, 
                                    manifestTable, 
                                    ['link', 'time', 'size'], 
                                    ['link'], 
                                    data, 
                                    pgloader.SQL_REPLACE);
                                if ( result instanceof Error ) {
                                    cb(result);
                                } else {
                                    cb(err, result);
                                }
                            },
                        ];

                        async.parallel(tasks, function(err, result) {
                            if( err ) {
                                console.log("Problem post-manifest parsing.");
                            }
                            debug("Done with post-manifest parsing tasks.");
                            cback(err, result);
                        });                       
                    }, 
                    function(timeRaw) {
                        return btcCharts.parseManifestTime(timeRaw).toISOString();
                    }
                );
            }
        ];
        async.parallel(tasks, function(err, result) {
            if( err ) {
                console.log("Problem with post-manifest download.");
            }
            debug("Done with post-manifest download tasks.");
            callback(err, result);
        });
    });
};

// Manifest download and dispatch testing - uncomment section to deactivate trade download
// scrapeTradesFromApi = function() { debug("Arguments to scrape trades from api: %s, %s", arguments[0], arguments[1]); arguments[2](null,true); };
// scrapeTradesFromManifestFile = function() { debug("Arguments to scrape trades from manifest file: %s", arguments[0]); arguments[1](null,true); };

// Trade scraping test - uncomment section to limit dispatch of trade scrapers to a small subset of trades
// insertRowLengthCap = 10;
// scrape = function(callback) {
//     tasks = [
//         function(cb) { scrapeTradesFromApi('bitstampUSD', Math.floor((new Date()).getTime()/1000) - 86400, cb); }//, // All bitstamp USD trades in the last day
//         // function(cb) { scrapeTradesFromManifestFile('localbtcPLN.csv.gz', cb); } // Historical record of all polish zloty trades on local btc
//     ];
//     async.parallel(tasks, function(err, result) {
//         if( err ) {
//             console.log("Problem encountered in trade scraping tasks.");
//         }
//         debug("Done with trade scraping tasks.");
//         callback(err, result);
//     });
// };

// Core scraper call
scrape(function(err, result) {
    if( err instanceof Error ) {
        throw err;
    }
    pg.end();
    debug("Closing all open postgres clients. Done with everything.");
});