#!/usr/bin/env node

// Requirements
var requestor = require(__dirname + "/lib/requestor.js"),
    parser = require(__dirname + "/lib/parser.js"),
    fileWriter = require(__dirname + "/lib/file-writer.js"),
    pgLoader = require(__dirname + "/lib/pg-loader.js"),
    async = require("async"),
    debug = require('debug')('bitcoin-charts-scraper'),
    fs = require("fs"),
    pg = require("pg"),
    zlib = require("zlib");

// Configuration
var dbConfFile = __dirname + "/.pgpass", // postgres host connection specification
    dataDir = __dirname + "/data", // data storage directory 
    batchSize = 50000, // max row size over which to begin data batching
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

var scrapeTrades = function (options, callback) {

    // Establish the CSV stream and save paths
    var csvStream, symbol, savePath, parseOptions, dbLoadOptions;
    if( options.source == "api" ) {
        symbol = options.symbol;
        savePath = dataDir + "/" + "trades_for_" + symbol.toLowerCase() + "_" +
            "api_start_" + options.start + "_on_" + runTimeUnix + ".csv";
        csvStream = requestor.createTradesStream({
            symbol : options.symbol,
            start : options.start
        }, null);
    } else {
        symbol = options.link.replace(".csv.gz","");
        savePath = dataDir + "/" + "trades_for_" + symbol.toLowerCase() + "_" +
            "historical" + "_on_" + runTimeUnix + ".csv";
        csvStream = requestor.createManifestFileStream(options.link, null)
            .pipe(zlib.createGunzip());
    }

    // Setup setup common parse and DB options
    parseOptions = {
        timeFormatter : parser.formatTradeTime,
        rowPadding : parser.splitSymbol(symbol),
        countInGroupField : 'cnt'
    };
    dbLoadOptions = {
        "dbUrl" : dbUrl, 
        "tableName" : tradeTable,
        "insertFields" : ['exchange', 'currency', 'time', 'price', 'volume', 'cnt'],
        "keyFields" : ['exchange', 'currency', 'time', 'price', 'volume'],
        "sqlMode" : pgLoader.SQL_REPLACE
    };

    async.parallel([
        // Pipe to parse and database load, w/ an event handler on success
        function(cb) {
            var dbsm = csvStream.pipe(parser.createTradeBatcher(batchSize))
                .pipe(parser.createTradeFormatter(parseOptions))
                .pipe(pgLoader.createStream(dbLoadOptions));
            dbsm.on("finish", function() {
                debug("Successfully saved the following trade information to the database: %s", JSON.stringify(options));
                cb(null, true);
            });
        },
        // Pipe to file save w/ an event handler on success
        function(cb) {
            var fsm = csvStream.pipe(fs.createWriteStream(savePath));
            fsm.on("finish", function() {
                debug("Successfully saved the following trade information to file: %s", JSON.stringify(options));
                cb(null, true);
            });
        }
    ], function(err, result) {
        if( err ) {
            console.log("Error encountered while scraping %s", JSON.stringify(options));
            callback(err);
        }
        else {
            debug("Successfully scraped %s", JSON.stringify(options));
            callback(null, true);
        }
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
                var manifestSymbol = manifestRecord.link.replace(".csv.gz", ""),
                    scrapeOptions;
                if( manifestSymbol in lastRecord ) { // If we have the history, get all trades for this symbol since the last recorded trade
                    debug("Scraping trades from API for symbol " + manifestSymbol + " starting on " + lastRecord[manifestSymbol]);
                    scrapeOptions = {
                        source : "api",
                        symbol : manifestSymbol,
                        start : lastRecord[manifestSymbol].getTime()/1000
                    };
                } else { // Otherwise, pull the history
                    debug("No recorded trades for API symbol " + manifestSymbol + ". Pulling historical trades file.");
                    scrapeOptions = {
                        source : "manifest",
                        link : manifestRecord.link
                    };
                }
                return function(cb) { return scrapeTrades(scrapeOptions, cb); };
            });

            // Execute scraping tasks in parallel and asynchronously
            async.series(scrapingTasks, function(err, result) {
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
    // Establish manifest file save convention
    var saveManifest = function(html, callback) {
        fileWriter.run(
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

    // Establish database load options
    var dbLoadOptions = {
            "dbUrl" : dbUrl, 
            "tableName" : manifestTable,
            "insertFields" : ['link', 'time', 'size'],
            "keyFields" : ['link'],
            "sqlMode" : pgLoader.SQL_REPLACE
        };

    // Request manifest
    requestor.getManifest( function(err, html) {
        if( err ) {
            debug("Error requesting historical trade file manifest.");
            callback(err);
        }
        debug("Pulled HTML request for historical trade file manifest.");
        
        // Save data to disk and parse in parallel
        var tasks = [
            function(cback) { saveManifest(html, cback); }, // Save to disk
            function(cback) { // Parse manifest
                parser.formatManifest(html, { timeFormatter : parser.formatManifestTime },
                    function(err, data) {
                        if( err ) {
                            console.log("Error parsing historical trade file manifest.");
                            cback(err);
                        }
                        debug("Successfully parsed historical trade file manifest.");

                        async.parallel([
                            async.apply(dispatchTradeScrapers, data),
                            async.apply(pgLoader.run.bind(pgLoader), data, dbLoadOptions)
                        ], function(err, result) {
                            if( err ) {
                                console.log("Problem post-manifest parsing.");
                            }
                            debug("Done with post-manifest parsing tasks.");
                            cback(err, result);
                        });                       
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
// scrapeTrades = function(options, callback) {
//     debug("Accepted the following options call %s", JSON.stringify(options));
//     callback(null, true);
// };

// // Trade scraping test - uncomment section to limit dispatch of trade scrapers to a small subset of trades
// batchSize = 1000;
// scrape = function(callback) {
//     async.series([
//         async.apply(scrapeTrades, { source : "api", symbol : 'bitstampUSD', start : Math.floor((new Date()).getTime()/1000) - 86400 }),
//         async.apply(scrapeTrades, { source : "manifest", link : "localbtcPLN.csv.gz"})
//     ], function(err, result) {
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