// // 1. Request and parsing of file listing
// var btcCharts = require(__dirname + "/../lib/bitcoin-charts.js");
// btcCharts.getManifest(function(err, html) {
//     if( err ) {
//         console.log("Error requesting file listing.");
//         throw err;
//      }
//     btcCharts.parseManifest(html, function(err, list) {
//         if( err ) {
//             console.log("Error parsing file.");
//             throw err;
//         }
//         console.log("Returned list is %o", list);
//     }, function(timeRaw) {
//         return btcCharts.parseManifestTime(timeRaw);
//     });
// });

// 2. Request and parsing of a specific set of trades (will also gunzip the file) 
// var btcCharts = require(__dirname + "/../lib/bitcoin-charts.js"),
//     tradesSpec = { "symbol" : "bitstampUSD" };
// btcCharts.getTrades(tradesSpec, function(err, csv) {
//     if( err ) {
//         console.log("Error requesting trades: %o", tradesSpec);
//         throw err;
//     }
//     btcCharts.parseTrades(csv, function(err, list) {
//         if( err ) {
//             console.log("Error parsing trades: %o", tradesSpec);
//             throw err;
//         }
//         console.log("Returned list is %o", list);
//     }, function(timeRaw) {
//         return btcCharts.parseTradeTime(timeRaw);
//     });
// });

// 3. Request, parse and DB load of manifest
// var btcCharts = require(__dirname + "/../lib/bitcoin-charts.js"),
//     pgloader = require(__dirname + "/../lib/pgloader.js"),
//     fs = require("fs");
// var dbConfFile = __dirname + "/../.pgpass",
//     outTable = 'manifest';
// var dbUrl;
// (function() {
//     var config = (fs.readFileSync(dbConfFile, { encoding : "ascii" })).trim().split(":"),
//     hostname = config[0],
//     port = config[1],
//     database = config[2],
//     username = config[3],
//     password = config[4];
//     dbUrl = "postgres://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database;
// })();
// btcCharts.getManifest(function(err, html) {
//     if( err ) {
//         console.log("Error requesting file listing.");
//         throw err;
//      }
//     btcCharts.parseManifest(html, function(err, data) {
//         if( err ) {
//             console.log("Error parsing file.");
//             throw err;
//         }
//         //console.log("Returned list is %o", list);
//         pgloader.run(dbUrl, 
//             outTable, 
//             ['link', 'time', 'size'], 
//             ['time'], 
//             data, 
//             pgloader.SQL_REPLACE);
        
//     }, function(timeRaw) {
//         return btcCharts.parseManifestTime(timeRaw).toISOString();
//     });
// });


// 4. Request, parse and DB load of trade data
// var btcCharts = require(__dirname + "/../lib/bitcoin-charts.js"),
//     pgloader = require(__dirname + "/../lib/pgloader.js"),
//     fs = require("fs");
// var dbConfFile = __dirname + "/../.pgpass",
//     outTable = 'trade',
//     tradesSpec = { "symbol" : "bitstampUSD" };
// var dbUrl;
// (function() {
//     var config = (fs.readFileSync(dbConfFile, { encoding : "ascii" })).trim().split(":"),
//     hostname = config[0],
//     port = config[1],
//     database = config[2],
//     username = config[3],
//     password = config[4];
//     dbUrl = "postgres://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database;
// })();
// btcCharts.getTrades(tradesSpec, function(err, csv) {
//     if( err ) {
//         console.log("Error requesting trades: %o", tradesSpec);
//         throw err;
//      }
//     btcCharts.parseTrades(csv, function(err, data) {
//         if( err ) {
//             console.log("Error parsing trades: %o", tradesSpec);
//             throw err;
//         }

//         var sqlErr = pgloader.run(dbUrl, 
//             outTable, 
//             ['exchange', 'currency', 'time', 'price', 'volume', 'cnt'], 
//             ['exchange', 'currency', 'time', 'price', 'volume'], 
//             data, 
//             pgloader.SQL_REPLACE);

//         if( sqlErr ) {
//             throw sqlErr;
//         }
        
//     }, function(timeRaw) {
//         return btcCharts.parseTradeTime(timeRaw).toISOString();
//     }, btcCharts.parseSymbol(tradesSpec.symbol), 'cnt');
// });

// 5. Testing out the download and parsing of a gzipped file (will need some interesting utilties here)

var btcCharts = require(__dirname + "/../lib/bitcoin-charts.js"),
    pgloader = require(__dirname + "/../lib/pgloader.js"),
    zlib = require("zlib"),
    fs = require("fs");
var link = "localbtcPLN.csv.gz",
    dbConfFile = __dirname + "/../.pgpass",
    outTable = 'trade';
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
btcCharts.getManifestFile(link, null, function(err, gz) {
    if( err ) { 
        console.log("Error downloading manifest link: %s", link);
        throw err;
    }

    //Gunzip the files
    zlib.gunzip( gz, function(err, csvObj) {
        if( err ) {
            console.log("Error gunzipping manifest link: %s", link);
            throw err;
        }
        btcCharts.parseTrades(csvObj.toString(), function(err, data) {
            if( err ) {
                console.log("Error parsing manifest link trades: %s", link);
                throw err;
            }

            var sqlErr = pgloader.run(dbUrl, 
                outTable, 
                ['exchange', 'currency', 'time', 'price', 'volume', 'cnt'], 
                ['exchange', 'currency', 'time', 'price', 'volume'], 
                data, 
                pgloader.SQL_REPLACE);

            if( sqlErr ) {
                throw sqlErr;
            }
            
        }, function(timeRaw) {
            return btcCharts.parseTradeTime(timeRaw).toISOString();
        }, btcCharts.parseSymbol("localbtcPLN"), 'cnt');
    });
});

// 6. Generating requests based on what we just received and the data we already have (must run 3 and 4 first)
// 
// var btcCharts = require(__dirname + "/../lib/bitcoin-charts.js"),
//     pg = require("pg");
// var dbConfFile = __dirname + "/../.pgpass";
// var dbUrl;
// (function() {
//     var config = (fs.readFileSync(dbConfFile, { encoding : "ascii" })).trim().split(":"),
//     hostname = config[0],
//     port = config[1],
//     database = config[2],
//     username = config[3],
//     password = config[4];
//     dbUrl = "postgres://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database;
// })();

// var requests = [];
// // Open postgres sql connection
// pg.connect(dbUrl, function(err, client, done) { // Connect!
//     if(err) {
//         console.log("Could not connect to database.");
//         return err;
//     }

//     client.query('SELECT * FROM manifest', function(err, result) {
//         if(err) {
//             console.log("Could not pull data from database");
//         }

//         var list = [];
//         for ( var i=0; i<result.rows.length; i++ ) {
//             list.push({
//                 "link" : result.rows[i].link,
//                 "time" : result.rows[i].time,
//                 "size" : result.rows[i].size
//             });
//         }

//         client.query('SELECT CONCAT(exchange, currency) as "symbol", MAX(time) as "latest" FROM trade GROUP BY 1', function(err, result) {
//             if(err) {
//                 console.log("Could not pull data from database");
//             }

//             var lastRecord = {};
//             for ( var i=0; i<result.rows.length; i++ ) {
//                 lastRecord[result.rows[i].symbol] = result.rows[i].latest;
//             }

//             console.log("Last records are here for the following: %o", lastRecord);

//             for ( var j=0; j<list.length; j++) {
//                 var symbol = list[j].link.replace(".csv.gz", "");
//                 console.log(symbol);
//                 if( symbol in lastRecord ) {
//                     requests.push({ "symbol" : symbol, "start" : lastRecord[symbol].getTime()/1000 });
//                 } else {
//                     requests.push({ "link" : list[j].link });
//                 }
//             }

//             console.log("I will try the following requests %o", requests);
//             client.end(); 
//         });

//     });
// });