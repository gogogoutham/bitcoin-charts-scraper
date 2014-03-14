// Requirements
var request = require("request"),
    cheerio = require("cheerio");

// Configuration variables
var manifestUrl = "http://api.bitcoincharts.com/v1/csv/",
    tradesUrl = "http://api.bitcoincharts.com/v1/trades.csv";

// Helper function to divide a symbol into its exchange and currency
exports.parseSymbol = function(symbol) {
    return {
        "exchange" : symbol.slice(0,-3),
        "currency" : symbol.slice(-3)
    };
};

// Gets the file listing as an array of objects
exports.getManifest = function(callback) {
    request.get({
        url : manifestUrl
    }, function(error, response, html){ //Callback to deal with response
        if (!error && response.statusCode == 200) {
            callback(null, html);    
        } else {
            callback(error);
        }
    }); 
};

// Helper function for formatting the manifest time string as a javascript date object
exports.parseManifestTime = function(manifestTime) {
    var timeParts = manifestTime.split(" "),
        dateStr = (new Date(timeParts[0])).toISOString().slice(0,10),
        time = (new Date(dateStr + "T" + timeParts[1] + ":00+00:00"));
    return time;
};

// Parses the listing of files, as specified above
exports.parseManifest = function(html, callback, timeFormatter) {

    var list = [],
        rawList,
        doc = cheerio.load(html);

    // Set the time formatter to the identity function if not specified
    if( !timeFormatter ) {
        timeFormatter = function(time) { return time; };
    }

    //Now process the timestamp and size information
    rawList = doc("pre").html().split("\r\n");
    for ( var i=0; i < rawList.length; i++ ) {

        // Skip the first row and any rows not in the expected format
        if( i === 0 || rawList[i].indexOf("<a href=") === -1 ) {
            continue;
        }

        // Replace multiple spaces with something more like a true seperator
        var cells = rawList[i].replace(/\s{2,}/g,"/;/;/;/;/;/;/;").split("/;/;/;/;/;/;/;");
        //console.log(cells);

        list.push({
            "link" : cells[0].match(/href="([\w\.\/]+)"/)[1],
            "time" : timeFormatter(cells[1]),
            "size" : parseInt(cells[2])
        });
    }
    callback(null, list);
};

// Basic wrapper around getting a file listed in the manifest; will almost always be a list of trades
exports.getManifestFile = function(sublink, encoding, callback) {
    var list = [];
    request.get({
        url : manifestUrl + "/" + sublink,
        "encoding" : encoding
    }, function(error, response, body){ //Callback to deal with response
        if (!error && response.statusCode == 200) {
            callback(null, body);    
        } else {
            callback(error);
        }
    }); 
};

// Get trades according to a user specified request
exports.getTrades = function(options, callback) {
    request.get({ // Specification of full request
        url : tradesUrl,
        qs : options
    }, function(error, response, html){ //Callback to deal with response
        if (!error && response.statusCode == 200) {
            callback(null, html);    
        } else {
            callback(error);
        }
    }); 
};

// Parses the trades specified time to a javascript date object
exports.parseTradeTime = function(tradeTime) {
    return new Date(tradeTime*1000);
};

// Parses the returned set of trades
exports.parseTrades = function(csv, callback, timeFormatter, rowPadding, groupField) {
    var lines = csv.split("\n"),
        data=[],
        groups;
    
    // Establish time formatting function
    if( !timeFormatter ) {
        timeFormatter = function(time) {
            return time;
        };
    }

    // Create list of counts for grouping functions
    if( groupField ) {
        groups = {};
    }

    for ( i=0; i<lines.length; i++ ) {
        var cells = lines[i].split(","),
            datum = {
                "time" : timeFormatter(cells[0]),
                "price" : cells[1],
                "volume" : cells[2]
            },
            groupId = JSON.stringify(datum);
        
        // Add row padding
        if( rowPadding ) {
            for( var prop in rowPadding ) {
                datum[prop] = rowPadding[prop];
            }
        }

        if( groupField ) { // Increment count for group if grouping
            if( groupId in groups ) {
                groups[groupId][groupField] += 1;
            } else {
                groups[groupId] = datum;
                groups[groupId][groupField] = 1;
            }
        } else { // Otherwise load w/o manipulation
            data.push(datum);
        }
    }

    // Load groups if we're performing grouping
    if( groupField ) {
        for( var gid in groups ) {
            data.push(groups[gid]);
        }
    }

    callback(null,data);
};