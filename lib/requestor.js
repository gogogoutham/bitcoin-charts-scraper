// Requirements
var debug = require("debug")("bitcoin-charts-requestor"),
    request = require("request"),
    cheerio = require("cheerio");

// Configuration variables
var manifestUrl = "http://api.bitcoincharts.com/v1/csv/",
    tradesUrl = "http://api.bitcoincharts.com/v1/trades.csv";

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


// Options passed to request object for requesting manifest files
var assembleManifestFileRequest = function(sublink, encoding) {
    return {
        url : manifestUrl + "/" + sublink,
        "encoding" : encoding
    };
};

// Basic wrapper around getting a file listed in the manifest; will almost always be a list of trades
exports.getManifestFile = function(sublink, encoding, callback) {
    var list = [];
    request.get(
        assembleManifestFileRequest(sublink,encoding),
        function(error, response, body){ //Callback to deal with response
            if (!error && response.statusCode == 200) {
                callback(null, body);    
            } else {
                callback(error);
            }
        }
    ); 
};

// Same as above, but returns the request object for piping
exports.createManifestFileStream = function(sublink, encoding) {
    return request.get(assembleManifestFileRequest(sublink,encoding));
};

// Options passed to request object for requesting trades info
var assembleTradesRequest = function(options) {
    return { // Specification of full request
        url : tradesUrl,
        qs : options
    };
};

// Get trades according to a user specified request
exports.getTrades = function(options, callback) {
    request.get(
        assembleTradesRequest(options),
        function(error, response, csv){ //Callback to deal with response
            if (!error && response.statusCode == 200) {
                callback(null, csv);    
            } else {
                callback(error);
            }
        }
    ); 
};

// Same as above but returns request object for piping
exports.createTradesStream = function(options) {
    return request.get(assembleTradesRequest(options));
};