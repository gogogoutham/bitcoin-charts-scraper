var requestor = require(__dirname + "/../lib/requestor.js");

exports.testGetManifest = function(test) {
    test.expect(2);
    var cheerio = require("cheerio");
    requestor.getManifest(function(err, html) {
        test.ifError(err);
        var doc = cheerio.load(html);
        firstFile = doc("pre").html().split("\r\n")[0];
        test.equal(firstFile.trim(), '<a href="../">../</a>', "Did not receive expected manifest file listing.");
        test.done();
    });
};

exports.testGetManifestFile = function(test){
    test.expect(3);
    var zlib = require("zlib");
    requestor.getManifestFile("bitaloGBP.csv.gz", null, function(reqErr, body) {
        test.ifError(reqErr);
        zlib.gunzip(body, function(zipErr, csvBuffer) {
            test.ifError(zipErr);
            var csvFirstLine = csvBuffer.toString().split("\n")[0];
            test.equal(csvFirstLine, "1387976140,410.000000000000,0.050000000000", "First line of CSV did not match expectations.");
            test.done();
        });
    });
};

exports.testGetTrades = function(test) {
    test.expect(2);
    // Pulls all USD trades in the last 20 minutes from bitstamp; should usually yield a trade
    requestor.getTrades({ "symbol" : "bitstampUSD", "start" : Math.floor(new Date().getTime()/1000) - 60*20}, function(reqErr, csvBuffer) {
        test.ifError(reqErr);
        var csvFirstLine = csvBuffer.toString().split("\n")[0];
        test.equal(csvFirstLine.match(/^\d{10},\d+\.\d+,\d+\.\d+$/), csvFirstLine); // Check that csv line matches usual format
        test.done();
    });
};

exports.testManifestFileStream = function(test) {
    var zlib = require("zlib");
    test.expect(1);
    var sm = requestor.createManifestFileStream("bitaloGBP.csv.gz", null)
            .pipe(zlib.createGunzip()),
        output = '';
    sm.on("data", function(chunk) {
        output += chunk.toString();
    });
    sm.on("end", function() {
        outputFirstLine = output.split("\n")[0];
        test.equal(outputFirstLine, "1387976140,410.000000000000,0.050000000000");
        test.done();
    });
};

exports.testTradesStream = function(test) {
    test.expect(1);
    var options = { "symbol" : "bitstampUSD", "start" : Math.floor(new Date().getTime()/1000) - 60*20 }, // Pulls all USD trades in the last 20 minutes from bitstamp; should usually yield a trade
        sm = requestor.createTradesStream(options, null),
        output = '';
    sm.on("data", function(chunk) {
        output += chunk.toString();
    });
    sm.on("end", function() {
        outputFirstLine = output.split("\n")[0];
        test.equal(outputFirstLine.match(/^\d{10},\d+\.\d+,\d+\.\d+$/), outputFirstLine); // Check that csv line matches usual format
        test.done();
    });
};