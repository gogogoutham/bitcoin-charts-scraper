var parser = require(__dirname + "/../lib/parser.js"),
    fs = require("fs"),
    zlib = require("zlib");

exports.testFormatManifest = function(test) {
    test.expect(5);
    fs.readFile(__dirname + "/read-files/manifest.html", function(fsErr, html) {
        test.ifError(fsErr);
        parser.formatManifest(html, { timeFormatter : parser.formatManifestTime }, function(fmtErr, data) {
            test.ifError(fmtErr);
            test.equal(data.length, 209, "Manifest length is not as expected.");
            test.equal(JSON.stringify(data[0]), JSON.stringify({
                link : "anxhkCNY.csv.gz",
                time : new Date("2014-03-03T22:16:00+00:00"),
                size : 31651 
            }), "First line of manifest data is not as expected.");
            test.equal(JSON.stringify(data.slice(-1)[0]), JSON.stringify({
                link : "weexUSD.csv.gz",
                time : new Date("2014-03-03T22:14:00+00:00"),
                size : 1495 
            }), "Last line of manifest data is not as expected.");
            test.done();
        });
    });
};

// 477 total groups should be present
// First line: 1372092982,371.170000000000,0.269400000000 (3 times)
// Last line: 1395132140,1828.360000000000,1.367300000000 (1 time)
exports.testFormatTrades = function(test) {
    test.expect(5);
    fs.readFile(__dirname + "/read-files/localbtcPLN.csv", function(fsErr, csvBuffer) {
        test.ifError(fsErr);
        var options = {
            timeFormatter : parser.formatTradeTime,
            rowPadding : parser.splitSymbol("localbtcPLN"),
            countInGroupField : 'cnt'
        };
        parser.formatTrades(csvBuffer.toString().trim(), options, function(fmtErr, data) {
            test.ifError(fmtErr);
            test.equal(data.length, 478, "Trade data is not of expected length.");
            test.equal(JSON.stringify(data[0]), JSON.stringify({
                time : new Date(1372092982*1000), 
                price : "371.170000000000",
                volume : "0.269400000000",
                exchange : "localbtc", 
                currency : "PLN",
                cnt : 3
            }), "First line of trade data is not as expected.");
            test.equal(JSON.stringify(data.slice(-1)[0]), JSON.stringify({
                time : new Date(1395132140*1000), 
                price : "1828.360000000000",
                volume : "1.367300000000",
                exchange : "localbtc", 
                currency : "PLN",
                cnt : 1
            }), "Last line of trade data is not as expected.");
            test.done();
        });
    });
};

exports.testTradeBatcher = function(test) {
    test.expect(7);
    var sm = fs.createReadStream(__dirname + "/read-files/localbtcPLN.csv")
        .pipe(parser.createTradeBatcher(100)),
        expectedChunkLineCounts =[100,100,100,106,88],
        lineCount = 0,
        chunkCount = 0,
        latestLine;
    sm.on("data", function(chunk) {
        var lines = chunk.toString().trim().split("\n"),
            chunkLineCount = lines.length,
            testMessage;
        latestLine = lines.slice(-1);
        lineCount += chunkLineCount;
        chunkCount += 1;
        testMessage = "Chunk nuumber " + chunkCount + " doesn't have " + expectedChunkLineCounts[chunkCount-1] + " lines.";
        test.equal(chunkLineCount, expectedChunkLineCounts[chunkCount-1], testMessage);
    });
    sm.on("end", function() {
        test.equal(lineCount, 494, "Total line count doesn't match up with exepctations");
        test.equal(latestLine, "1395132140,1828.360000000000,1.367300000000", "Last line does not match expectations.");
        test.done();
    });
};

exports.testTradeFormatter = function(test) {
    test.expect(3);

    // Setup common configuration and the results comparision method to be executed at the end
    var parseOptions = {
            timeFormatter : parser.formatTradeTime,
            rowPadding : parser.splitSymbol("localbtcPLN"),
            countInGroupField : 'cnt'
        },
        results = {},
        compareResults = function(source, data) {
            results[source] = data;
            if( ("stream" in results) && ("block" in results) ) {
                var testMessage = "Stream and block trade formatting results don't match";
                test.equal(JSON.stringify(results.stream), JSON.stringify(results.block), testMessage);
                test.done();
            }
        };

    // Setup / run the stream parser
    (function() {
        var sm = fs.createReadStream(__dirname + "/read-files/localbtcPLN.csv")
            .pipe(parser.createTradeBatcher(100))
            .pipe(parser.createTradeFormatter(parseOptions)),
            data = [];
        sm.on("data", function(chunk) {
            data = data.concat(chunk);
        });
        sm.on("end", function() {
            compareResults("stream", data);
        });
    })();

    // Setup / run the block parser
    (function() {
        fs.readFile(__dirname + "/read-files/localbtcPLN.csv", function(fsErr, csvBuffer) {
            test.ifError(fsErr);
            parser.formatTrades(csvBuffer.toString(), parseOptions, function(fmtErr, data) {
                test.ifError(fmtErr);
                compareResults("block", data);
            });
        });
    })();
};


exports.testTradeFormatterEmpty = function(test) {
    test.expect(1);
    var sm = fs.createReadStream(__dirname + "/read-files/bcmBMAUD.csv.gz")
        .pipe(zlib.createGunzip())
        .pipe(parser.createTradeBatcher(100))
        .pipe(parser.createTradeFormatter({
            timeFormatter : parser.formatTradeTime,
            rowPadding : parser.splitSymbol("bcmBMAUD"),
            countInGroupField : 'cnt'
        })),
        chunkCount = 0;

    sm.on("data", function(chunk) {
        chunkCount += 1;
        // console.log("Received the following chunk of length %s: %o", chunk.length, chunk);
    });

    sm.on("end", function() {
        test.equal(chunkCount, 0, "Got more than 0 chunks.");
        test.done();
    });
};

exports.testTradeFormatterSmall = function(test) {
    test.expect(1);
    var sm = fs.createReadStream(__dirname + "/read-files/bcmBMGAU.csv.gz")
        .pipe(zlib.createGunzip())
        .pipe(parser.createTradeBatcher(100)) 
        .pipe(parser.createTradeFormatter({
            timeFormatter : parser.formatTradeTime,
            rowPadding : parser.splitSymbol("bcmBMGAU"),
            countInGroupField : 'cnt'
        })),
        lineCount = 0;

    sm.on("data", function(chunk) {
        lineCount += chunk.length;
        // console.log("Received the following chunk of length %s: %o", chunk.length, chunk);
    });

    sm.on("end", function() {
        test.equal(lineCount, 1, "Got more than 1 chunk.");
        test.done();
    });
}; 