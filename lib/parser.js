// Requirements
var debug = require("debug")("bitcoin-charts-parser"),
    cheerio = require("cheerio"),
    stream = require("stream"),
    util = require("util");

// Helper function to divide a symbol into its exchange and currency
exports.splitSymbol = function(symbol) {
    return {
        "exchange" : symbol.slice(0,-3),
        "currency" : symbol.slice(-3)
    };
};

// Helper function for formatting the manifest time string as a javascript date object
exports.formatManifestTime = function(manifestTime) {
    var timeParts = manifestTime.split(" "),
        dateStr = (new Date(timeParts[0])).toISOString().slice(0,10),
        time = (new Date(dateStr + "T" + timeParts[1] + ":00+00:00"));
    return time;
};

// Parses the listing of manifest file (paired with the output requestor::getManifestFile)
exports.formatManifest = function(html, options, callback) {

    var list = [],
        rawList,
        timeFormatter,
        doc = cheerio.load(html);

    // Set the time formatter to the identity function if not specified
    if( !options.timeFormatter ) {
        timeFormatter = function(time) { return time; };
    } else {
        timeFormatter = options.timeFormatter;
    }

    //Now process the timestamp and size information (asynchronously)
    setTimeout(function() {
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
    },0);
};

// Parses the trades specified time to a javascript date object
exports.formatTradeTime = function(tradeTime) {
    return (new Date(tradeTime*1000));
};

// Parses the returned set of trades
// NOTE: assumes that trades are presented in chronological order; will not work otherwise
exports.formatTrades = function(csv, options, callback) {
    var lines = csv.trim().split("\n"),
        data=[],
        timeFormatter,
        rowPadding,
        countInGroupField,
        countInGroup,
        lastTime = null;

    //console.log("These are the lines I got :%o", lines);
    
    // Establish time formatting function
    if( options && options.timeFormatter ) {
        timeFormatter = options.timeFormatter;
    } else {
        timeFormatter = function(time) {
            return time;
        };
    }

    // Establish row padding if specified in options
    if( options && options.rowPadding ) {
        rowPadding = options.rowPadding;
    } else {
        rowPadding = null;
    }

    // Establish group field is specified
    if( options && options.countInGroupField ) {
        countInGroupField = options.countInGroupField;
        countInGroup = {};
    } else {
        countInGroupField = null;
        countInGroup = null;
    }

    // Parse line-by-line asynchronously
    setTimeout(function() {
        for ( i=0; i<lines.length; i++ ) {
            var cells = lines[i].split(","),
                datum = {
                    "time" : timeFormatter(parseInt(cells[0])),
                    "price" : cells[1],
                    "volume" : cells[2]
                },
                time = parseInt(cells[0]);
            
            // Add row padding
            if( rowPadding ) {
                for( var prop in rowPadding ) {
                    datum[prop] = rowPadding[prop];
                }
            }

            if( countInGroupField ) { // Handle grouping if specified
                if( time == lastTime ) { // New datum occured at the same time as the last
                    // Increment the count of group members and move on
                    if( JSON.stringify(datum) in countInGroup ) {
                        countInGroup[JSON.stringify(datum)] += 1;
                    } else {
                        countInGroup[JSON.stringify(datum)] = 1;
                    }
                } else { // We have a new datum

                    // Push previous all entries with the previous time into the array after adding on the group count
                    // console.log("Pushing out groups for time %s", time);
                    for( var datumStr in countInGroup ) {
                        var preDatum = JSON.parse(datumStr);
                        preDatum[countInGroupField] = countInGroup[datumStr];
                        // console.log("--Pushing out value %s, with count %s", datumStr, countInGroup[datumStr]);
                        data.push(preDatum);
                    }

                    // Reset the group count and the last time for the next go around
                    countInGroup = {};
                    countInGroup[JSON.stringify(datum)] = 1;
                    lastTime = time;
                }
            } else {
                data.push(datum);
            }
        }

        // Push the last group to the data stack if we're grouping
        if( countInGroupField ) {
            for( var finalDatumStr in countInGroup ) {
                var preFinalDatum = JSON.parse(finalDatumStr);
                preFinalDatum[countInGroupField] = countInGroup[finalDatumStr];
                data.push(preFinalDatum);
            }
        }

        callback(null,data);      
    }, 0);
};

// Stream batcher for raw trade CSV text:
// Creates blocks of raw CSV trade text of the specified line size, cleanly split across line breaks and group boundaries
util.inherits(TradeBatcher, stream.Transform);
function TradeBatcher(batchSize) {
    stream.Transform.call(this, {});
    this._writableState.objectMode = false;
    this._readableState.objectMode = true; // Reads from this stream are in object mode
    this._batchSize = batchSize; // number of lines in each block
    this._queue = [];
    this._remainder = '';
}
TradeBatcher.prototype._transform = function(chunk, encoding, callback) {
    
    // Establish the remainder that can't be passed along yet
    chunk = this._remainder + chunk.toString();
    var newLines = chunk.split("\n");
    this._remainder = newLines.pop();

    // Shift off the last line and any prior lines with mathcing time into the remainder (this groups all transactions of exact same price / volume / time)
    if( newLines.length > 0 ) {
        var lastLine = newLines.splice(-1)[0]; 
        this._remainder = lastLine + "\n" + this._remainder;
        while( newLines.length > 0 && parseInt((newLines.slice(-1)[0]).split(",")[0]) === parseInt(lastLine.split(",")[0]) ) { // check if times match
            this._remainder = newLines.pop() +"\n" + this._remainder; 
        }
        // console.log("Number of new lines is %s.", newLines.length);
    }

    // Add on the new lines to the queue and push out in blocks of the specified size
    // NOTE: we will expand beyond the specified batch size to complete the group
    this._queue = this._queue.concat(newLines);
    // console.log("***Starting push for current run of transform.");
    while( this._queue.length >= this._batchSize ) {
        // console.log("Trying to push.");
        var pushCandidate = this._queue.splice(0,this._batchSize);
        // console.log("There are %s entries in push candidate prior group bondary adjustment", pushCandidate.length);
        while( this._queue.length > 0 && parseInt((pushCandidate.slice(-1)[0]).split(",")[0]) == parseInt((this._queue[0]).split(",")[0]) ) { // Expand the push frame if a time is split across the boundary 
            pushCandidate.push(this._queue.shift());
        }
        // console.log("There are %s entries in push candidate after group boundary adjustment", pushCandidate.length);
        this.push(pushCandidate.join("\n"));
    }
    // console.log("***Finishing push for current run of transform.");

    // Return
    callback();
};
TradeBatcher.prototype._flush = function(callback) {
    this.push([this._queue.join("\n"), this._remainder].join("\n"));
    callback();
};

// Exported method for creation
exports.createTradeBatcher = function(batchSize) {
    return new TradeBatcher(batchSize);
};

// Formats CSV block of trades into JSON suitable for database insertion
// NOTE: Expects CSV text chunks of the format of TradeFormatter with trade lines and groups unsplit across chunks 
util.inherits(TradeFormatter, stream.Transform);
function TradeFormatter(formatOptions) {
    stream.Transform.call(this, { objectMode : true });
    this._formatOptions = formatOptions; // options of the kind expected by formatTrades
}
TradeFormatter.prototype._transform = function(chunk, encoding, callback) {
    // Format trades according to formatTrades() and push out
    var self = this;
    if( chunk.toString().trim().length === 0 ) {
        debug("Encountered empty chunk for %s. Terminating stream.", JSON.stringify(self._formatOptions));
        self.push(null);
    } else {
        exports.formatTrades(chunk.toString(), self._formatOptions, function(err, data) {
            if( err ) {
                console.log("Error formatting trade chunk.");
                callback(err);
            } else {
                self.push(data);
                callback();
            }
        });
    }
};
// Exported method for creation
exports.createTradeFormatter = function(formatOptions) {
    return new TradeFormatter(formatOptions);
};