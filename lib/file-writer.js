// Requirements
var debug = require("debug")("bitcoin-charts-file-writer"),
    fs = require("fs"),
    path = require("path"),
    util = require("util");

// Saves file somewhere in the local directory structure; this is performed asynchronously with the callback
exports.run = function(fileName, fileContents, callback) {
    fs.mkdir(path.dirname(fileName), "0755", function(err) { // Make the containing directory
        if (!err || err.message.substring(0,6) == "EEXIST") { // If no errors or a directory already exists, continue
            fs.writeFile(fileName, fileContents, function(err) { // Write the contents
                if(err) { callback(error); }
                else { callback(null, true); }
            });
        }
        else {
            callback(err);  
        }
    });
};

// Touches the file
exports.touch = function(fileName, callback) {
    exports.run(fileName, '', callback);
};