var fileWriter = require(__dirname + "/../lib/file-writer.js"),
    fs = require("fs"),
    stream = require("stream");

exports.testRun = function(test) {
    test.expect(5);
    var targetDir = __dirname + "/write-files-" + ("0000000000" + Math.floor(10000000000*Math.random())).slice(-10), // Random ten number directory suffix
        targetFile = targetDir + "/foo",
        targetContents = "bar";
    
    // Create file through method, confirm existence & contents, then remove
    fileWriter.run(targetFile, targetContents, function(runErr, result) {
        test.ifError(runErr);
        test.ok(fs.existsSync(targetFile), "Written file doesn't exist.");
        test.equal(fs.readFileSync(targetFile).toString(), targetContents, "Written file contents don't match what was sent");
        fs.unlink(targetFile, function(ulErr, result) {
            test.ifError(ulErr);
            fs.rmdir(targetDir, function(rmErr, result) {
                test.ifError(rmErr);
                test.done();
            });
        });
    });    
};

exports.testTouch = function(test) {
    test.expect(4);
    var targetDir = __dirname + "/write-files-" + ("0000000000" + Math.floor(10000000000*Math.random())).slice(-10), // Random ten number directory suffix
        targetFile = targetDir + "/foo",
        targetContents = 
    
    // Create file through method, confirm existence, then remove
    fileWriter.touch(targetFile, function(touchErr, result) {
        test.ifError(touchErr);
        test.ok(fs.existsSync(targetFile), "Written file doesn't exist.");
        fs.unlink(targetFile, function(ulErr, result) {
            test.ifError(ulErr);
            fs.rmdir(targetDir, function(rmErr, result) {
                test.ifError(rmErr);
                test.done();
            });
        });
    });    
};