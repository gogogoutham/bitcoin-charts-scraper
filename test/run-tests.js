#!/usr/bin/env node
var reporter = require('nodeunit').reporters.default;
process.chdir(__dirname);
reporter.run([
    "test-requestor.js",
    "test-parser.js",
    "test-file-writer.js",
    "test-pg-loader.js"
]);