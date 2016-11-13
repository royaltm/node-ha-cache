"use strict";

if (process.env.DEBUG_METHOD === 'IPC') {
  const emptyFunction = () => {};
  const colors = require('colors/safe');
  const util = require('util');

  const prefix = `[${process.pid}]: `;

  module.exports = exports = function(name) {
    if (process.env.DEBUG === '*' || process.env.DEBUG === name) {
      name = colors.cyan(name + ' ');
      return function(format, ...args) {
        var text = util.format(name + format, ...args);
        process.send({text: prefix + text});
      }
    }
    return emptyFunction;
  };

}
else module.exports = exports = require('debug');
