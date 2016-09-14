"use strict";

function emptyFunction(){}

const colors = require('colors/safe');
const util = require('util');

module.exports = function(name) {
  if (process.env.DEBUG === '*' || process.env.DEBUG === name) {
    name = colors.cyan(name + ' ');
    return function(format, ...args) {
      var text = util.format(name + format, ...args);
      process.send({text: text});
    }
  }
  return emptyFunction;
};
