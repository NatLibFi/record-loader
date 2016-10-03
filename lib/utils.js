/**
 *
 * @licstart  The following is the entire license notice for the JavaScript code in this file. 
 *
 * Load records into a data store while filtering, preprocessing and matching & merging them in the process
 *
 * Copyright (c) 2015-2016 University Of Helsinki (The National Library Of Finland)
 *
 * This file is part of record-loader
 *
 * record-loader is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *  
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *  
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * @licend  The above is the entire license notice
 * for the JavaScript code in this file.
 *
 **/

/* istanbul ignore next: umd wrapper */
(function(root, factory) {

  'use strict';
  
  if (typeof define === 'function' && define.amd) {
    define(['es6-polyfills/lib/polyfills/object'], factory);
  } else if (typeof module === 'object' && module.exports) {
    module.exports = factory(require('es6-polyfills/lib/polyfills/object'));
  }

}(this, factory));

function factory(Object)
{

  'use strict';

  var exports = {
    /**
     * Each step is processedng with the corresponding module. Steps are always processed in order.
     * @constant
     * @type {Array}
     * @default
     */
    PROCESSING_STEPS: Object.freeze(['filter', 'preprocess', 'match', 'merge', 'load']),
    clone: function(obj)
    {
      return JSON.parse(JSON.stringify(obj));
    },
    undefineProperty: function(obj, property)
    {
      delete obj[property];
      return obj;
    },
    undefineProperties: function(obj, properties)
    {

      properties.forEach(function(property) {
        exports.undefineProperty(obj, property);
      });

      return obj;

    },
    mergeObjects: function(from, to)
    {

      var key,
      keys = Object.keys(from),
      obj_merged = keys.length > 0 ? exports.mergeObjects(to, {}) : to;

      while ((key = keys.shift())) {
        
        if (!obj_merged.hasOwnProperty(key)) {
          obj_merged[key] = from[key];
        } else if (typeof from[key] === 'object') {
          obj_merged[key] = exports.mergeObjects(from[key], to[key]);
        }
        
      }

      return obj_merged;

    },
    callFactories: function(factories, options)
    {
            
      function iterate(factories, options)
      {

        options = typeof options == 'object' ? options : {};
        return Object.keys(factories).reduce(function(result, key) {
          
          if (typeof factories[key] === 'object') {
            result[key] = iterate(factories[key], options[key]);
          } else {
            result[key] = factories[key](options[key]);
          }
          
          return result;
          
        }, {});

      }

      return iterate(factories, options);
      
    },
    /**
     * Constructs a new object based on a existing error which was thrown. Instances of Error object do not have properties as enumerable. This function makes them that way
     * @param {Error|*} The error thrown
     * @param {object} [additional_properties={}] Additional properties to add to the object
     */
    createError: function(error, additional_properties)
    {

      error = typeof error === 'object' ? error : {
        message: error
      };

      return Object.assign(
        Object.getOwnPropertyNames(error).reduce(function(result, name) {

          return Object.defineProperty(result, name, {
            enumerable: true,
            value: error[name]
          });
          
        }, {}),
        {
          failed: true
        },
        typeof additional_properties === 'object' ? additional_properties : {}
      );
    }
  };

  return exports;

}
