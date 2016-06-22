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

    return {
	assignDefaultModules: function(modules, modules_default)
	{

	    function iterate(obj, obj_target)
	    {
		return Object.keys(obj).reduce(function(result, key) {

		    if (!result.hasOwnProperty(key)) {
			result[key] = obj[key];
		    } else if (typeof result[key] === 'object') {
			result[key] = iterate(obj[key], result[key]);
		    }
		    
		    return result;

		}, obj_target);
	    }

	    return iterate(modules_default, modules);

	},
	initializeModules: function(modules, config)
	{

	    function iterate(obj, parameters, keys_parent)
	    {

		parameters = parameters ? parameters : {};

		return Object.assign(obj, Object.keys(obj).filter(function(key) {

		    return ['logger', 'converter'].indexOf(key) < 0;
		    
		}).reduce(function(result, key) {
		    
		    if (typeof result[key] === 'function') {

			result[key] = result[key](parameters[key]);
			result[key].setLogger(modules.logger(parameters.logger, Array.isArray(keys_parent) ? keys_parent.concat(key).join('/') : key));

		    } else if (typeof result[key] === 'object') {

			result[key] = iterate(result[key], parameters[key], Array.isArray(keys_parent) ? keys_parent.concat(key) : [key]);
		    }

		    return result;

		}, obj));

	    }
	    
	    modules = Object.keys(modules).filter(function(key) {

		return ['logger', 'converter'].indexOf(key) >= 0;

	    }).reduce(function(result, key) {

		if (key === 'converter') {
		    result.converter = result.converter(config.moduleParameters.converter);
		} else {
		    result.logger = result.logger(config.logging);
		}
		
		return result;

	    }, modules);

	    return iterate(modules, config.moduleParameters);
	    
	}
    };

}