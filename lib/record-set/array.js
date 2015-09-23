/**
 *
 * @licstart  The following is the entire license notice for the JavaScript code in this file. 
 *
 * Load records into a data store while filtering, preprocessing and matching & merging them in the process
 *
 * Copyright (c) 2015 University Of Helsinki (The National Library Of Finland)
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
 * for the JavaScript code in this page.
 *
 **/

/* istanbul ignore next: umd wrapper */
(function (root, factory) {

    'use strict';

    if (typeof define === 'function' && define.amd) {
        define(['es6-polyfills/lib/promise', 'es6-polyfills/lib/object', 'record-loader-prototypes/lib/record-set/prototype'], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory(require('es6-polyfills/lib/promise'), require('es6-polyfills/lib/object'), require('record-loader-prototypes/lib/record-set/prototype'));
    }

}(this, factory));

function factory(Promise, Object, recordSetProto) {

    'use strict';

	return function() {

	    var arr;
	    var index = 0;
	    var proto = Object.assign({}, recordSetProto());

	    proto.initialise = function(input_data)
	    {
		if (Array.isArray(input_data)) {
		    arr = JSON.parse(JSON.stringify(input_data));
		    return Promise.resolve();
		} else {
		    return Promise.reject(new Error('Input is not an array'));
		}
	    };

	    proto.current = function()
	    {

		var record_data = {
		    index: index === 0 ? index : index - 1
		};

		record_data.data = arr[record_data.index] === undefined ? undefined : JSON.parse(JSON.stringify(arr[record_data.index]));

		return Promise.resolve(record_data);

	    };
	    
	    proto.next = function()
	    {

		var record_data = {
		    data: arr[index] === undefined ? undefined : JSON.parse(JSON.stringify(arr[index])),
		    index: index
		};

		if (record_data.data !== undefined) {
		    index++;
		}

		return Promise.resolve(record_data);

	    };

	    return proto;

	};

}