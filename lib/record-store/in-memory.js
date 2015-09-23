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
        define(['es6-polyfills/lib/promise', 'es6-polyfills/lib/object', 'record-loader-prototypes/lib/record-store/prototype'], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory(require('es6-polyfills/lib/promise'), require('es6-polyfills/lib/object'), require('record-loader-prototypes/lib/record-store/prototype'));
    }

}(this, factory));

function factory(Promise, Object, recordStoreProto) {

    'use strict';
    
    /**
     * @todo Write documentation
     * @todo Support objects as records (Comparison doesn't work)
     */
    return function(parameters) {

	var records = [];
	var rollback_log = [];
	var transaction_enabled = 0;
	var proto = Object.assign({}, recordStoreProto());

	function findSingleRecord(query)
	{
	    
	    var record_index;
	    var results = records.map(function(record) {
		return JSON.parse(JSON.stringify(record));
	    }).filter(function(record, index) {
		
		var result = query(record);
		
		if (result) {
		    record_index = index;
		    return 1;
		}
		
	    });

	    if (results.length !== 1) {
		throw new Error('No single record found with query');
	    }
	    
	    return record_index;
	    
	}

	if (parameters !== undefined) {
	    records = records.concat(parameters);
	}

	proto.create = function(record, options)
	{

	    records.push(JSON.parse(JSON.stringify(record)));

	    if (transaction_enabled) {
		rollback_log.push({
		    record: JSON.parse(JSON.stringify(record)),
		    operation: 'create'
		});
	    }

	    return Promise.resolve();

	};
	
	proto.read = function(query)
	{

	    var records_copied = records.map(function(record) {
		return JSON.parse(JSON.stringify(record));
	    });

	    return Promise.resolve(query ? records_copied.filter(query) : records_copied);

	};
	
	proto.update = function(query, record)
	{

	    var index = findSingleRecord(query);
	    var original_record = JSON.parse(JSON.stringify(records[index]));

	    records[index] = JSON.parse(JSON.stringify(record));

	    if (transaction_enabled) {
		write_log.push({
		    record: original_record,
		    query: query,
		    operation: 'update'
		});
	    }

	    return Promise.resolve();

	};
	
	proto.delete = function(query)
	{

	    var index = findSingleRecord(query);
	    var deleted_record = records.splice(index, 1)[0];

	    if (transaction_enabled) {
		write_log.push({
		    record: deleted_record,
		    operation: 'delete'
		});
	    }

	    return Promise.resolve();

	};
	
	proto.toggleTransaction = function(toggle)
	{
	    if (toggle) {
		transaction_enabled = 1;
	    } else {
		rollback_log = [];
		transaction_enabled = 0;
	    }
	};

	proto.rollback = function()
	{
	    if (transaction_enabled) {
		
		rollback_log.forEach(function(log_entry) {
		    switch (log_entry.operation) {
		    case 'create':
			records.some(function(record, index) {
			    if (record === log_entry.record) {
				records.splice(index, 1);
				return 1;
			    }
			});
			break;
		    case 'delete':
			records.push(log_entry.record);
			break;
		    case 'update':
			records.some(function(record, index) {
			    if (log_entry.query(record)) {
				records[index] = log_entry.record;
				return 1;
			    }
			});
			break;
		    }
		});

		transaction_enabled = 0;

		return Promise.resolve();

	    } else {
		return Promise.reject('Transaction is not enabled');
	    }
	};

	return proto;
	
    };

}