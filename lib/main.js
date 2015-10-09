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
 * for the JavaScript code in this file.
 *
 **/

/* istanbul ignore next: umd wrapper */
(function(root, factory) {

    'use strict';

    if (typeof define === 'function' && define.amd) {
        define(['es6-polyfills/lib/promise', 'es6-polyfills/lib/object', 'loglevel', 'loglevel-std-streams', 'jjv', 'jjve', '../resources/config-schema.json'], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory(require('es6-polyfills/lib/promise'), require('es6-polyfills/lib/object'), require('loglevel'), require('loglevel-std-streams'), require('jjv'), require('jjve'), require('../resources/config-schema.json'));
    }

}(this, factory));

function factory(Promise, Object, log, loglevelStandardStreams, jjv, jjve, schema)
{
    
    'use strict';

    return function(input_data, modules, config) {

	var processed_records_indices = [];
	var transaction_retries;
	var results = {
	    processed: 0,
	    skipped: 0,
	    recordStore: {
		created: 0,
		updated: 0,
		deleted: 0
	    },
	    merged: 0,
	    transactions: []
	};

	function validateConfig()
	{

	    var env = jjv();
	    var je = jjve(env);
	    var errors = env.validate(schema, config, {
		useDefault: true
	    });
	    
	    if (errors) {
		throw new Error(JSON.stringify(je(schema, config, errors), undefined, 4));
	    }
	    
	}
	
	function initialise()
	{	    

	    function initialiseModule(module_type, is_processor) {
		if (is_processor) {
		    if (module_type in modules.processors) {

			modules.processors[module_type] = modules.processors[module_type](config.moduleParameters.processors[module_type]);
			
			if (module_type === 'match' || module_type === 'load') {
			    modules.processors[module_type].setRecordStore(modules.recordStore);
			}

			if (module_type === 'merge' || module_type === 'load') {
			    modules.processors[module_type].setResultsLevel(config.processing.resultsLevel);
			}

			if (modules.converter) {
			    modules.processors[module_type].setConverter(modules.converter);
			}
			
		    }
		} else if (module_type in modules) {
		    modules[module_type] = modules[module_type](config.moduleParameters[module_type]);
		}
	    }

	    modules = modules === undefined ? {} : modules;
	    config = config === undefined ? {} : config;

	    try {
		validateConfig();
	    } catch (excp) {
		throw new Error("Configuration doesn't validate against schema: "+excp.message);
	    }

	    loglevelStandardStreams(log);
	    log.setLevel(config.logging.level);

	    log.debug('Configuration: '+JSON.stringify( config, null, 2 ));

	    Object.keys(modules).filter(function(key) {
		return key !== 'processors';
	    }).forEach(function(key) {
		initialiseModule(key);
	    });

	    if ('processors' in modules) {
		Object.keys(modules.processors).forEach(function(key) {
		    initialiseModule(key, 1);
		});
	    }

	}   
	
	function processRecordSet(previous_results_or_first_run)
	{
	    log.debug('processRecordSet');
	    if (previous_results_or_first_run === undefined) {
		return Promise.resolve(results);
	    } else {
		if (typeof previous_results_or_first_run === 'object') {
		    
		    var previous_results = Array.isArray(previous_results_or_first_run) ? previous_results_or_first_run : [previous_results_or_first_run];

		    if (config.processing.resultsLevel !== 'total' && !results.hasOwnProperty('records')) {
			results.records = [];
		    }

		    previous_results.forEach(function(previous_result) {

			var record_level_results;

			if (config.processing.resultsLevel !== 'total') {

			    record_level_results = JSON.parse(JSON.stringify(previous_result));

			    delete record_level_results.processed;
			    delete record_level_results.skipped;
			    delete record_level_results.transactions;
			    
			    results.records.push(record_level_results);

			}
		
			Object.keys(previous_result).forEach(function(key) {
			    if (key === 'recordStore') {
				Object.keys(previous_result.recordStore).forEach(function(record_store_key) {
				    results.recordStore[record_store_key] += previous_result.recordStore[record_store_key].length;
				});
			    } else if (key === 'merged') {
				results.merged += previous_result.merged.length > 1 ?  previous_result.merged.length : 0;
			    } else if (previous_result[key]) {
				/**
				 * @internal Adds to 'processed' or 'skipped'
				 */
				results[key]++;
			    }
			});

		    });
		    
		}

		return getRecordData().then(handleRecordData).then(processRecordSet);
	    }
	}

	function getRecordData()
	{
	    log.debug('getRecordData');
	    return modules.recordSet.next().then(function(record_data) {
		if (record_data !== undefined && processed_records_indices.indexOf(record_data.index) >= 0) {
		    log.debug('Skipping already processed record at index ' + record_data.index);
		    return getRecordData();
		} else {
		    return record_data;
		}
	    });
	}

	function handleRecordData(record_data)
	{
	    log.debug('handleRecordData');
	    if (record_data === undefined ) {
		return Promise.resolve();
	    } else if (Array.isArray(record_data)) {
		transaction_retries = 0;
		return startRecordDataTransaction(record_data);
	    } else {
		return processRecord(record_data);
	    }
	}

	function startRecordDataTransaction(records)
	{

	    var transaction_results = [],
	    processed_indices = []; 

	    function iterate(list)
	    {
		
		var record = list.shift();
		
		if (record === undefined) {

		    log.debug('Transaction complete');
		    modules.recordStore.toggleTransaction();

		    results.transactions.push({
			retries: transaction_retries
		    });
	    
		    Array.prototype.push.apply(processed_records_indices, processed_indices);

		    return Promise.resolve(transaction_results);

		} else {
		    return processRecord(record, 1).then(
			function(result) {
			    transaction_results.push(result);
			    processed_indices.push(record.index);
			    return iterate(list);
			},
			function(reason) {
			    log.info('Transaction failed');
			    if (config.processing.transaction.enableRollback) {
				return modules.recordStore.rollback().then(function() {

				    var rejection_message;

				    if (typeof config.processing.transaction.retryAfterRollback === 'boolean' && config.processing.transaction.retryAfterRollback === true || typeof config.processing.transaction.retryAfterRollback === 'number' && transaction_retries < config.processing.transaction.retryAfterRollback) {
					transaction_retries++;
					return startRecordDataTransaction(records);
				    } else {
					rejection_message = transaction_retries > 0
					    ? 'Transaction failed after ' + transaction_retries + ' retries: '
					    : 'Transaction failed: ';
					return Promise.reject(new Error(rejection_message + reason.message));
				    }

				});
			    } else {
				return Promise.reject(new Error('Transaction failed: ' + reason.message));
			    }
			}
		    );
		}
	    }

	    log.info('Starting transaction for ' + records.length + ' records');
	    modules.recordStore.toggleTransaction(1);
	    
	    /**
	     * @todo Does this backfire if our record data is Huge?
	     */
	    return iterate(records.slice());

	}

	function processRecord(record_data, skip_setting_index_as_processed)
	{
	    log.debug('processRecord');

	    var promise;
	    var steps = ['filter', 'preprocess', 'match', 'merge', 'load'];
	    var result = {
		processed: 0,
		skipped: 0,
		recordStore: {
		    created: [],
		    updated: [],
		    deleted: []
		},
		merged: []
	    };

	    /**
	     * 1) If step is undefined, resolve with result.
	     * 2) If no module is available, reject
	     * 3) If step is 'preprocess' and record is undefined, set skipped and resolve with result.
	     * 4) If promise is undefined, create new promise with module's run method. Otherwise and the method as thenable to the promise
	     */
	    function iterate(step)
	    {

		if (step === undefined) {

		    return promise.then(function(value) {

			if (!result.skipped) {
			    result.processed = 1;
			}

			if (value) {
			    if (config.processing.target === 'load') {
				Object.assign(result.recordStore, value[0]);
			    } else {

				if (config.processing.resultsLevel === 'debug') {
				    result.data = value[0];
				}

				if (config.processing.target === 'merge' && value.length > 1) {
				    result.merged = value[1];
				}

			    }
			}

			if (!skip_setting_index_as_processed) {
			    processed_records_indices.push(record_data.index);
			}

			return Promise.resolve(result);

		    });

		} else if (modules.processors[step] === undefined) {

		    var error = new Error('Target processing step cannot be reached because of a invalid/undefined processor: ' + step);

		    if (!promise) {
			return Promise.reject(error);
		    } else {
			return promise.then(function() {
			    throw error;
			});
		    }
		} else if (step === 'filter') {
		    promise = modules.processors[step].run(record_data.data);
		} else if (step === 'preprocess') {
		    
		    promise = promise.then(function(record) {
			if (record === undefined) {
			    result.skipped = 1;
			    return Promise.resolve();
			} else {
			    return modules.processors.preprocess.run(record);
			}
		    });

		} else {
		    promise = promise.then(function(value) {

			if (value) {

			    if (step === 'load') {
				
				if (value.length > 0) {
				    result.merged = value[1];
				}

				if (config.processing.resultsLevel === 'debug') {
				    result.data = value[0];
				}

			    }

			    return modules.processors[step].run.apply(undefined, Array.isArray(value) ? value : [value]);

			} else {
			    throw new Error("Cannot proceed to step '" + step + "' because previous processor returned undefined");
			}
		    });
		}
		
		return iterate(step === config.processing.target ? undefined : steps.shift());

	    }

	    return iterate(steps.shift());
	    
	}

	try {
	    initialise();
	    return modules.recordSet.initialise(input_data, config.processing.findRelatedRecords).then(function(){
		return processRecordSet(1);
	    });
	} catch (excp) {
	    return Promise.reject(excp);
	}
	
    };

}