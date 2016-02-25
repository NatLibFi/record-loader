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
        define([
	    'es6-polyfills/lib/polyfills/promise',
	    'es6-polyfills/lib/polyfills/object',
	    'jjv',
	    'jjve',
	    '../resources/config-schema.json',
	    'record-loader-prototypes/lib/hooks/prototype',
	    'record-loader-prototypes/lib/result-formatters/prototype',
	    'record-loader-prototypes/lib/logger/prototype'
	], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory(
	    require('es6-polyfills/lib/polyfills/promise'),
	    require('es6-polyfills/lib/polyfills/object'),
	    require('jjv'),
	    require('jjve'),
	    require('../resources/config-schema.json'),
	    require('record-loader-prototypes/lib/hooks/prototype'),
	    require('record-loader-prototypes/lib/result-formatters/prototype'),
	    require('record-loader-prototypes/lib/logger/prototype')
	);
    }

}(this, factory));

function factory(Promise, Object, jjv, jjve, schema, hookFactory, resultFormatterFactory, createLoggerFactory)
{
    
    'use strict';

    var PROCESSING_STEPS = ['filter', 'preprocess', 'match', 'merge', 'load'];

    return function(input_data, modules, config) {

	var transaction_retries, logger,
	processed_records_indices = [],
	results = {
	    processed: 0,
	    skipped: 0,
	    recordStore: {
		created: 0,
		updated: 0,
		deleted: 0
	    },
	    merged: 0,
	    matched: 0,
	    transactions: []
	},
	modules_default = {
	    hooks: {
		beforeTransaction: hookFactory
	    },
	    resultFormatter: resultFormatterFactory,
	    createLoggerFactory: createLoggerFactory
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
	    
	    function assignDefaultModules()
	    {

		if (modules.hasOwnProperty('hooks')) {
		    Object.keys(modules_default.hooks).forEach(function(key) {
			modules.hooks[key] = modules.hooks.hasOwnProperty(key)
			    ? modules.hooks[key]
			    : modules_default.hooks[key](config.moduleParameters.hooks[key]);
		    });
		} else {
		    modules.hooks = modules_default.hooks;
		}

		modules.resultFormatter = modules.hasOwnProperty('resultFormatter')
		    ? modules.resultFormatter
		    : modules_default.resultFormatter(config.moduleParameters.resultFormatter);

	    }

	    function initialiseModules(keys)
	    {

		function initialiseSubModules(keys, key_parent)
		{
		    keys.forEach(function(key) {
			
			modules[key_parent][key] = modules[key_parent][key](config.moduleParameters[key_parent][key]);
		    	modules[key_parent][key].setLogger(modules.loggerFactory(config.logging.level, config.logging.prefixes, key_parent + '/' + key));

			if (key_parent === 'processors') {
			    
			    if (key === 'match' || key === 'load') {
				modules.processors[key].setRecordStore(modules.recordStore);
			    }
			    if (key === 'merge' || key === 'load') {
				modules.processors[key].setResultsLevel(config.processing.resultsLevel);
			    }	
			    if (modules.converter) {
				modules.processors[key].setConverter(modules.converter);
			    }
			    
			} else if (key_parent === 'hooks') {
			    modules.hooks[key].setRecordStore(modules.recordStore);
			    modules.hooks[key].setRecordSet(modules.recordSet);
			    modules.hooks[key].setConverter(modules.converter);
			}
			
		    });
		}

		keys.forEach(function(key) {
		    switch (key) {
		    case 'processors':
		    case 'hooks':
			initialiseSubModules(Object.keys(modules[key]), key);
			break;
		    case undefined:
			break;
		    default:

			modules[key] = modules[key](config.moduleParameters[key]);

			if (key !== 'loggerFactory') {
		    	    modules[key].setLogger(modules.loggerFactory(config.logging.level, config.logging.prefixes, key));
			}

			break;
		    }
		});

	    }

	    modules = modules === undefined ? {} : modules;
	    config = config === undefined ? {} : config;
	    
	    try {
		validateConfig();
	    } catch (excp) {
		throw new Error("Configuration doesn't validate against schema: "+excp.message);
	    }

	    /**
	     * @internal loggerFactory needs to be initialised here (Or in initialiseModules, if it's user-defined) because we need it for every other module
	     **/
	    if (!modules.hasOwnProperty('loggerFactory')) {
		modules.loggerFactory = modules_default.createLoggerFactory(config.moduleParameters.loggerFactory);
	    }

	    initialiseModules(Object.keys(modules).sort(function(a, b) {
		
		function isGreater(value)
		{
		    switch (value) {
		    case 'loggerFactory':
			return 2;
		    case 'processors':
		    case 'hooks':
			return 0;
		    default:
			return 1;
		    }
		}
		return isGreater(b) - isGreater(a);

	    }));

	    assignDefaultModules();
	    
	    logger = modules.loggerFactory(config.logging.level, config.logging.prefixes);

	    logger.debug('Configuration: '+JSON.stringify(config, null, 2));

	}
	
	function processRecordSet(previous_results_or_first_run)
	{
	    logger.debug('processRecordSet');
	    if (previous_results_or_first_run === undefined) {
		logger.info('Finished processing record set');
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

			    delete record_level_results.index;
			    delete record_level_results.processed;
			    delete record_level_results.skipped;
			    delete record_level_results.transactions;
			    
			    results.records[previous_result.index] = record_level_results;

			}
			
			Object.keys(previous_result).forEach(function(key) {
			    if (key === 'index') {
				return;
			    } else if (key === 'recordStore') {
				Object.keys(previous_result.recordStore).forEach(function(record_store_key) {
				    results.recordStore[record_store_key] += previous_result.recordStore[record_store_key].length;
				});
			    } else if (key === 'merged') {
				results.merged += previous_result.merged.length;
			    } else if (key === 'matched') {
				results.matched += previous_result.matched.length;
			    } else if (key === 'processed' || key === 'skipped') {
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
	    logger.debug('getRecordData');
	    return modules.recordSet.next().then(function(record_data) {

		var index;

		if (record_data !== undefined) {

		    index = Array.isArray(record_data) ? record_data[0].index : record_data.index;

		    if (processed_records_indices.indexOf(index) >= 0) {
			logger.debug('Skipping already processed record at index ' + index);
			return getRecordData();
		    }
		    
		}
		
		if (index !== undefined) {
		    logger.info('Starting processing of record index ' + index);
		}
		
		return record_data;

	    });
	}

	function handleRecordData(record_data)
	{
	    logger.debug('handleRecordData');
	    if (record_data === undefined ) {
		return Promise.resolve();
	    } else if (Array.isArray(record_data)) {

		transaction_retries = 0;

		return modules.hooks.beforeTransaction.run(record_data).then(
		    function() {
			return startRecordDataTransaction(record_data);
		    },
		    function(reason) {
			logger.error('Processing interrupted by beforeTransaction -hook');
			throw reason;
		    }
		);
		
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

		    logger.info('Transaction complete');
		    modules.recordStore.toggleTransaction();

		    results.transactions.push({
			retries: transaction_retries
		    });
		    
		    Array.prototype.push.apply(processed_records_indices, processed_indices);

		    return Promise.resolve(transaction_results);

		} else {

		    logger.info('Starting processing of record index ' + record.index);
		    
		    return processRecord(record, 1).then(
			function(result) {
			    transaction_results.push(result);
			    processed_indices.push(record.index);
			    return iterate(list);
			},
			function(reason) {
			    if (config.processing.transaction.enableRollback) {
				return modules.recordStore.rollback().then(function() {

				    if (typeof config.processing.transaction.retryAfterRollback === 'boolean' && config.processing.transaction.retryAfterRollback === true || typeof config.processing.transaction.retryAfterRollback === 'number' && transaction_retries < config.processing.transaction.retryAfterRollback) {
					transaction_retries++;
					return startRecordDataTransaction(records);
				    } else {
					logger.error(
					    transaction_retries > 0
						? 'Transaction failed after ' + transaction_retries + ' retries'
						: 'Transaction failed'
					);
					return Promise.reject(reason);
				    }

				});
			    } else {
				logger.error('Transaction failed');
				return Promise.reject(reason);
			    }
			}
		    );
		}
	    }

	    logger.info('Starting transaction for ' + records.length + ' records');
	    modules.recordStore.toggleTransaction(1);
	    
	    /**
	     * @todo Does this backfire if our record data is Huge?
	     */
	    return iterate(records.slice());

	}

	function processRecord(record_data, skip_setting_index_as_processed)
	{

	    logger.debug('processRecord');

	    var promise,
	    steps = PROCESSING_STEPS.slice(),
	    result = {
		index: record_data.index,
		processed: 0,
		skipped: 0,
		recordStore: {
		    created: [],
		    updated: [],
		    deleted: []
		},
		merged: [],
		matched: []
	    };

	    /**
	     * 1) If step is undefined, resolve with result.
	     * 2) If no module is available, reject
	     * 3) If step is 'preprocess' and record is undefined, set skipped and resolve with result.
	     * 4) If promise is undefined, create new promise with module's run method. Otherwise and the method as thenable to the promise
	     */
	    function iterate(step, step_previous)
	    {

		function setResult(value, step)
		{

		    var index_additional = 1;

		    value = Array.isArray(value) ? value : [value];

		    result.data = {
			input: modules.resultFormatter.data(record_data.data, config.processing.resultsLevel, 'input'),
			    output: modules.resultFormatter.data(step === 'filter' ? record_data.data : value[0], config.processing.resultsLevel, 'output')
		    };

		    switch (step) {
		    case 'filter':
			if (value[0] === undefined) {
			    result.skipped = 1;
			}
			break;
		    case 'match':
			if (value.length > 1) {
			    result.matched = modules.resultFormatter[step](value[1], config.processing.resultsLevel);
			    index_additional = 2;
			}
			break;
		    case 'merge':
			if (value.length > 1) {
			    result.merged = modules.resultFormatter[step](value[1], config.processing.resultsLevel);
			    index_additional = 2;
			}
			break;
		    case 'load':
			Object.assign(result.recordStore, modules.resultFormatter.load(value[0]));
			break;
		    }

		    if (value[index_additional] !== undefined) {
			result[step] = modules.resultFormatter.additional(value[index_additional], config.processing.resultsLevel);
		    }

		    if (step === config.processing.target) {

			if (!result.skipped) {
			    result.processed = 1;
			}
			
		    }

		}

		var step_next = step === config.processing.target ? undefined : steps.shift();

		if (step === undefined) {

		    return promise.then(function(value) {

			if (value) {
			    setResult(value, step_previous);
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

		} else {

		    promise = promise !== undefined ?  promise : function() {

			logger.debug("Entering '" + step + "' processing step");

			return modules.processors[step].run(record_data.data);

		    }();
		    promise = promise.then(function(value) {
		
			value = Array.isArray(value) ? value : [value];
		
			if (value[0] !== undefined && step_next !== undefined) {
			    
			    /**
			     * @internal Filter does not resolve with record data so it needs to replaced here
			     */
			    if (step === 'filter') {
				value[0] = record_data.data;
			    }

			    setResult(value, step);

			    logger.debug("Entering '" + step_next + "' processing step");

			    return modules.processors[step_next].run.apply(undefined, value);
			    
			} else {
			    setResult(value, step);		    
			    return Promise.resolve();
			}

		    });

		}

		return iterate(step_next, step);

	    }

	    return iterate(steps.shift());
	    
	}

	try {
	    initialise();
	    return modules.recordSet.initialise(input_data, config.processing.findRelatedRecords).then(function(){
		logger.info('Starting processing record set');
		return processRecordSet(1);
	    });
	} catch (excp) {
	    return Promise.reject(excp);
	}
	
    };

}
