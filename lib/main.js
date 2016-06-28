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
	    'es6-shims/lib/shims/array',
	    'jjv',
	    'jjve',
	    '../resources/config-schema.json',
	    'workerpool/dist/workerpool',
	    './utils',
	    'record-loader-prototypes/lib/hooks/prototype',
	    'record-loader-prototypes/lib/result-formatters/prototype',
	    'record-loader-prototypes/lib/logger/prototype'
	], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory(
	    require('es6-polyfills/lib/polyfills/promise'),
	    require('es6-polyfills/lib/polyfills/object'),
	    require('es6-shims/lib/shims/array'),
	    require('jjv'),
	    require('jjve'),
	    require('../resources/config-schema.json'),
	    require('workerpool'),
	    require('./utils'),
	    require('record-loader-prototypes/lib/hooks/prototype'),
	    require('record-loader-prototypes/lib/result-formatters/prototype'),
	    require('record-loader-prototypes/lib/logger/prototype')
	);
    }

}(this, factory));

function factory(Promise, Object, shim_array, jjv, jjve, schema, workerpool, utils, hookFactory, resultFormatterFactory, createLoggerFactory)
{

    'use strict';

    var PROCESSING_STEPS = ['filter', 'preprocess', 'match', 'merge', 'load'];

    return function(input_data, modules, script_processor, config) {

	var pool_workers,
	processed_records_indexes = [],
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
		beforeTransaction: hookFactory,
		afterWait: {
		    preprocess: hookFactory,
		    match: hookFactory,
		    merge: hookFactory,
		    load: hookFactory
		}
	    },
	    resultFormatter: resultFormatterFactory,
	    logger: createLoggerFactory
	};

	function initialize()
	{

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
	    
	    validateConfig(config);
	    
	    if (!modules.hasOwnProperty('recordSet')) {
		throw new Error('Record set module is mandatory');
	    } else {
		
		modules = utils.initializeModules(utils.assignDefaultModules(modules, modules_default), config);
		modules.logger = modules.logger();
		modules.hooks = Object.keys(modules.hooks).reduce(function(result, key) {

		    function callSetters(target)
		    {
			target.setRecordSet(modules.recordSet);
			target.setRecordStore(modules.recordStore);
			target.setConverter(modules.converter);
			target.setResults(results);
		    }
		    
		    if (key === 'afterWait') {
			
			result[key] = Object.keys(result[key]).reduce(function(result2, key2) {
			    
			    callSetters(result2[key2]);
			    return result2;
			    
			}, result[key]);
			
		    } else {
			callSetters(result[key]);
		    }
		    
		    return result;
		    
		}, modules.hooks);
		
		pool_workers = workerpool.pool(script_processor, typeof config.processing.maxWorkers === 'number' ? {
		    maxWorkers: config.processing.maxWorkers
		} : {});

	    }

	}
	
	function applyResults(results_processing)
	{

	    results_processing = Array.isArray(results_processing) ? results_processing : [results_processing];

	    results_processing = results_processing.map(function(result) {
		return result.results;
	    });

	    if (config.processing.resultsLevel !== 'total' && !results.hasOwnProperty('records')) {
		results.records = [];
	    }

	    results_processing.forEach(function(result) {
		
		var record_level_results;
		
		Object.keys(result).forEach(function(key) {
		    if (key === 'index') {
			return;
		    } else if (key === 'recordStore') {

			result.recordStore = modules.resultFormatter.load(result.recordStore);

			Object.keys(result.recordStore).forEach(function(record_store_key) {
			    results.recordStore[record_store_key] += result.recordStore[record_store_key].length;
			});

		    } else if (key === 'merged') {

			result.merged = modules.resultFormatter.merge(result.merged, config.processing.resultsLevel);
			results.merged += result.merged.length;

		    } else if (key === 'matched') {

			result.matched = modules.resultFormatter.match(result.matched, config.processing.resultsLevel);
			results.matched += result.matched.length;

		    } else if (key === 'processed' || key === 'skipped') {

			results[key]++;

		    } else if (key === 'data') {
			result.data.input = modules.resultFormatter.data(result.data.input, config.processing.resultsLevel, 'input');
			result.data.output = modules.resultFormatter.data(result.data.output, config.processing.resultsLevel, 'output');
		    } else {

			result[key] = modules.resultFormatter.additional(result[key], key, config.processing.resultsLevel);

		    }

		});

		if (config.processing.resultsLevel !== 'total') {
		    
		    record_level_results = JSON.parse(JSON.stringify(result));
		    
		    delete record_level_results.index;
		    delete record_level_results.processed;
		    delete record_level_results.skipped;
		    delete record_level_results.transactions;
		    
		    results.records[result.index] = record_level_results;
		    
		}

	    });
	    
	}

	function processRecordSet(previous_results_or_first_run)
	{
	    modules.logger.debug('processRecordSet');
	    if (previous_results_or_first_run === undefined) {

		modules.logger.info('Finished processing record set');

		pool_workers.clear();

		return Promise.resolve(results);

	    } else {
		if (typeof previous_results_or_first_run === 'object') {
		    applyResults(previous_results_or_first_run);
		}

		return getRecordData().then(handleRecordData).then(processRecordSet).catch(function(error) {

		    results.status = 'failed';

		    if (typeof error.stack !== undefined) {
			results.stack = error.stack;
		    }

		    if (typeof error.message === 'string') {
			results.message = error.message;
		    }
		    
		    throw results;

		});
	    }
	}

	function getRecordData()
	{
	    modules.logger.debug('getRecordData');
	    return modules.recordSet.next().then(function(record_data) {

		var index;

		if (record_data !== undefined) {

		    index = Array.isArray(record_data) ? record_data[0].index : record_data.index;

		    if (processed_records_indexes.indexOf(index) >= 0) {
			modules.logger.debug('Skipping already processed record at index ' + index);
			return getRecordData();
		    }
		    
		}
		
		if (index !== undefined) {
		    modules.logger.info('Starting processing of record index ' + index);
		}
		
		return record_data;

	    });
	}

	function handleRecordData(record_data)
	{
	    modules.logger.debug('handleRecordData');
	    if (record_data === undefined ) {
		return Promise.resolve();
	    } else if (Array.isArray(record_data)) {

		return modules.hooks.beforeTransaction.run(record_data).then(
		    function() {

			return startRecordDataTransaction(record_data);

		    },
		    function(reason) {
			modules.logger.error('Processing interrupted by beforeTransaction -hook');
			throw reason;
		    }
		);
		
	    } else {

		spec_steps = {
		    stepStart: 0,
		    stepEnd: PROCESSING_STEPS.indexOf(config.processing.target)
		};

		spec_steps = Object.assign(spec_steps, {
		    steps: PROCESSING_STEPS.slice(spec_steps.stepStart, spec_steps.stepEnd + 1),
		    areLastSteps: PROCESSING_STEPS.indexOf(config.processing.target) === spec_steps.stepEnd
		});

		return pool_workers.exec('processRecord', [record_data, spec_steps]).then(function(results_processing) {
		    
		    processed_records_indexes.push(record_data.index);
		    return results_processing;
		    
		});

	    }
	}

	function startRecordDataTransaction(records, retries)
	{
	    
	    function getRecordResult(results, record)
	    {
		return shim_array.find(results, function(result) {
		    return result.results.index === record.index;
		});
	    }	    

	    function handleProcessRejection(reason)
	    {

		pool_workers.clear(true);

		if (config.processing.transaction.enableRollback) {
		    return modules.recordStore.rollback().then(function() {

			if (typeof config.processing.transaction.retryAfterRollback === 'boolean' && config.processing.transaction.retryAfterRollback === true || typeof config.processing.transaction.retryAfterRollback === 'number' && retries < config.processing.transaction.retryAfterRollback) {
			    retries++;
			    return startRecordDataTransaction(records);
			} else {
			    modules.logger.error(
				retries > 0
				    ? 'Transaction failed after ' + retries + ' retries'
				    : 'Transaction failed'
			    );
			    return Promise.reject(reason);
			}
			
		    });
		} else {
		    modules.logger.error('Transaction failed');
		    return Promise.reject(reason);
		}

	    }
	    
	    function recordsProcessed(processCallback, results_transaction)
	    {

		var steps_next;

		if (steps_all.length > 0) {

		    steps_next = steps_all.shift();
		    
		    modules.logger.info("Dispatching afterWait hook for '" + steps_next[0] + "' step");

		    return modules.hooks.afterWait[steps_next[0]].run(results_transaction).then(
			function() {
			    
			    modules.logger.info("Waited for all records to enter step '" + steps_next[0] + "'");
			    return processCallback(steps_next);

			},
			function(reason) {		

			    modules.logger.error('Processing interrupted by afterWait -hook');
			    applyResults(results_transaction);
			    throw reason;

			}
		    );

		} else {

		    modules.logger.info('Transaction complete');
		    modules.recordStore.toggleTransaction();

		    results.transactions.push({
			retries: retries
		    });
		    
		    processed_records_indexes = processed_records_indexes.concat(records.map(function(record_data) {			    
			return record_data.index;
		    }));

		    return Promise.resolve(results_transaction);

		}
		
	    }
	    
	    function processRecordsParallel(records_list, steps, results_processing)
	    {

		results_processing = results_processing ? results_processing : [];

		return Promise.all(records_list.map(function(record) {		    
		    
		    return pool_workers.exec('handleRecordProcessing', [record, config, steps, getRecordResult(results_processing, record)]);
		    
		})).then(
		    function(results_parallel) {

			return recordsProcessed(function(steps_next) {
			    return processRecordsParallel(records_list, steps_next, results_parallel);
			}, results_parallel);

		    },
		    handleProcessRejection
		);

	    }

	    function processRecordsSequential(records_list, steps, results_transaction)
	    {
		
		function iterate(list, results_processing)
		{

		    var record = list.shift();

		    results_processing = results_processing ? results_processing : [];

		    if (record === undefined) {

			return recordsProcessed(function(steps_next) {
			    return processRecordsSequential(records_list, steps_next, results_processing);
			}, results_processing);

		    } else {	

			return pool_workers.exec('handleRecordProcessing', [record, config, steps, getRecordResult(results_processing, record)]).then(function(result) {
			    return iterate(list, results_processing.concat(result));
			});

		    }
		    
		}

		return iterate(records_list.slice(), results_transaction);

	    }

	    var steps_all = PROCESSING_STEPS.slice(0, PROCESSING_STEPS.indexOf(config.processing.target) + 1);

	    retries = retries === undefined ? 0 : retries;

	    if (config.processing.transaction.wait === true) {

		steps_all = steps_all.map(function(step) {
		    return [step];
		});

	    } else if (Array.isArray(config.processing.transaction.wait)) {

		steps_all = steps_all.reduce(function(result, step) {

		    if (config.processing.transaction.wait.indexOf(step) >= 0) {
			result.push([step]);
		    } else {
			result[result.length - 1].push(step);
		    }

		    return result;

		}, [[]]);

	    } else {
		steps_all = [steps_all];
	    }

	    modules.logger.info('Starting transaction for ' + records.length + ' records');
	    modules.recordStore.toggleTransaction(1);	    

	    if (config.processing.transaction.async) {

		modules.logger.info('Starting parallel record processing');
		return processRecordsParallel(records.slice(), steps_all.shift());

	    } else {		
		return processRecordsSequential(records.slice(), steps_all.shift());
	    }

	}

	function handleErrors(error)
	{

	    if (typeof pool_workers === 'object') {
		pool_workers.clear(true);
	    }

	    return Promise.reject(typeof error === 'object' && error.hasOwnProperty('message') ? error : new Error(error));

	}

	try {

	    initialize();
	    	    
	    return modules.recordSet.initialise(input_data, config.processing.findRelatedRecords).then(function() {

		modules.logger.info('Starting processing record set');
		return processRecordSet(1);
		
	    }).catch(handleErrors);	   
	    
	} catch (excp) {
	    return handleErrors(excp);
	}
	
    };

}
