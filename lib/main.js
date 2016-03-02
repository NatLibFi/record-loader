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

	var logger,
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
			if (key === 'afterWait') {

			    modules.hooks.afterWait = modules.hooks.hasOwnProperty('afterWait') ? modules.hooks.afterWait : {};
			    config.moduleParameters.hooks.afterWait = config.moduleParameters.hooks.hasOwnProperty('afterWait')
				? config.moduleParameters.hooks.afterWait
				: {};

			    Object.keys(modules_default.hooks.afterWait).forEach(function(key) {

				modules.hooks.afterWait[key] = modules.hooks.afterWait.hasOwnProperty(key)
				? modules.hooks.afterWait[key]
				: modules_default.hooks.afterWait[key](config.moduleParameters.hooks.afterWait[key]);

			    });

			} else {

			    modules.hooks[key] = modules.hooks.hasOwnProperty(key)
				? modules.hooks[key]
				: modules_default.hooks[key](config.moduleParameters.hooks[key]);
			}
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

		function initialiseSubModules(keys, keys_parent)
		{

		    var modules_parent = keys_parent.reduce(function(result, key_parent) {
			return result[key_parent];
		    }, modules),
		    module_parameters_parent = keys_parent.reduce(function(result, key_parent) {
			return Object.keys(result).length > 0 && result.hasOwnProperty(key_parent) ? result[key_parent] : {};
		    }, config.moduleParameters);

		    keys.forEach(function(key) {
			
			modules_parent[key] = modules_parent[key](module_parameters_parent[key]);
		    	modules_parent[key].setLogger(modules.loggerFactory(config.logging.level, config.logging.prefixes, keys_parent.join('/') + '/' + key));

			if (keys_parent[0] === 'processors') {
			    
			    if (key === 'match' || key === 'load') {
				modules_parent[key].setRecordStore(modules.recordStore);
			    }
			    if (key === 'merge' || key === 'load') {
				modules_parent[key].setResultsLevel(config.processing.resultsLevel);
			    }	
			    if (modules.converter) {
				modules_parent[key].setConverter(modules.converter);
			    }
			    
			} else if (keys_parent[0] === 'hooks' || key) {
			    modules_parent[key].setRecordStore(modules.recordStore);
			    modules_parent[key].setRecordSet(modules.recordSet);
			    modules_parent[key].setConverter(modules.converter);
			    modules_parent[key].setResults(results);
			}
			
		    });
		}

		keys.forEach(function(key) {
		    switch (key) {
		    case 'processors':
			initialiseSubModules(Object.keys(modules[key]), ['processors']);
			break;
		    case 'hooks':			

			initialiseSubModules(Object.keys(modules[key]).filter(function(key_parent) {
			    return key_parent !== 'afterWait';
			}), ['hooks']);

			initialiseSubModules(Object.keys(modules.hooks.afterWait), ['hooks', 'afterWait']);

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
	
	function applyResults(results_processing)
	{

	    var results_processing = Array.isArray(results_processing) ? results_processing : [results_processing];
	    
	    results_processing = results_processing.map(function(result) {
		return result.results;
	    });

	    if (config.processing.resultsLevel !== 'total' && !results.hasOwnProperty('records')) {
		results.records = [];
	    }

	    results_processing.forEach(function(result) {
		
		var record_level_results;

		if (config.processing.resultsLevel !== 'total') {

		    record_level_results = JSON.parse(JSON.stringify(result));

		    delete record_level_results.index;
		    delete record_level_results.processed;
		    delete record_level_results.skipped;
		    delete record_level_results.transactions;
		    
		    results.records[result.index] = record_level_results;

		}
		
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

	    });
	    
	}

	function processRecordSet(previous_results_or_first_run)
	{
	    logger.debug('processRecordSet');
	    if (previous_results_or_first_run === undefined) {
		logger.info('Finished processing record set');
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
	    logger.debug('getRecordData');
	    return modules.recordSet.next().then(function(record_data) {

		var index;

		if (record_data !== undefined) {

		    index = Array.isArray(record_data) ? record_data[0].index : record_data.index;

		    if (processed_records_indexes.indexOf(index) >= 0) {
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
		return processRecord(record_data, 0, PROCESSING_STEPS.indexOf(config.processing.target)).then(function(result_processing) {
		    
		    processed_records_indexes.push(record_data.index);
		    return result_processing;

		});
	    }
	}

	function startRecordDataTransaction(records, retries)
	{

	    function iterate(list, steps, results_transaction)
	    {

		var steps_next,
		record = list.shift();
		
		results_transaction = results_transaction === undefined ? [] : results_transaction;
		
		if (record === undefined) {

		    if (steps_all.length > 0) {

			steps_next = steps_all.shift();
			
			logger.info("Dispatching afterWait hook for '" + steps_next[0] + "' step");

			return modules.hooks.afterWait[steps_next[0]].run(results_transaction).then(
			    function() {
				
				logger.info("Waited for all records to enter step '" + steps_next[0] + "'");				
				return iterate(records.slice(), steps_next, results_transaction);

			    },
			    function(reason) {				

				logger.error('Processing interrupted by afterWait -hook');
				applyResults(results_transaction);
				throw reason;

			    }
			);

		    } else {

			logger.info('Transaction complete');
			modules.recordStore.toggleTransaction();

			results.transactions.push({
			    retries: retries
			});
			
			processed_records_indexes = processed_records_indexes.concat(records.map(function(record_data) {			    
			    return record_data.index;
			}));

			return Promise.resolve(results_transaction);

		    }

		} else {

		    logger.info('Starting processing of record index ' + record.index);
		    
		    return processRecord(record, PROCESSING_STEPS.indexOf(steps[0]), PROCESSING_STEPS.indexOf(steps[steps.length - 1]), results_transaction[records.length - list.length - 1]).then(
			function(result) {

			    if (results_transaction[records.length - list.length - 1]) {
				results_transaction[records.length - list.length - 1] = result;
			    } else {
				results_transaction.push(result);
			    }

			    return iterate(list, steps, results_transaction);

			},
			function(reason) {
			    if (config.processing.transaction.enableRollback) {
				return modules.recordStore.rollback().then(function() {

				    if (typeof config.processing.transaction.retryAfterRollback === 'boolean' && config.processing.transaction.retryAfterRollback === true || typeof config.processing.transaction.retryAfterRollback === 'number' && retries < config.processing.transaction.retryAfterRollback) {
					retries++;
					return startRecordDataTransaction(records);
				    } else {
					logger.error(
					    retries > 0
						? 'Transaction failed after ' + retries + ' retries'
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

	    logger.info('Starting transaction for ' + records.length + ' records');
	    modules.recordStore.toggleTransaction(1);	    
	    
	    /**
	     * @todo Does this backfire if our record data is Huge?
	     */
	    return iterate(records.slice(), steps_all.shift());

	}

	function processRecord(record_data, step_start, step_end, processing_results)
	{

	    function processStep(step, data)
	    {
		
		data = Array.isArray(data) ? data : [data];
		
		logger.debug("Entering '" + step + "' processing step");
		
		return modules.processors[step].run.apply(undefined, data);
		
	    }

	    function setResults(step, step_results)
	    {

		var index_additional = 1;

		step_results = Array.isArray(step_results) ? step_results : [step_results];
		processing_results.results.data.output = step === 'filter' ? record_data.data : step_results[0];

		switch (step) {
		case 'filter':
		    if (step_results[0] === undefined) {
			processing_results.results.skipped = 1;
		    }
		    break;
		case 'match':
		    if (step_results.length > 1) {
			processing_results.results.matched = step_results[1];
			index_additional = 2;
		    }
		    break;
		case 'merge':
		    if (step_results.length > 1) {
			processing_results.results.merged = step_results[1];
			index_additional = 2;
		    }
		    break;
		case 'load':
		    Object.assign(processing_results.results.recordStore, step_results[0]);
		    break;
		}

		if (step_results[index_additional] !== undefined) {
		    processing_results.results[step] = step_results[index_additional];
		}

	    }

	    function iterate(steps, step_results)
	    {

		var step = steps.shift();

		if (step === undefined) {

		    if (!processing_results.results.skipped && last_steps === true) {
			processing_results.results.processed = 1;
		    }

		    processing_results.stepResults = step_results;

		    return Promise.resolve(processing_results);

		} else {
		    return processStep(step, step_results === undefined ? record_data.data : step_results).then(function(step_results) {

			setResults(step, step_results);

			if (step === 'filter') {

			    if (step_results[0] === undefined) {
				return Promise.resolve({
				    results: processing_results
				});
			    } else {
				step_results[0] = record_data.data;
			    }

			}

			return iterate(steps, step_results);

		    });
		}

	    }

	    var steps = PROCESSING_STEPS.slice(step_start, step_end + 1),
	    last_steps = PROCESSING_STEPS.indexOf(config.processing.target) === step_end;

	    processing_results = processing_results !== undefined ? processing_results : {
		results: {
		    index: record_data.index,
		    data: {
			input: record_data.data
		    },
		    recordStore: {
			created: [],
			updated: [],
			deleted: []
		    },
		    merged: [],
		    matched: []
		}
	    };	    

	    return iterate(steps, processing_results.stepResults);

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
