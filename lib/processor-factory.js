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
	    'workerpool/dist/workerpool',
	    './utils',
	    'record-loader-prototypes/lib/logger/prototype',
	    'record-loader-prototypes/lib/converters/prototype'
	], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory(
	    require('es6-polyfills/lib/polyfills/promise'),
	    require('es6-polyfills/lib/polyfills/object'),
	    require('es6-shims/lib/shims/array'),
	    require('workerpool'),
	    require('./utils'),
	    require('record-loader-prototypes/lib/logger/prototype'),
	    require('record-loader-prototypes/lib/converters/prototype')
	);
    }

}(this, factory));

function factory(Promise, Object, shim_array, workerpool, utils, createLoggerFactory, converterFactory)
{

    'use strict';

    return function(modules)
    {

	function initialize(parameters)
	{

	    function callSetters(modules)
	    {
		return Object.keys(modules)
		    .filter(function(key) {
			return key === 'recordStore' || key === 'processors';
		    })
		    .reduce(function(result, key) {
			
			if (key === 'processors') {
			    Object.keys(result.processors).forEach(function(key) {

				result.processors[key].setConverter(result.converter);

				if (key === 'match' || key === 'load') {
				    result.processors[key].setRecordStore(result.recordStore);
				}
				
			    });
			} else {
			    result.recordStore.setConverter(result.converter);
			}
			
			return result;
			
		    }, modules);
	    }

	    if (!initialized) {

		modules = callSetters(utils.initializeModules(utils.assignDefaultModules(modules, modules_default), parameters));
		modules.logger = modules.logger();
		initialized = 1;

	    }

	}

	function handleRecordProcessing(record, parameters, steps, results)
	{

	    function getRecordResult(results, record)
	    {
		return shim_array.find(results, function(result) {
		    return result.results.index === record.index;
		});
	    }

	    var step_target = parameters.processing.target;
	    var spec_steps = {
		stepStart: PROCESSING_STEPS.indexOf(steps[0]),
		stepEnd: PROCESSING_STEPS.indexOf(steps[steps.length - 1])
	    };

	    initialize(parameters);

	    spec_steps = Object.assign(spec_steps, {
		steps: PROCESSING_STEPS.slice(spec_steps.stepStart, spec_steps.stepEnd + 1),
		areLastSteps: PROCESSING_STEPS.indexOf(step_target) === spec_steps.stepEnd
	    });

	    modules.logger.info('Starting processing of record index ' + record.index);
	    	    
	    return processRecord(record, spec_steps, getRecordResult(results, record)).then(function(result) {
		return results.concat(result);
	    });

	}

	function processRecord(record_data, spec_steps, processing_results)
	{

	    function processStep(step, data)
	    {
		
		data = Array.isArray(data) ? data : [data];
		
		modules.logger.debug("Entering '" + step + "' processing step");
		
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

		    if (!processing_results.results.skipped && spec_steps.areLastSteps === true) {
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

	    return iterate(spec_steps.steps, processing_results.stepResults);

	}

	var initialized,
	PROCESSING_STEPS = ['filter', 'preprocess', 'match', 'merge', 'load'],
	modules_default = {
	    logger: createLoggerFactory,
	    converter: converterFactory
	};

	workerpool.worker({
	    initialize: initialize,
	    handleRecordProcessing: handleRecordProcessing,
	    processRecord: processRecord
	});

    };

}