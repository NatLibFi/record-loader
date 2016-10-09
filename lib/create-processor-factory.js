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
      './utils',
      'record-loader-prototypes/lib/logger/prototype',
      'record-loader-prototypes/lib/record-store/prototype',
      'record-loader-prototypes/lib/processors/filter/prototype',
      'record-loader-prototypes/lib/processors/preprocess/prototype',
      'record-loader-prototypes/lib/processors/match/prototype',
      'record-loader-prototypes/lib/processors/merge/prototype',
      'record-loader-prototypes/lib/processors/load/prototype'
    ], factory);
  } else if (typeof module === 'object' && module.exports) {
    module.exports = factory(
      require('es6-polyfills/lib/polyfills/promise'),
      require('es6-polyfills/lib/polyfills/object'),
      require('es6-shims/lib/shims/array'),
      require('./utils'),
      require('record-loader-prototypes/lib/logger/prototype'),
      require('record-loader-prototypes/lib/record-store/prototype'),
      require('record-loader-prototypes/lib/processors/filter/prototype'),
      require('record-loader-prototypes/lib/processors/preprocess/prototype'),
      require('record-loader-prototypes/lib/processors/match/prototype'),
      require('record-loader-prototypes/lib/processors/merge/prototype'),
      require('record-loader-prototypes/lib/processors/load/prototype')
    );
  }

}(this, factory));

function factory(Promise, Object, shim_array, utils, loggerFactory, recordStoreFactory, filterProcessorFactory, preprocessProcessorFactory, matchProcessorFactory, mergeProcessorFactory, loadProcessorFactory)
{

  'use strict';

  /**
   * @constant
   * @property {function} logger - xxx. No-op by {@link module:record-loader-prototypes/lib/logger/prototype|default}.
   * @property {function} recordStore - xxx. No-op by {@link module:record-loader-prototypes/lib/record-store/prototype|default}.
   * @property {object} processors
   * @property {function} processors.filter - Creates a filter processor object. No-op by {@link module:record-loader-prototypes/lib/processors/filter/prototype|default}.
   * @property {function} processors.preprocess - Creates a preprocess processor object. No-op by {@link module:record-loader-prototypes/lib/processors/preprocess/prototype|default}.
   * @property {function} processors.match - Creates a match processor object. No-op by {@link module:record-loader-prototypes/lib/processors/match/prototype|default}.
   * @property {function} processors.merge - Creates a merge processor object. No-op by {@link module:record-loader-prototypes/lib/processors/merge/prototype|default}.
   * @property {function} processors.load - Creates a load processor object. No-op by {@link module:record-loader-prototypes/lib/processors/load/prototype|default}.
   */
  var MODULES_DEFAULT = Object.seal(Object.freeze({
    logger: loggerFactory,
    recordStore: recordStoreFactory,
    processors: {
      filter: filterProcessorFactory,
      preprocess: preprocessProcessorFactory,
      match: matchProcessorFactory,
      merge: mergeProcessorFactory,
      load: loadProcessorFactory
    }
  })),
  OPTIONS_DEFAULT = Object.seal(Object.freeze({
    logLevel: 'info',
    modules: {
      processors: {}
    }      
  }));

  return function(workerpool)
  {

    /**
     * Factory
     * @alias module:record-loader/lib/processor-factory
     * @returns {function}
     * @param {object} [modules={}] - Factory functions used to construct the corresponding objects. See {@link module:record-loader/lib/processor-factory~MODULES_DEFAULT|MODULES_DEFAULT} for default values
     */
    var exports = function(modules)
    {
      
      var modules_run;
      
      function initialize(modules_run, options)
      {
        if (modules_run) {
          return modules_run;
        } else {

          options = utils.mergeObjects(OPTIONS_DEFAULT, typeof options === 'object' ? options : {});
          modules_run = utils.callFactories(utils.mergeObjects(MODULES_DEFAULT, typeof modules === 'object' ? modules : {}), options.modules);         

          modules_run.logger.setLevel(options.logLevel);
          modules_run.recordStore.setLogger(modules_run.logger.createInstance('record-store'));

          Object.keys(modules_run.processors).forEach(function(name) {
            modules_run.processors[name].setLogger(modules_run.logger.createInstance('processors/'+name));
          });

          modules_run.processors.match.setReadRecordStore(modules_run.recordStore.read);
          modules_run.processors.load.setRecordStoreMethods({
            create: modules_run.recordStore.create,
            read: modules_run.recordStore.read,
            update: modules_run.recordStore.update,
            delete: modules_run.recordStore.delete
          });

          return modules_run;

        }
      }

      function processRecord(state, target_step, options, exchange_data)
      {

        var steps = utils.PROCESSING_STEPS.slice(
          state.step ? utils.PROCESSING_STEPS.indexOf(state.step) + 1 : 0,
          utils.PROCESSING_STEPS.indexOf(target_step) + 1
        );

        function processResult(result)
        {
          return {
            processing: result,
            exchange: utils.getExchangeData(modules_run)
          };
        }

        function rejectWithError(error)
        {
          return Promise.reject(processResult(utils.createError(error)));
        }

        function formatState(state)
        {
          return Object.getOwnPropertyNames(state).filter(function(name) {
            return name !== 'step';
          }).reduce(function(product, name) {

            return Object.defineProperty(product, name, {
              configurable: true,
              enumerable: true,
              writable: true,
              value: state[name]
            });

          }, {});
        }

        function iterate(steps, state)
        {

          var fn_run_processor,
          step = steps.shift();

          function handleProcessorError(error)
          {
           return Promise.reject(utils.createError(error, state));
          }

          if (step) {

            fn_run_processor = modules_run.processors[step].run;

            modules_run.logger.debug('Running ' + step);

            switch (step) {
            case 'filter':

              return fn_run_processor(state.record).then(function(result) {
                return result.passes ? iterate(steps, Object.assign(state, utils.undefineProperties(result, ['passes', 'record']))) : Promise.resolve(Object.assign(
                  state,
                  utils.undefineProperties(result, ['passes', 'record']),
                  {
                    skipped: true
                  }
                ));
              }).catch(handleProcessorError);
              
            case 'preprocess':
              
              return fn_run_processor(state.record).then(function(result) {
                return iterate(steps, Object.assign(state, result));
              }).catch(handleProcessorError);
              
            case 'match':

              return fn_run_processor(state.record).then(function(results) {
                return iterate(steps, Object.assign(state, utils.undefineProperty(results, 'record')));
              }).catch(handleProcessorError);

            case 'merge': 

              return fn_run_processor(state.record, utils.clone(state.matchedRecords)).then(function(results) {
                return iterate(steps, Object.assign(state, results));
              }).catch(handleProcessorError);

            case 'load':

              return fn_run_processor(state.record, state.mergedRecords).then(function(results) {
                return iterate(steps, Object.assign(
                  state, 
                  typeof results === 'object' ? {
                    recordStore: results
                  } : {}
                ));
              }).catch(handleProcessorError);

            }

          } else {
            return Promise.resolve(state);
          }
          
        }

        try {

          modules_run = utils.setExchangeData(initialize(modules_run, options), exchange_data);

          modules_run.logger.info('Starting processing of a record');

          return iterate(steps, formatState(state)).then(function(result) {

            modules_run.logger.flush();
            return processResult(result);

          }).catch(function(error) {

            modules_run.logger.flush();            
            return utils.createError(error);

          });

        } catch (e) {
          return rejectWithError(e);
        }
        
      }

      workerpool.worker({
        processRecord: processRecord
      });
      
    };

    return exports;

  };

}
