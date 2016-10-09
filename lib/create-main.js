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
      'es6-polyfills/lib/polyfills/array',
      'es6-shims/lib/shims/array',
      './utils',
      'record-loader-prototypes/lib/logger/prototype',
      'record-loader-prototypes/lib/result-formatter/prototype',
      'record-loader-prototypes/lib/record-store/prototype',
      'record-loader-prototypes/lib/hooks/related-records-retrieved/prototype',
      'record-loader-prototypes/lib/hooks/related-records-matched/prototype'
    ], factory);
  } else if (typeof module === 'object' && module.exports) {
    module.exports = factory(
      require('es6-polyfills/lib/polyfills/promise'),
      require('es6-polyfills/lib/polyfills/object'),
      require('es6-polyfills/lib/polyfills/array'),
      require('es6-shims/lib/shims/array'),
      require('./utils'),
      require('record-loader-prototypes/lib/logger/prototype'),
      require('record-loader-prototypes/lib/result-formatter/prototype'),
      require('record-loader-prototypes/lib/record-store/prototype'),
      require('record-loader-prototypes/lib/hooks/related-records-retrieved/prototype'),
      require('record-loader-prototypes/lib/hooks/related-records-matched/prototype')
    );
  }

}(this, factory));

/**
 * @module record-loader/lib/main
 */
function factory(Promise, Object, Array, shim_array, utils, loggerFactory, resultFormatterFactory, recordStoreFactory, relatedRecordsRetrievedHookFactory, relatedRecordsMatchedHookFactory)
{

  'use strict';

  return function(workerpool)
  {

    /**
     * Factory
     * @alias module:record-loader/lib/main
     * @returns {module:record-loader/lib/main~loadRecords}
     * @param {object} [modules={}] - Factory functions used to construct the corresponding objects. See {@link module:record-loader/lib/main~MODULES_DEFAULT|MODULES_DEFAULT} for default values
     * @param {string} script_processor - Path to script that calls the {@link module:record-loader/lib/processor-factory|processor factory}
     */
    var exports = function(modules, script_processor)
    {
      
      /**
       * @constant
       * @property {function} logger - Creates a logger factory function. No-op by {@link module:record-loader-prototypes/lib/logger/prototype|default}.
       * @property {function} resultFormatter - Creates a result formatter object. No-op by {@link module:record-loader-prototypes/lib/result-formatter/prototype|default}.
       * @property {function} recordStore - xxx. No-op by {@link module:record-loader-prototypes/lib/record-store/prototype|default}.
       * @property {object} hooks
       * @property {function} hooks.relatedRecordsRetrieved - Creates a relatedRecordsRetrieved hook. No-op by {@link module:record-loader-prototypes/lib/hooks/related-records-retrieved/prototype|default}.
       * @property {function} hooks.relatedRecordsMatched - Creates a relatedRecordsMatched hook. No-op by {@link module:record-loader-prototypes/lib/hooks/related-records-matched/prototype|default}.
       */
      var MODULES_DEFAULT = Object.seal(Object.freeze({
        logger: loggerFactory,
        resultFormatter: resultFormatterFactory,
        recordStore: recordStoreFactory,
        hooks: {
          relatedRecordsRetrieved: relatedRecordsRetrievedHookFactory,
          relatedRecordsMatched: relatedRecordsMatchedHookFactory
        }
      })),
      RESULT_LEVELS = resultFormatterFactory.getLevels(),
      /**
       * @constant
       * @property {string} [logLevel='info'] - Logging level. Can be one of the following: trace, debug, info, warn, error.
       * @property {string} [target='load'] - Target processing step. Can be one of the following: filter, preprocess, match, merge, load
       * @property {string} [abortOnError=true] - Whether to abort if processing of a record fails
       * @property {string} [resultLevel='total'] - Amount of data returned in the processing results. Can be one of the following: total, record, debug
       * @property {number} [parallel] - Number of workers/processed dispatched for parallel processing. Set 0 to disable parallel processing. Defaults to number of processors - 1
       * @property {boolean} [rollback=false] - xxx
       * @property {object} modules - Configuration options for modules. The type of the options depends on the module in use. All module options default to undefined.
       * @property {*} modules.logger
       * @property {*} modules.resultFormatter
       * @property {*} modules.recordSet
       * @property {*} modules.recordStore
       * @property {object} modules.processors
       * @property {*} modules.processors.filter
       * @property {*} modules.processors.preprocess
       * @property {*} modules.processors.match
       * @property {*} modules.processors.merge
       * @property {*} modules.processors.load
       * @property {object} modules.hooks
       * @property {*} modules.hooks.relatedRecordsRetrieved
       * @property {*} modules.hooks.relatedRecordsMatched       
       */
      OPTIONS_DEFAULT = Object.seal(Object.freeze({
        logLevel: 'info',
        target: 'load',
        abortOnError: true,
        resultLevel: RESULT_LEVELS.statistics | RESULT_LEVELS.recordMetaData,
        rollback: false,
        modules: {              
          processors: {},
          hooks: {}
        }
      }));

      modules = utils.mergeObjects(MODULES_DEFAULT, typeof modules === 'object' ? modules : {});

      if (!modules.hasOwnProperty('recordSet')) {
        throw new Error('Record set module is mandatory');
      } else {
        /**
         * @callback loadRecords
         * @param {*} input_data - Input data locator. Type dedends on the {@link module:record-loader-prototypes/lib/record-set/prototype|record set} module in use.
         * @param {object} options - Configuration options. See {@link module:record-loader/lib/main~OPTIONS_DEFAULT|OPTIONS_DEFAULT} for default values.
         * @returns {object}
         */
        return function(input_data, options)
        {

          var modules_run, pool_workers;

          function postProcess(result)
          {

            pool_workers.clear(true);
            if (result instanceof Error) {
              modules_run.logger.error('Processing failed');
              throw result;
            } else {
              modules_run.logger.info('Processing completed succesfully');              
              return result;
            }

          }

          function initializeOptions(options)
          {

            options = Object.assign(utils.mergeObjects(OPTIONS_DEFAULT, typeof options === 'object' ? options : {}));

            return Object.seal(Object.freeze(Object.assign(options, {
              parallel: typeof options.parallel === 'number' ? options.parallel || 1 : (workerpool.cpus || 4) - 1
            })));

          }

          function initializeModules(factories_modules, options)
          {

            function callSetters(modules)
            {

              modules.logger
                .setLevel(options.logLevel)
                .setAutoFlush(true);

              modules.recordSet.setLogger(modules.logger.createInstance('record-set').setAutoFlush(true));
              modules.recordStore.setLogger(modules.logger.createInstance('record-store').setAutoFlush(true));

              modules.resultFormatter
                .setLevel(options.resultLevel)
                .setLogger(modules.logger.createInstance('result-formatter').setAutoFlush(true));
              
              Object.keys(modules.hooks).forEach(function(name) {
                modules.hooks[name]
                  .setLogger(modules.logger.createInstance('hooks/' + name).setAutoFlush(true))
                  .setRecordStore(modules.recordStore);
              });

              return modules;

            }

            return callSetters(utils.callFactories(factories_modules, options.modules));
            
          }

          options = initializeOptions(options);
          modules_run = initializeModules(modules, options);
          pool_workers = workerpool.pool(script_processor, {
            maxWorkers: options.parallel
          });
          
          return pool_workers.proxy().then(function(worker) {

            function isWorkerValid(worker)
            {
              return typeof worker === 'object' && typeof worker.processRecord === 'function';
            }

            function processRecordSet(total_results)
            {
              
              var fn_related_records_pipeline;

              function dispatchWorker(record, target_step, options)
              {

                function processResult(result)
                {
                  modules_run = utils.setExchangeData(modules_run, result.exchange);
                  return result.processing;
                }
                
                return worker.processRecord(record, target_step, options, utils.getExchangeData(modules_run))
                  .then(processResult, processResult);

              }

              function buildRelatedRecordsPipeline(target)
              {

                var target_index = utils.PROCESSING_STEPS.indexOf(target),
                match_index = utils.PROCESSING_STEPS.indexOf('match');
                
                function process(records, target)
                {
                  return new Promise(function(resolveCallback, rejectCallback) {

                    var results = [];

                    records.forEach(function(record, index) {

                      function handleResult(result)
                      {
                        if (records.length === results.push({
                          index: index,
                          result: result
                        })) {
                          
                          results = results.sort(function(a, b) {
                            
                            return a.index - b.index;
                            
                          }).map(function(result) {
                            return result.result;
                          });

                          if (results.some(function(result) {
                            return result.failed;
                          })) {                            
                            rejectCallback(results.map(utils.createError));
                          } else {
                            resolveCallback(results);
                          }

                        }
                      }

                      dispatchWorker(record, target, options).then(handleResult, handleResult);
                        
                    });

                  });
                }

                function processLoad(records)
                {
                                    
                  function iterate(records, results) {

                    var record = records.shift();

                    function next(result)
                    {
                      return iterate(records, results ? results.concat(result) : [result]);
                    }

                    if (record) {

                      return dispatchWorker(record, 'load', options).then(next, function(error) {
                        return results.concat(error);
                      });

                    } else {
                      return results;
                    }

                  }

                  return iterate(records);

                }

                function noop(records)
                {
                  return Promise.resolve(records);
                }

                return function(records) {
                  return process(records, target_index <= match_index ? target : 'match')
                    .then(target_index >= match_index ? modules_run.hooks.relatedRecordsMatched.run : noop)                    
                    .then(target_index <= match_index ? noop : function(records) {
                      return process(records, 'merge');
                    })
                    .then(target_index < utils.PROCESSING_STEPS.indexOf('load') ? noop : processLoad);
                };

              }

              function getRecords(retrieved_records)
              {

                retrieved_records = retrieved_records || [];

                return retrieved_records.length === 0 || retrieved_records.length < options.parallel ? modules_run.recordSet.get().then(function(records) {

                  return records ? getRecords(retrieved_records.concat([records])) : retrieved_records;

                }) : retrieved_records;
                
              }
               

              function doRollback(error)
              {

                modules_run.logger.info('Rolling back changes in the record store');

                return modules_run.recordStore.rollback(error.recordStore).then(function(record_store_state) {                  

                  /**
                   * Add record store results to the error if available. Otherwise remove the recordStore property since rollback cancelled all previous record store operations
                   */
                  return record_store_state ? Object.assign(error, {
                    recordStore: record_store_state
                  }) : Object.keys(error).filter(function(key) {
                    return key !== 'recordStore';
                  }).reduce(function(result, key) {
                    
                    return Object.defineProperty(result, key, {
                      configurable: true,
                      writable: true,
                      enumerable: true,
                      value: error[key],
                    });

                  }, {});

                });

              }
       
              function processRecord(record)
              {
                return dispatchWorker(record, options.target, options).then(Array.of, function(error) {

                  error = typeof error === 'object' && error.hasOwnProperty('failed') ? error : utils.createError(error);
                  return error.recordStore && options.rollback ? doRollback(error).then(Array.of) : [error];

                });
              }                                           
              
              function processRelatedRecords(records)
              {

                modules_run.logger.info('Processing ' + records.length  +' related records');

                return modules_run.hooks.relatedRecordsRetrieved.run(records).then(fn_related_records_pipeline).then(function(results) {                 

                  function iterate(records, results)
                  {
                    
                    var record = records.shift();

                    results = results || [];

                    return record ?
                      record.failed && record.recordStore && options.rollback ?
                      doRollback(record).then(function(result) {
                        
                        return iterate(records, results.concat(result));
                        
                      }) : iterate(records, results.concat(record))
                    : Promise.resolve(results);

                  }

                  return results.some(function(result) {
                    
                    return result.failed;
                    
                  }) ? iterate(results.map(function(result) {
                    
                    return Object.assign(result, {
                      failed: true
                    });
                    
                  })) : results;
                  
                });

              }
                  

              function checkForErrors(result_sets)
              {
               return !result_sets || result_sets.some(function(results) {
                 return results.some(function(result) {
                   return result.failed;
                 });
               });
              }

              function processRecords(record_sets)
              {
                return record_sets.length > 0 ? Promise.all(record_sets.map(function(records) {

                  records = records.map(function(record) {
                    return {
                      record: record
                    };
                  });

                  return records.length > 1 ? processRelatedRecords(records) : processRecord(records.shift());

                })) : undefined;
              }
              
              total_results = total_results || [];
              fn_related_records_pipeline = buildRelatedRecordsPipeline(options.target);

              return getRecords().then(processRecords).then(function(processing_results) {

                return !processing_results || checkForErrors(processing_results) && options.abortOnError ? total_results.concat(processing_results || []) : processRecordSet(total_results.concat(processing_results));
              });

            }

            function createFinalResults(result_sets)
            {

              var results, final_results;

              function getAmount(testCallback)
              {
                return results.reduce(function(product, result) {
                  return testCallback(result) ? product + 1 : product; 
                }, 0);
              }

              function getRecordStoreAmount(name)
              {
                return results.reduce(function(product, result) {                  

                  return product + (result.recordStore && result.recordStore[name] ? result.recordStore[name].length : 0);
                  
                }, 0);
              }

              if (result_sets instanceof Error) {
                modules_run.logger.error('Unhandled error occurred');
                return Promise.reject(result_sets);
              } else {

                results = result_sets.reduce(function(product, set) {
                  return product.concat(set);
                }, []);
                final_results = Object.assign(
                  {
                    status: options.abortOnError && results.some(function(result) {
                      return result.failed;
                    }) ? 'aborted': 'ok'
                  },
                  options.resultLevel & RESULT_LEVELS.statistics ? {
                    statistics: {
                      processed: results.length,
                      succeeded: getAmount(function(result) {
                        return !(result.skipped || result.failed);
                      }),
                      skipped: getAmount(function(result) {
                        return result.skipped;
                      }),
                      failed: getAmount(function(result) {
                        return result.failed;
                      }),
                      recordStore: {
                        created: getRecordStoreAmount('created'),
                        updated: getRecordStoreAmount('updated'),
                        deleted: getRecordStoreAmount('deleted')
                      }
                    }
                  } : {},
                  options.resultLevel & RESULT_LEVELS.recordMetaData || options.resultLevel & RESULT_LEVELS.recordData ? {

                    records: results
                      .reduce(function(product, results) {
                        return product.concat(results);
                      }, [])
                      .map(function(result) {

                        return Object.keys(result).filter(function(key) {
                          
                          switch (key) {
                          case 'message':
                          case 'stack':
                          case 'failed':
                            return true;
                          case 'record':
                            return options.resultLevel & RESULT_LEVELS.recordData;
                          default:
                            return options.resultLevel & RESULT_LEVELS.recordMetaData;
                          }
                          
                        }).reduce(function(product, key) {
                          
                          return Object.defineProperty(product, key, {
                            configurable: true,
                            enumerable: true,
                            writable: true,
                            value: result[key]
                          });
                          
                        }, {});

                      })
                    
                  } : {}
                );
                
                try {                    
                  return modules_run.resultFormatter.run(final_results).catch(function(error) {
                    return Promise.reject(Object.assign(final_results, utils.createError(error, {
                      status: 'aborted'
                    })));
                  });
                } catch (error) {
                  return Promise.reject(Object.assign(final_results, utils.createError(error, {
                    status: 'aborted'
                  })));
                }

              }
              
            }

            if (isWorkerValid(worker)) {
              return modules_run.recordSet.initialize(input_data).then(function() {

                modules_run.logger.info('Starting processing of record set');

                return processRecordSet().then(createFinalResults, createFinalResults);

              });
            } else {
              return Promise.reject(new Error('Worker is not valid'));
            }
            
          }).then(postProcess, postProcess);
          
        };
        
      }

    };

    return exports;

  };

}
