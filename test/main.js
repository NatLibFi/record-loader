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

(function(root, factory) {

  'use strict';

  if (typeof define === 'function' && define.amd) {
    define([
      'es6-polyfills/lib/polyfills/promise',
      'es6-polyfills/lib/polyfills/object',
      'chai-as-promised',
      'simple-mock',
      '../lib/utils',
      'record-loader-prototypes/lib/logger/prototype',
      'record-loader-prototypes/lib/record-set/prototype',
      'record-loader-prototypes/lib/record-store/prototype',
      'record-loader-prototypes/lib/result-formatter/prototype',
      'record-loader-prototypes/lib/hooks/related-records-retrieved/prototype',
      'record-loader-prototypes/lib/hooks/related-records-matched/prototype',
      '../lib/create-main'
    ], factory);
  } else if (typeof module === 'object' && module.exports) {
    module.exports = factory(
      require('es6-polyfills/lib/polyfills/promise'),
      require('es6-polyfills/lib/polyfills/object'),
      require('chai-as-promised'),
      require('simple-mock'),
      require('../lib/utils'),
      require('record-loader-prototypes/lib/logger/prototype'),
      require('record-loader-prototypes/lib/record-set/prototype'),
      require('record-loader-prototypes/lib/record-store/prototype'),
      require('record-loader-prototypes/lib/result-formatter/prototype'),
      require('record-loader-prototypes/lib/hooks/related-records-retrieved/prototype'),
      require('record-loader-prototypes/lib/hooks/related-records-matched/prototype'),
      require('../lib/create-main')
    );
  }

}(this, factory));

function factory(Promise, Object, chaiAsPromised, simple, utils, loggerFactory, recordSetFactory, recordStoreFactory, resultFormatterFactory, relatedRecordsRetrievedHookFactory, relatedRecordsMatchedHookFactory, createLoadRecordsFactory)
{

  'use strict';

  return function(chai)
  {

    var expect = chai.expect;
    
    simple.Promise = Promise;
    
    chai.use(chaiAsPromised);

    describe('create-main', function() {

      describe('create-factory', function() {

        it('Should be a function', function() {
          expect(createLoadRecordsFactory).to.be.a('function');
        });

        it('Should return a function', function() {
          expect(createLoadRecordsFactory()).to.be.a('function');
        });

        describe('factory', function() {

          var RESULT_LEVELS = resultFormatterFactory.getLevels(),
          loadRecordsFactoryNoop = createLoadRecordsFactory({
            pool: simple.stub().returnWith({
              clear: simple.stub(),
              proxy: simple.stub().resolveWith({})
            })
          });

          it('Should be a function', function() {
            expect(loadRecordsFactoryNoop).to.be.a('function');
          });
          
          it('Should throw because record set module is missing', function() {
            expect(loadRecordsFactoryNoop).to.throw(Error, /^Record set module is mandatory$/);
          });
          
          it('Should return a function', function() {
            expect(loadRecordsFactoryNoop({
              recordSet: recordSetFactory
            })).to.be.a('function');
          });
          
          describe('function', function() {
            
            function resultFormatterFactoryNoContext()
            {

              var obj = resultFormatterFactory.apply(undefined, arguments);
              
              simple.mock(obj, 'setLevel');
              simple.mock(obj, 'run')
                .callFn(function(results) {

                  return Promise.resolve(Object.assign(results, {
                    records: results.records.map(function(record) {
                      
                      return Object.keys(record)
                        .filter(function(key) {
                          return ['stack', 'sourceURL', 'line'].indexOf(key) < 0;
                        })
                        .reduce(function(product, key) {
                          return Object.defineProperty(product, key, {
                            enumerable: true,
                            value: record[key]
                          });
                        }, {});
                      
                    })
                  }));

                });
              
              return obj;

            }

            it('Should be rejected because worker is not valid', function() {
              return expect(loadRecordsFactoryNoop({
                recordSet: recordSetFactory
              })()).to.be.rejectedWith(Error, /^Error: Worker is not valid$/);
            });        

            it('Should process the records with default options', function() {

              var mock_result_formatter, mock_logger, mock_record_store,
              spy_process_record = simple.spy().resolveWith({
                processing: {
                  record: {},
                  recordStore: {
                    created: [{}]
                  }
                }
              }).rejectWith(utils.createError(new Error('foobar'), {

                record: {},
                step: 'filter'

              })),
              spy_pool = simple.spy(),
              mock_workerpool = {
                
                cpus: 2,
                pool: spy_pool.returnWith({
                  clear: simple.stub(),
                  proxy: simple.stub().resolveWith({
                    processRecord: spy_process_record
                  })
                })                      

              };
              
              return createLoadRecordsFactory(mock_workerpool)({
                logger: simple.stub(function() {

                  mock_logger = loggerFactory.apply(undefined, arguments);

                  simple.mock(mock_logger, 'setLevel');
                  
                  return mock_logger;

                }),
                recordStore: simple.stub(function() {

                  mock_record_store = recordStoreFactory.apply(undefined, arguments);

                  simple.mock(mock_record_store, 'rollback');

                  return mock_record_store;

                }),
                recordSet: simple.stub(function() {

                  var obj = recordSetFactory.apply(undefined, arguments);

                  simple.mock(obj, 'get')
                    .resolveWith([{}])
                    .resolveWith([{}])
                    .resolveWith();

                  return obj;

                }),
                resultFormatter: simple.stub(function() {
                  return mock_result_formatter = resultFormatterFactoryNoContext.apply(undefined, arguments) /* jshint -W093 */;
                })
              })().then(function(results) {

                expect(results).to.eql({
                  status: 'aborted',
                  statistics: {
                    processed: 2,
                    succeeded: 1,
                    skipped: 0,
                    failed: 1,
                    recordStore: {
                      created: 1,
                      updated: 0,
                      deleted: 0
                    }
                  },
                  records: [
                    {
                      recordStore: {
                        created: [{}]
                      }
                    },
                    {
                      failed: true,
                      message: 'foobar',
                      step: 'filter'
                    }
                  ]
                });

                expect(mock_logger.setLevel.callCount).to.equal(1);
                expect(mock_logger.setLevel.calls[0].args).to.eql(['info']);

                expect(mock_result_formatter.setLevel.callCount).to.equal(1);
                expect(mock_result_formatter.setLevel.calls[0].args).to.eql([RESULT_LEVELS.statistics | RESULT_LEVELS.recordMetaData]);

                expect(mock_record_store.rollback.callCount).to.equal(0);                
                expect(spy_process_record.callCount).to.equal(2);

              });
              
            });      

            it('Should process the records with default options (Related records)', function() {

              var mock_result_formatter, mock_logger, mock_record_store,
              spy_process_record = simple.spy()
                .resolveWith({
                  processing: {
                    record: {},
                    recordStore: {
                      created: [{}]
                    }
                  }
                })
                .rejectWith(utils.createError(new Error('foobar'), {
                  
                  record: {},
                  step: 'filter'
                  
                })),               
              spy_pool = simple.spy(),
              mock_workerpool = {
                
                cpus: 2,
                pool: spy_pool.returnWith({
                  clear: simple.stub(),
                  proxy: simple.stub().resolveWith({
                    processRecord: spy_process_record
                  })
                })                      

              };
              
              return createLoadRecordsFactory(mock_workerpool)({
                logger: simple.stub(function() {

                  mock_logger = loggerFactory.apply(undefined, arguments);

                  simple.mock(mock_logger, 'setLevel');
                  
                  return mock_logger;

                }),
                recordStore: simple.stub(function() {

                  mock_record_store = recordStoreFactory.apply(undefined, arguments);

                  simple.mock(mock_record_store, 'rollback');

                  return mock_record_store;

                }),
                recordSet: simple.stub(function() {

                  var obj = recordSetFactory.apply(undefined, arguments);

                  simple.mock(obj, 'get')
                    .resolveWith([{}, {}])
                    .resolveWith();

                  return obj;

                }),
                resultFormatter: simple.stub(function() {
                  return mock_result_formatter = resultFormatterFactoryNoContext.apply(undefined, arguments) /* jshint -W093 */;
                })
              })().then(function(results) {

                expect(results).to.eql({
                  status: 'aborted',
                  statistics: {
                    processed: 2,
                    succeeded: 0,
                    skipped: 0,
                    failed: 2,
                    recordStore: {
                      created: 1,
                      updated: 0,
                      deleted: 0
                    }
                  },
                  records: [
                    {
                      failed: true,
                      recordStore: {
                        created: [{}]
                      }
                    },
                    {
                      failed: true,
                      message: 'foobar',
                      step: 'filter'
                    }
                  ]
                });

                expect(mock_logger.setLevel.callCount).to.equal(1);
                expect(mock_logger.setLevel.calls[0].args).to.eql(['info']);

                expect(mock_result_formatter.setLevel.callCount).to.equal(1);
                expect(mock_result_formatter.setLevel.calls[0].args).to.eql([RESULT_LEVELS.statistics | RESULT_LEVELS.recordMetaData]);

                expect(mock_record_store.rollback.callCount).to.equal(0);
                expect(spy_process_record.callCount).to.equal(2);

              });
              
            });      

            it('Should call logger#setLevel with specified value', function() {

              var mock_logger,
              mock_workerpool = {

                cpus: 2,                
                pool: simple.stub().returnWith({
                  clear: simple.stub(),
                  proxy: simple.stub().resolveWith({
                    processRecord: simple.stub().resolveWith({
                      processing: {}
                    })
                  })
                })                      
                
              };
              
              return createLoadRecordsFactory(mock_workerpool)({
                logger: simple.stub(function() {

                  mock_logger = loggerFactory.apply(undefined, arguments);

                  simple.mock(mock_logger, 'setLevel');
                  
                  return mock_logger;

                }),
                recordSet: simple.stub(function() {

                  return Object.assign(recordSetFactory.apply(undefined, arguments), {
                    get: simple.stub().resolveWith([{}]).resolveWith()
                  });

                })
              })(undefined, {
                logLevel: 'debug'
              }).then(function() {
                
                expect(mock_logger.setLevel.callCount).to.equal(1);
                expect(mock_logger.setLevel.calls[0].args).to.eql(['debug']);

              });

            });

            it('Should handle unexpected errors', function() {

              var spy_pool = simple.spy(),
              mock_workerpool = {
                
                cpus: 2,
                pool: spy_pool.returnWith({
                  clear: simple.stub(),
                  proxy: simple.stub().resolveWith({
                    processRecord: simple.stub().throwWith(new Error('foobar'))
                  })
                })                      

              };
              
              return createLoadRecordsFactory(mock_workerpool)({
                resultFormatter: resultFormatterFactoryNoContext,
                recordSet: simple.stub(function() {

                  var obj = recordSetFactory.apply(undefined, arguments);

                  simple.mock(obj, 'get').resolveWith([{}]).resolveWith();

                  return obj;

                })
              })().catch(function(result) {
                expect(result).to.be.an('error').and.to.have.property('message', 'foobar');
              });

            });

            it('Should handle unformatted errors', function() {

              var spy_pool = simple.spy(),
              mock_workerpool = {
                
                cpus: 2,
                pool: spy_pool.returnWith({
                  clear: simple.stub(),
                  proxy: simple.stub().resolveWith({
                    processRecord: simple.stub().rejectWith(new Error('foobar'))
                  })
                })                      

              };
              
              return createLoadRecordsFactory(mock_workerpool)({
                resultFormatter: resultFormatterFactoryNoContext,
                recordSet: simple.stub(function() {

                  var obj = recordSetFactory.apply(undefined, arguments);

                  simple.mock(obj, 'get').resolveWith([{}]).resolveWith();

                  return obj;

                })
              })().then(function(result) {

                /* We are not going to compare stack traces */
                delete result.stack;

                expect(result).to.eql({
                  status: 'aborted',
                  statistics: {
                    processed: 1,
                    succeeded: 0,
                    skipped: 0,
                    failed: 1,
                    recordStore: {
                      created: 0,
                      updated: 0,
                      deleted: 0
                    }
                  },
                  records: [{
                    failed: true,
                    message: 'foobar'
                  }]
                });
              });

            });

            it('Should reject because running result formatter fails', function() {

              var spy_pool = simple.spy(),
              mock_workerpool = {
                
                cpus: 2,
                pool: spy_pool.returnWith({
                  clear: simple.stub(),
                  proxy: simple.stub().resolveWith({
                    processRecord: simple.stub().resolveWith({
                      processing: {
                        record: {},
                        recordStore: {
                          created: [{}]
                        }
                      }
                    })
                  })
                })                      

              };
              
              return createLoadRecordsFactory(mock_workerpool)({
                recordSet: simple.stub(function() {

                  var obj = recordSetFactory.apply(undefined, arguments);

                  simple.mock(obj, 'get').resolveWith([{}]).resolveWith();

                  return obj;

                }),
                resultFormatter: simple.stub(function() {

                  return Object.assign(resultFormatterFactory.apply(undefined, arguments), {
                    run: simple.stub().rejectWith(new Error('foobar'))
                  });

                })
              })().then(function(result) {

                /* We are not going to compare stack traces */
                delete result.stack;

                expect(result).to.eql({
                  failed: true,
                  message: 'foobar',
                  status: 'aborted',
                  statistics: {
                    processed: 1,
                    succeeded: 1,
                    skipped: 0,
                    failed: 0,
                    recordStore: {
                      created: 1,
                      updated: 0,
                      deleted: 0
                    }
                  },
                  records: [{
                      recordStore: {
                        created: [{}]
                      }
                  }]                  
                });
              });

            });
            
            describe('processing', function() {
              
              it('Should run the processing only up to the specified step', function() {

                var spy_process_record = simple.spy(function(result) {
                  return Promise.resolve(result.hasOwnProperty('processing') ? result : {
                    processing: result
                  });
                }),
                mock_workerpool = {
                  
                  cpus: 2,
                  pool: simple.stub().returnWith({
                    clear: simple.stub(),
                    proxy: simple.stub().resolveWith({
                      processRecord: spy_process_record
                    })
                  })                      
                  
                };
                
                return createLoadRecordsFactory(mock_workerpool)({
                  recordSet: simple.stub(function() {
                    return Object.assign(recordSetFactory.apply(undefined, arguments), {
                      get: simple.stub().resolveWith([{}]).resolveWith()
                    });
                  })
                })(undefined, {
                  target: 'preprocess'
                }).then(function() {
                  
                  expect(spy_process_record.callCount).to.equal(1);
                  expect(spy_process_record.calls[0].args).to.eql([
                    {
                      record: {}
                    },
                    'preprocess',
                    {
                      abortOnError: true,
                      logLevel: 'info',
                      parallel: 1,
                      resultLevel: RESULT_LEVELS.statistics | RESULT_LEVELS.recordMetaData,
                      rollback: false,
                      target: 'preprocess',
                      modules: {
                        hooks: {},
                        processors: {}
                      }
                    },
                    {
                    }
                  ]);

                });

              });

              it('Should run the processing only up to the specified step (Related records)', function() {

                var spy_process_record = simple.spy(function(result) {
                  return Promise.resolve(result.hasOwnProperty('processing') ? result : {
                    processing: result
                  });
                }),
                mock_workerpool = {
                  
                  cpus: 2,
                  pool: simple.stub().returnWith({
                    clear: simple.stub(),
                    proxy: simple.stub().resolveWith({
                      processRecord: spy_process_record
                    })
                  })                      
                  
                };
                
                return createLoadRecordsFactory(mock_workerpool)({
                  recordSet: simple.stub(function() {
                    return Object.assign(recordSetFactory.apply(undefined, arguments), {
                      get: simple.stub().resolveWith([{}, {}]).resolveWith()
                    });
                  })
                })(undefined, {
                  target: 'preprocess'
                }).then(function() {
                  
                  expect(spy_process_record.callCount).to.equal(2);
                  expect(spy_process_record.calls[0].args).to.eql([
                    {
                      record: {}
                    },
                    'preprocess',
                    {
                      abortOnError: true,
                      logLevel: 'info',
                      parallel: 1,
                      resultLevel: RESULT_LEVELS.statistics | RESULT_LEVELS.recordMetaData,
                      rollback: false,
                      target: 'preprocess',
                      modules: {
                        hooks: {},
                        processors: {}
                      }
                    },
                    {}
                  ]);

                });

              });

              it('Should return results with the specified results level', function() {

                var mock_result_formatter,
                mock_workerpool = {
                                    
                  cpus: 2,
                  pool: simple.stub().returnWith({
                    clear: simple.stub(),
                    proxy: simple.stub().resolveWith({
                      processRecord: simple.spy(function(result) {
                        return Promise.resolve(result.hasOwnProperty('processing') ? result : {
                          processing: result
                        });
                      })
                    })
                  })                  
                  
                };

                return createLoadRecordsFactory(mock_workerpool)({
                  resultFormatter: simple.stub(function() {

                    mock_result_formatter = resultFormatterFactoryNoContext.apply(undefined, arguments);

                    simple.mock(mock_result_formatter, 'setLevel');
                    
                    return mock_result_formatter;

                  }),
                  recordSet: simple.stub(function() {
                    return Object.assign(recordSetFactory.apply(undefined, arguments), {
                      get: simple.stub().resolveWith([{}]).resolveWith()
                    });
                  })
                })(undefined, {

                  resultLevel: 'record'

                }).then(function() {

                  expect(mock_result_formatter.setLevel.callCount).to.equal(1);
                  expect(mock_result_formatter.setLevel.calls[0].args).to.eql(['record']);

                });

              });

              it('Should not abort the processing when a record fails', function() {

                var mock_workerpool = {
                                    
                  cpus: 2,
                  pool: simple.stub().returnWith({
                    clear: simple.stub(),
                    proxy: simple.stub().resolveWith({
                      processRecord: simple.stub().rejectWith({
                        processing: utils.createError(new Error('foobar'), {
                          record: {},
                          step: 'preprocess'
                        })
                      }).resolveWith({
                        processing: {
                          record: {},
                          recordStore: {
                            created: [{}]
                          }
                        }
                      })
                    })
                  })

                };
                
                return expect(createLoadRecordsFactory(mock_workerpool)({
                  resultFormatter: resultFormatterFactoryNoContext,
                  recordSet: simple.stub(function() {
                    return Object.assign(recordSetFactory.apply(undefined, arguments), {
                      get: simple.stub()
                        .resolveWith([{}])
                        .resolveWith([{}])
                        .resolveWith()
                    });
                  })
                })(undefined, {

                  abortOnError: false

                })).to.be.eventually.eql({
                  status: 'ok',
                  statistics: {
                    processed: 2,
                    succeeded: 1,
                    skipped: 0,
                    failed: 1,
                    recordStore: {
                      created: 1,
                      updated: 0,
                      deleted: 0
                    }
                  },
                  records: [
                    {
                      step: 'preprocess',
                      failed: true,
                      message: 'foobar'
                    },
                    {
                      recordStore: {
                        created: [{}]
                      }
                    }
                  ]
                });

              });

              it('Should not abort the processing when record fails (Related records)', function() {

                var mock_workerpool = {
                                    
                  cpus: 2,
                  pool: simple.stub().returnWith({
                    clear: simple.stub(),
                    proxy: simple.stub().resolveWith({
                      processRecord: simple.stub()
                        .rejectWith({
                          processing: utils.createError(new Error('foobar'), {
                            record: {},
                            step: 'preprocess'
                          })
                        })
                        .resolveWith({
                          processing: {
                            record: {},
                            matchedRecords: []
                          }
                        })
                        .resolveWith({
                          processing: {
                            record: {},
                            matchedRecords: []
                          }
                        })
                        .resolveWith({
                          processing: {
                            record: {},
                            matchedRecords: []
                          }
                        })
                        .resolveWith()
                    })
                  })

                };
                
                return createLoadRecordsFactory(mock_workerpool)({
                  resultFormatter: resultFormatterFactoryNoContext,
                  recordSet: simple.stub(function() {
                    return Object.assign(recordSetFactory.apply(undefined, arguments), {
                      get: simple.stub()
                        .resolveWith([{}, {}, {}])
                        .resolveWith([{}])
                        .resolveWith()
                    });
                  })
                })(undefined, {

                  abortOnError: false,
                  target: 'match'

                }).then(function(result) {

                  return expect(result).to.eql({
                    status: 'ok',
                    statistics: {
                      processed: 4,
                      succeeded: 1,
                      skipped: 0,
                      failed: 3,
                      recordStore: {
                        created: 0,
                        updated: 0,
                        deleted: 0
                      }
                    },
                    records: [
                      {
                        step: 'preprocess',
                        failed: true,
                        message: 'foobar'
                      },
                      {
                        failed: true,
                        matchedRecords: []
                      },
                      {
                        failed: true,
                        matchedRecords: []
                      },
                      {
                        matchedRecords: []
                      }
                    ]
                  });

                });

              });

              it('Should process the records with the specified amount of workers', function() {

                var spy_workerpool_pool = simple.spy().returnWith({

                  clear: simple.stub(),
                  proxy: simple.stub().resolveWith({
                    processRecord: simple.stub().resolveWith({
                      processing: {
                        record: {},
                        recordStore: {
                          created: [{}]
                        }
                      }
                    })
                  })

                }),
                mock_workerpool = {
                                    
                  cpus: 10,
                  pool: spy_workerpool_pool
                  
                };
                
                return createLoadRecordsFactory(mock_workerpool)({
                  recordSet: simple.stub(function() {
                    return Object.assign(recordSetFactory.apply(undefined, arguments), {
                      get: simple.stub()
                        .resolveWith([{}])
                        .resolveWith([{}])
                        .resolveWith([{}])
                        .resolveWith()
                        .resolveWith()
                    });
                  })
                })(undefined, {

                  parallel: 5

                }).then(function(results) {

                  expect(results).to.be.eql({
                    status: 'ok',
                    statistics: {
                      processed: 3,
                      succeeded: 3,
                      skipped: 0,
                      failed: 0,
                      recordStore: {
                        created: 3,
                        updated: 0,
                        deleted: 0
                      }
                    },
                    records: [
                      {
                        recordStore: {
                          created: [{}]
                        }
                      },
                      {
                        recordStore: {
                          created: [{}]
                        }
                      },
                      {
                        recordStore: {
                          created: [{}]
                        }
                      }
                    ]
                  });

                  expect(spy_workerpool_pool.callCount).to.equal(1);
                  expect(spy_workerpool_pool.calls[0].args).to.eql([
                    undefined,
                    {
                      maxWorkers: 5
                    }
                  ]);

                });

              });

              it('Should roll back changes', function() {

                var mock_record_store,
                spy_process_record = simple.spy().resolveWith({
                  processing: {
                    record: {},
                    recordStore: {
                      created: [{}]
                    }
                  }
                }).rejectWith({
                  processing: utils.createError(new Error('foobar'), {
                    record: {},
                    step: 'load',
                    recordStore: {
                      created: [{}]
                    }
                  })
                }),
                mock_workerpool = {
                  
                  cpus: 2,
                  pool: simple.stub().returnWith({
                    clear: simple.stub(),
                    proxy: simple.stub().resolveWith({
                      processRecord: spy_process_record
                    })
                  })

                };
                
                return createLoadRecordsFactory(mock_workerpool)({
                  resultFormatter: resultFormatterFactoryNoContext,
                  recordStore: simple.stub(function() {
                    
                    mock_record_store = recordStoreFactory.apply(undefined, arguments);
                    
                    simple.mock(mock_record_store, 'rollback').resolveWith({
                      updated: [{}]
                    });
                    
                    return mock_record_store;
                    
                  }),
                  recordSet: simple.stub(function() {
                    return Object.assign(recordSetFactory.apply(undefined, arguments), {
                      get: simple.stub()
                        .resolveWith([{}])
                        .resolveWith([{}])
                        .resolveWith([{}])
                        .resolveWith()
                    });
                  })
                })(undefined, {
                  
                  rollback: true
                  
                }).then(function(results) {
                  
                  expect(mock_record_store.rollback.callCount).to.equal(1);
                  expect(mock_record_store.rollback.calls[0].args).to.eql([{
                    created: [{}]
                  }]);

                  expect(results).to.be.eql({
                    status: 'aborted',
                    statistics: {
                      processed: 2,
                      succeeded: 1,
                      skipped: 0,
                      failed: 1,
                      recordStore: {
                        created: 1,
                        updated: 1,
                        deleted: 0
                      }
                    },
                    records: [
                      {
                        recordStore: {
                          created: [{}]
                        }
                      },
                      {
                        failed: true,
                        step: 'load',
                        message: 'foobar',
                        recordStore: {
                          updated: [{}]
                        }
                      }
                    ]
                  });
                  
                });
                
              });

              it('Should roll back changes of all related records', function() {

                var mock_record_store,
                spy_process_record = simple.spy(function(record, target) {

                  return target !== 'load' ? Promise.resolve({
                    processing: record
                  }) : spy_process_record.callCount === 8 ? Promise.reject({
                    processing: utils.createError(new Error('foobar'), Object.assign(record, {
                      step: 'load',
                      recordStore: {
                        created: [{}]
                      }
                    }))
                  }) : Promise.resolve({
                    processing: Object.assign(record, {
                      recordStore: {
                        created: [{}]
                      }
                    })
                  });
                  
                }),
                mock_workerpool = {
                  
                  cpus: 2,
                  pool: simple.stub().returnWith({
                    clear: simple.stub(),
                    proxy: simple.stub().resolveWith({
                      processRecord: spy_process_record
                    })
                  })

                };

                return createLoadRecordsFactory(mock_workerpool)({
                  resultFormatter: resultFormatterFactoryNoContext,
                  recordStore: simple.stub(function() {

                    mock_record_store = recordStoreFactory.apply(undefined, arguments);

                    simple.mock(mock_record_store, 'rollback');

                    return mock_record_store;
                    
                  }),
                  recordSet: simple.stub(function() {
                    return Object.assign(recordSetFactory.apply(undefined, arguments), {
                      get: simple.stub().resolveWith([{}, {}, {}]).resolveWith()
                    });
                  })
                })(undefined, {

                  rollback: true

                }).then(function(results) {

                  expect(spy_process_record.callCount).to.equal(8);
                  expect(mock_record_store.rollback.callCount).to.equal(2);
                  expect(mock_record_store.rollback.calls[0].args).to.eql([{
                    created: [{}]
                  }]);
                  expect(mock_record_store.rollback.calls[1].args).to.eql([{
                    created: [{}]
                  }]);

                  expect(results).to.be.eql({
                    status: 'aborted',
                    statistics: {
                      processed: 2,
                      succeeded: 0,
                      skipped: 0,
                      failed: 2,
                      recordStore: {
                        created: 0,
                        updated: 0,
                        deleted: 0
                      }
                    },
                    records: [
                      {
                        failed: true
                      },
                      {
                        step: 'load',
                        failed: true,
                        message: 'foobar'
                      }
                    ]
                  });

                });

              });

            });

            describe('hooks', function() {

              it('Should call the relatedRecordsRetrieved hook', function() {

                var mock_hook,
                mock_workerpool = {
                  
                  cpus: 2,
                  pool: simple.stub().returnWith({
                    clear: simple.stub(),
                    proxy: simple.stub().resolveWith({
                      processRecord: simple.stub()
                        .resolveWith({
                          processing: {
                            record: {},
                            recordStore: {
                              created: [{}]
                            }
                          }
                        })
                        .resolveWith({
                          processing: {
                            record: {},
                            recordStore: {
                              created: [{}]
                            }
                          }
                        })
                        .resolveWith({
                          processing: {
                            record: {},
                            recordStore: {
                              created: [{}]
                            }
                          }
                        })
                    })
                  })

                };

                return createLoadRecordsFactory(mock_workerpool)({
                  recordSet: simple.stub(function() {
                    return Object.assign(recordSetFactory.apply(undefined, arguments), {
                      get: simple.stub().resolveWith([{}, {}, {}]).resolveWith()
                    });
                  }),
                  hooks: {
                    relatedRecordsRetrieved: simple.stub(function() {
                      
                      mock_hook = relatedRecordsRetrievedHookFactory.apply(undefined, arguments);

                      simple.mock(mock_hook, 'setLogger');
                      simple.mock(mock_hook, 'setRecordStore');
                      simple.mock(mock_hook, 'run');
                                            
                      return mock_hook;

                    })
                  }
                })().then(function(results) {

                  expect(mock_hook.setLogger.callCount).to.equal(1);
                  expect(mock_hook.setRecordStore.callCount).to.equal(1);
                  expect(mock_hook.run.callCount).to.equal(1);
                  expect(mock_hook.run.calls[0].args).to.eql([[
                    {
                      record: {}
                    },
                    {
                      record: {}
                    },
                    {
                      record: {}
                    }
                  ]]);

                  expect(results).to.be.eql({
                    status: 'ok',
                    statistics: {
                      processed: 3,
                      succeeded: 3,
                      skipped: 0,
                      failed: 0,
                      recordStore: {
                        created: 3,
                        updated: 0,
                        deleted: 0
                      }
                    },
                    records: [
                      {
                        recordStore: {
                          created: [{}]
                        }
                      },
                      {
                        recordStore: {
                          created: [{}]
                        }
                      },
                      {
                        recordStore: {
                          created: [{}]
                        }
                      }
                    ]
                  });

                });

              });

              it('Should call the relatedRecordsMatched hook', function() {

                var mock_hook,
                mock_workerpool = {
                  
                  cpus: 2,
                  pool: simple.stub().returnWith({
                    clear: simple.stub(),
                    proxy: simple.stub().resolveWith({
                      processRecord: function(record, target)
                      {

                        record = JSON.parse(JSON.stringify(record));

                        switch (target) {
                        case 'merge':
                        case 'match':
                          return Promise.resolve({
                            processing: Object.assign(record, {
                              matchedRecords: [{}]
                            })
                          });
                        case 'load':
                          return Promise.resolve({
                            processing: Object.assign(record, {
                              recordStore: {
                                updated: [{}]
                              }
                            })
                          });
                        default:
                          return Promise.resolve({
                            processing: {
                              record: {}
                            }
                          });
                        }

                      }
                    })
                  })

                };

                return createLoadRecordsFactory(mock_workerpool)({
                  recordSet: simple.stub(function() {
                    return Object.assign(recordSetFactory.apply(undefined, arguments), {
                      get: simple.stub().resolveWith([{}, {}, {}]).resolveWith()
                    });
                  }),
                  hooks: {
                    relatedRecordsMatched: simple.stub(function() {

                      mock_hook = relatedRecordsMatchedHookFactory.apply(undefined, arguments);

                      simple.mock(mock_hook, 'setLogger');
                      simple.mock(mock_hook, 'setRecordStore');
                      simple.mock(mock_hook, 'run');

                      return mock_hook;

                    })
                  }
                })().then(function(results) {

                  expect(mock_hook.setLogger.callCount).to.equal(1);
                  expect(mock_hook.setRecordStore.callCount).to.equal(1);
                  expect(mock_hook.run.callCount).to.equal(1);
                  expect(mock_hook.run.calls[0].args).to.eql([[
                    {
                      record: {},
                      matchedRecords: [{}]
                    },
                    {
                      record: {},
                      matchedRecords: [{}]
                    },
                    {
                      record: {},
                      matchedRecords: [{}]
                    }
                  ]]);

                  expect(results).to.be.eql({
                    status: 'ok',
                    statistics: {
                      processed: 3,
                      succeeded: 3,
                      skipped: 0,
                      failed: 0,
                      recordStore: {
                        created: 0,
                        updated: 3,
                        deleted: 0
                      }
                    },
                    records: [
                      {
                        matchedRecords: [{}],
                        recordStore: {
                          updated: [{}]
                        }
                      },
                      {
                        matchedRecords: [{}],
                        recordStore: {
                          updated: [{}]
                        }
                      },
                      {
                        matchedRecords: [{}],
                        recordStore: {
                          updated: [{}]
                        }
                      }
                    ]
                  });

                });

              });

            });


          });

        });
        
      });

    });

  };
  
}
