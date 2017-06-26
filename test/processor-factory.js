/**
 *
 * @licstart  The following is the entire license notice for the JavaScript code in this file. 
 *
 * Load records into a data store while filtering, preprocessing, matching & merging them in the process
 *
 * Copyright (c) 2015-2017 University Of Helsinki (The National Library Of Finland)
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
      'chai-as-promised',
      'simple-mock',
      '../lib/create-processor-factory',
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
      require('chai-as-promised'),
      require('simple-mock'),
      require('../lib/create-processor-factory'),
      require('record-loader-prototypes/lib/record-store/prototype'),
      require('record-loader-prototypes/lib/processors/filter/prototype'),
      require('record-loader-prototypes/lib/processors/preprocess/prototype'),
      require('record-loader-prototypes/lib/processors/match/prototype'),
      require('record-loader-prototypes/lib/processors/merge/prototype'),
      require('record-loader-prototypes/lib/processors/load/prototype')
    );
  }

}(this, factory));

function factory(Promise, chaiAsPromised, simple, processorFactory, recordStoreFactory, filterProcessorFactory, preprocessProcessorFactory, matchProcessorFactory, mergeProcessorFactory, loadProcessorFactory)
{

  'use strict';

  return function(chai)
  {

    var expect = chai.expect;
    
    simple.Promise = Promise;
    
    chai.use(chaiAsPromised);
    
    describe('processor-factory', function() {

      describe('factory', function() {

        it('Should return a function', function() {
          expect(processorFactory()).to.be.a('function');
        });
        
        describe('function', function() {

          it('Should call workerpool.worker with the expected arguments', function() {
            
            var spy_worker = simple.spy();
            
            processorFactory({
              worker: spy_worker
            })();
            
            expect(spy_worker.callCount).to.equal(1);
            expect(spy_worker.calls[0].args.length).to.equal(1);
            expect(spy_worker.calls[0].args[0]).to.respondTo('processRecord');
            
          });

          describe('worker', function() {
            
            describe('#processRecord', function() {

              it('Should process the record from the beginning to the target step (With default, no-op modules)', function() {
                
                var spy_worker = simple.spy();

                processorFactory({
                  worker: spy_worker
                })();

                expect(spy_worker.callCount).to.equal(1);

                return expect(spy_worker.calls[0].args[0].processRecord({
                  record: undefined
                }, 'load')).to.eventually.eql({
                  processing: {
                    record: undefined,
                    matchedRecords: [],
                    mergedRecords: []
                  },
                  exchange: {}
                });

              });

              it('Should process multiple records with the same instance', function() {
                
                var spy_worker = simple.spy();

                processorFactory({
                  worker: spy_worker
                })();

                expect(spy_worker.callCount).to.equal(1);

                return spy_worker.calls[0].args[0].processRecord({}, 'load').then(function(result) {
                  
                  expect(result).to.eql({
                    processing: {
                      record: undefined,
                      matchedRecords: [],
                      mergedRecords: []
                    },
                    exchange: {}
                  });
                  
                  return expect(spy_worker.calls[0].args[0].processRecord({}, 'load')).to.eventually.eql({
                    processing: {
                      record: undefined,
                      matchedRecords: [],
                      mergedRecords: []
                    },
                    exchange: {}
                  });

                });

              });

              it('Should reject because initialization of modules fails', function() {
                
                var spy_worker = simple.spy();

                processorFactory({
                  worker: spy_worker
                })({
                  logger: 'foobar'
                });

                return spy_worker.calls[0].args[0].processRecord({}, 'load').catch(function(error) {

                  expect(error).to.contain.all.keys(['processing', 'exchange']);
                  expect(error.processing).to.contain.all.keys(['message', 'failed']);
                  expect(error.processing.failed).to.equal(true);

                });

              });


              it('Should process the record from the beginning to the target step (Using default record store)', function() {
                
                var mock_load,
                spy_worker = simple.spy();

                processorFactory({
                  worker: spy_worker
                })({
                  processors: {
                    load: simple.stub(function() {
                      
                      mock_load = loadProcessorFactory.apply(undefined, arguments);

                      simple.mock(mock_load, 'setRecordStoreMethods');
                      simple.mock(mock_load, 'run');
                      
                      return mock_load;

                    })
                  }
                });
                
                expect(spy_worker.callCount).to.equal(1);

                return spy_worker.calls[0].args[0].processRecord({}, 'load').then(function(result) {
                  
                  expect(result).to.eql({
                    processing: {
                      record: undefined,
                      matchedRecords: [],
                      mergedRecords: []
                    },
                    exchange: {}
                  });
                  
                  expect(mock_load.setRecordStoreMethods.callCount).to.equal(1);
                  expect(mock_load.run.callCount).to.equal(1);

                });

              });

              it('Should process the record from the beginning to the target step', function() {

                var mock_filter, mock_preprocess, mock_match, mock_merge, mock_load, mock_record_store,
                spy_worker = simple.spy();

                processorFactory({
                  worker: spy_worker
                })({
                  recordStore: simple.stub(function() {

                    mock_record_store = recordStoreFactory.apply(undefined, arguments);
                    
                    simple.mock(mock_record_store, 'create');
                    simple.mock(mock_record_store, 'read');
                    simple.mock(mock_record_store, 'update');
                    simple.mock(mock_record_store, 'delete');
                    
                    return mock_record_store;
                    
                  }),
                  processors: {
                    filter: simple.stub(function() {

                      mock_filter = filterProcessorFactory.apply(undefined, arguments);
                      simple.mock(mock_filter, 'run');                     
                      return mock_filter;
                      
                    }),
                    preprocess: simple.stub(function() {
                      
                      mock_preprocess = preprocessProcessorFactory.apply(undefined, arguments);                    
                      simple.mock(mock_preprocess, 'run');                      
                      return mock_preprocess;
                      
                    }),
                    match: simple.stub(function() {
                      
                      mock_match = matchProcessorFactory.apply(undefined, arguments);

                      simple.mock(mock_match, 'setReadRecordStore');
                      simple.mock(mock_match, 'run');
                      
                      return mock_match;
                      
                    }),
                    merge: simple.stub(function() {
                      
                      mock_merge = mergeProcessorFactory.apply(undefined, arguments);                      
                      simple.mock(mock_merge, 'run');                      
                      return mock_merge;
                      
                    }),
                    load: simple.stub(function() {
                      
                      mock_load = loadProcessorFactory.apply(undefined, arguments);
                      
                      simple.mock(mock_load, 'setRecordStoreMethods');
                      simple.mock(mock_load, 'run').resolveWith({
                        created: [{}]                        
                      });
                      
                      return mock_load;
                      
                    })
                  }
                });
                
                expect(spy_worker.callCount).to.equal(1);

                return spy_worker.calls[0].args[0].processRecord({}, 'load', {}).then(function(results) {
                  
                  expect(mock_filter.run.callCount).to.equal(1);
                  expect(mock_preprocess.run.callCount).to.equal(1);
                  expect(mock_merge.run.callCount).to.equal(1);
                  expect(mock_merge.run.calls[0].args).to.eql([undefined, []]);

                  expect(mock_match.setReadRecordStore.callCount).to.equal(1);
                  expect(mock_match.run.callCount).to.equal(1);

                  expect(mock_load.setRecordStoreMethods.callCount).to.equal(1);
                  expect(mock_load.setRecordStoreMethods.calls[0].args).to.eql([{
                    create: mock_record_store.create,
                    read: mock_record_store.read  ,
                    update: mock_record_store.update,
                    delete: mock_record_store.delete,
                  }]);

                  expect(mock_load.run.callCount).to.equal(1);
                  expect(mock_load.run.calls[0].args).to.eql([undefined, []]);

                  expect(results).to.eql({
                    processing: {
                      record: undefined,
                      matchedRecords: [],
                      mergedRecords: [],
                      recordStore: {
                        created: [{}]
                      }
                    },
                    exchange: {}
                  });

                });

              });

              it('Should process the record from the specified step to target step', function(done) {

                var mock_filter, mock_preprocess, mock_match, mock_merge, mock_load,
                spy_worker = simple.spy();

                processorFactory({
                  worker: spy_worker
                })({
                  processors: {
                    filter: simple.stub(function() {

                      mock_filter = filterProcessorFactory.apply(undefined, arguments);
                      simple.mock(mock_filter, 'run');                     
                      return mock_filter;
                      
                    }),
                    preprocess: simple.stub(function() {
                      
                      mock_preprocess = preprocessProcessorFactory.apply(undefined, arguments);                    
                      simple.mock(mock_preprocess, 'run');                      
                      return mock_preprocess;
                      
                    }),
                    match: simple.stub(function() {
                      
                      mock_match = matchProcessorFactory.apply(undefined, arguments);

                      simple.mock(mock_match, 'setReadRecordStore');
                      simple.mock(mock_match, 'run');
                      
                      return mock_match;
                      
                    }),
                    merge: simple.stub(function() {
                      
                      mock_merge = mergeProcessorFactory.apply(undefined, arguments);                      
                      simple.mock(mock_merge, 'run');                      
                      return mock_merge;
                      
                    }),
                    load: simple.stub(function() {
                      
                      mock_load = loadProcessorFactory.apply(undefined, arguments);
                      
                      simple.mock(mock_load, 'setRecordStoreMethods');
                      simple.mock(mock_load, 'run');
                      
                      return mock_load;
                      
                    })
                  }
                });
                
                expect(spy_worker.callCount).to.equal(1);
                spy_worker.calls[0].args[0].processRecord({
                  step: 'preprocess'
                }, 'merge').then(function(results) {

                  expect(mock_filter.run.callCount).to.equal(0);
                  expect(mock_preprocess.run.callCount).to.equal(0);
                  expect(mock_match.run.callCount).to.equal(1);
                  expect(mock_merge.run.callCount).to.equal(1);
                  expect(mock_merge.run.calls[0].args).to.eql([undefined, []]);
                  expect(mock_load.run.callCount).to.equal(0);
                  
                  expect(results).to.eql({
                    processing: {
                      record: undefined,
                      matchedRecords: [],
                      mergedRecords: []
                    },
                    exchange: {}
                  });

                  done();

                }).catch(done);

              });

              it("Should fail to reach the target step because the record doesn't pass the filter", function() {

                var mock_filter, mock_preprocess, mock_match, mock_merge, mock_load, mock_record_store,
                spy_worker = simple.spy();

                processorFactory({
                  worker: spy_worker
                })({
                  processors: {
                    filter: simple.stub(function() {

                      mock_filter = filterProcessorFactory.apply(undefined, arguments);
                      simple.mock(mock_filter, 'run').resolveWith({
                        passed: false
                      });

                      return mock_filter;
                      
                    }),
                    preprocess: simple.stub(function() {
                      
                      mock_preprocess = preprocessProcessorFactory.apply(undefined, arguments);                    
                      simple.mock(mock_preprocess, 'run');                      
                      return mock_preprocess;
                      
                    }),
                    match: simple.stub(function() {
                      
                      mock_match = matchProcessorFactory.apply(undefined, arguments);

                      simple.mock(mock_match, 'setReadRecordStore');
                      simple.mock(mock_match, 'run');
                      
                      return mock_match;
                      
                    }),
                    merge: simple.stub(function() {
                      
                      mock_merge = mergeProcessorFactory.apply(undefined, arguments);                      
                      simple.mock(mock_merge, 'run');                      
                      return mock_merge;
                      
                    }),
                    load: simple.stub(function() {
                      
                      mock_load = loadProcessorFactory.apply(undefined, arguments);
                      
                      simple.mock(mock_load, 'setRecordStoreMethods');
                      simple.mock(mock_load, 'run').resolveWith({
                        created: [{}]                        
                      });
                      
                      return mock_load;
                      
                    })
                  }
                });
                
                expect(spy_worker.callCount).to.equal(1);

                return spy_worker.calls[0].args[0].processRecord({}, 'load').then(function(results) {
                  
                  expect(mock_filter.run.callCount).to.equal(1);
                  expect(mock_preprocess.run.callCount).to.equal(0);
                  expect(mock_merge.run.callCount).to.equal(0);
                  expect(mock_match.run.callCount).to.equal(0);
                  expect(mock_load.run.callCount).to.equal(0);

                  expect(results).to.eql({
                    processing: {
                      passed: false,
                      skipped: true
                    },
                    exchange: {}
                  });

                });

              });

              it("Should reject because processing fails", function() {

                var spy_worker = simple.spy();

                processorFactory({
                  worker: spy_worker
                })({
                  processors: {
                    filter: simple.stub(function() {

                      var filter = filterProcessorFactory.apply(undefined, arguments);

                      simple.mock(filter, 'run').rejectWith(new Error('foobar'));

                      return filter;

                    })
                  }
                });
                
                expect(spy_worker.callCount).to.equal(1);

                return spy_worker.calls[0].args[0].processRecord({}, 'load').catch(function(error) {

                  expect(error).to.contain.all.keys(['message', 'failed']);
                  expect(error.failed).to.equal(true);

                });

              });

            });

          });

        });

      });

    });

  };

}
