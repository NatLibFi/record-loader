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

/* istanbul ignore next: umd wrapper */
(function(root, factory) {

  'use strict';

  if (typeof define === 'function' && define.amd) {
    define([
      '@natlibfi/es6-polyfills/lib/polyfills/promise',
      '@natlibfi/es6-polyfills/lib/polyfills/object',
      'chai-as-promised',
      'simple-mock',
      '@natlibfi/record-loader-prototypes/lib/record-set/prototype',
      '@natlibfi/record-loader-prototypes/lib/result-formatter/prototype'
    ], factory);
  } else if (typeof module === 'object' && module.exports) {
    module.exports = factory(
      require('@natlibfi/es6-polyfills/lib/polyfills/promise'),
      require('@natlibfi/es6-polyfills/lib/polyfills/object'),
      require('chai-as-promised'),
      require('simple-mock'),
      require('@natlibfi/record-loader-prototypes/lib/record-set/prototype'),
      require('@natlibfi/record-loader-prototypes/lib/result-formatter/prototype')
    );
  }

}(this, factory));

function factory(Promise, Object, chaiAsPromised, simple, recordSetFactory, resultFormatterFactory)
{

  'use strict';

  return function(chai, loadRecordsFactory, script_processor)
  {

    var expect = chai.expect;
    
    simple.Promise = Promise;
    
    chai.use(chaiAsPromised);

    describe('run', function() {

      var RESULT_LEVELS = resultFormatterFactory.getLevels();

      it('Runs the whole pipeline', function() {

        return loadRecordsFactory({
          recordSet: simple.stub(function() {

            var records;

            return Object.assign(recordSetFactory.apply(undefined, arguments), {
              initialize: function(records_arg)
              {
                records = records_arg;                
                return Promise.resolve();
              },
              get: function()
              {
                return Promise.resolve(records.length > 0 ? [records.shift()] : undefined);
              }
            });

          })
        }, script_processor)([
          {
            value: 1
          },
          {
            value: 2
          },
          {
            value: 3
          }            
        ], {
          parallel: 0,
          resultLevel: RESULT_LEVELS.statistics | RESULT_LEVELS.recordMetaData | RESULT_LEVELS.recordData
        }).then(function(result) {

          expect(result).to.eql({
            status: 'ok',
            statistics: {
              processed: 3,
              succeeded: 3,
              failed: 0,
              skipped: 0,
              recordStore: {
                created: 0,
                updated: 0,
                deleted: 0
              }
            },
            records: [
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 1
                }
              },
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 2
                }
              },
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 3
                }
              }
            ]
          });

        });

      });

      it('Runs the whole pipeline (Related records)', function() {

        return loadRecordsFactory({
          recordSet: simple.stub(function() {

            var records;

            return Object.assign(recordSetFactory.apply(undefined, arguments), {
              initialize: function(records_arg)
              {
                records = records_arg;                
                return Promise.resolve();
              },
              get: function()
              {
                return Promise.resolve(records.length > 0 ? [].concat(records.shift()) : undefined);
              }
            });

          })
        }, script_processor)([
          {
            value: 1
          },
          [
            {
              value: 'a'
            },
            {
              value: 'b'
            },
            {
              value: 'c'
            }

          ],
          {
            value: 3
          }            
        ], {
          parallel: 0,
          resultLevel: RESULT_LEVELS.statistics | RESULT_LEVELS.recordMetaData | RESULT_LEVELS.recordData
        }).then(function(result) {

          expect(result).to.eql({
            status: 'ok',
            statistics: {
              processed: 5,
              succeeded: 5,
              failed: 0,
              skipped: 0,
              recordStore: {
                created: 0,
                updated: 0,
                deleted: 0
              }
            },
            records: [
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 1
                }
              },
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 'a'
                }
              },
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 'b'
                }
              },
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 'c'
                }
              },
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 3
                }
              }
            ]
          });

        });

      });

      it('Runs the whole pipeline (Parallel processing)', function() {


        return loadRecordsFactory({
          recordSet: simple.stub(function() {

            var records;

            return Object.assign(recordSetFactory.apply(undefined, arguments), {
              initialize: function(records_arg)
              {
                records = records_arg;                
                return Promise.resolve();
              },
              get: function()
              {
                return Promise.resolve(records.length > 0 ? [records.shift()] : undefined);
              }
            });

          })
        }, script_processor)([
          {
            value: 1
          },
          {
            value: 2
          },
          {
            value: 3
          },
          {
            value: 4
          },
          {
            value: 5
          }               
        ], {
          parallel: 5,
          resultLevel: RESULT_LEVELS.statistics | RESULT_LEVELS.recordMetaData | RESULT_LEVELS.recordData
        }).then(function(result) {

          expect(result).to.eql({
            status: 'ok',
            statistics: {
              processed: 5,
              succeeded: 5,
              failed: 0,
              skipped: 0,
              recordStore: {
                created: 0,
                updated: 0,
                deleted: 0
              }
            },
            records: [
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 1
                }
              },
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 2
                }
              },
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 3
                }
              },
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 4
                }
              },
              {
                matchedRecords: [],
                mergedRecords: [],
                record: {
                  value: 5
                }
              }
            ]
          });

        });

      });

    });

  };

}
