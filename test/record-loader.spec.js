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
 * for the JavaScript code in this page.
 *
 **/

(function (root, factory) {

    'use strict';

    if (typeof define === 'function' && define.amd) {
	define(['chai', 'chai-as-promised', '../lib/main', '../lib/record-set/array', '../lib/record-store/in-memory'], factory);
    } else if (typeof module === 'object' && module.exports) {
        module.exports = factory(require('chai'), require('chai-as-promised'), require('../lib/main'), require('../lib/record-set/array'), require('../lib/record-store/in-memory'));
    }

}(this, factory));

function factory(chai, chaiAsPromised, recordLoader, recordSet, recordStore)
{

    'use strict';

    var should = chai.should();
    
    chai.use(chaiAsPromised);

    describe('record-loader', function() {

	it('Should reject because of invalid record set module', function() {
	    return recordLoader().should.be.rejectedWith(/Cannot read property 'initialise' of undefined/);
	});
	
	it('Should reject because of invalid modules', function() {
	    return recordLoader([], {
		recordSet: recordSet,
		processors: {
		    filter: {}
		}
	    }).should.be.rejectedWith(/object is not a function/);
	});
	
	it('Should reject because of invalid configuration', function() {
	    return recordLoader(
		undefined,
		{
		    recordSet: {},
		    processors: {}
		},
		{
		    processing: {}
		}
	    ).should.be.rejectedWith(/Configuration doesn't validate against schema/);
	});

	it('Should reject because target processor cannot be reached', function() {
	    return recordLoader(
		[1, 2 ,3],
		{
		    recordSet: recordSet,
		    processors: {}
		}
	    ).should.be.rejectedWith(/Target processing step cannot be reached because of a invalid\/undefined processor/);
	});

	it('Should only process with filter and preprocess', function() {
	    return recordLoader(
		[1, 2, 3],
		{
		    recordSet: recordSet,
		    processors: {
			filter: function() {
			    return {
				run: function(record){
				    return Promise.resolve(record);
				}
			    };
			},
			preprocess: function(record){
			    return {
				run: function(record){
				    return Promise.resolve(record);
				}
			    };
			}
		    }
		},
		{
		    processing: {
			target: 'preprocess',
			findRelatedRecords: false
		    }
		}
	    ).should.eventually.eql({
		processed: 3,
		skipped: 0,
		recordStore: {
		    created: 0,
		    updated: 0,
		    deleted: 0
		},
		transactions: []
	    });
	});

	it('Should filter out one record', function() {
	    return recordLoader(
		[1, 2, 3],
		{
		    recordSet: recordSet,
		    processors: {
			filter: function() {
			    return {
				run: function(record){
				    return Promise.resolve(record === 2 ? undefined : record);
				}
			    };
			},
			preprocess: function(){
			    return {
				run: function(record){
				    return Promise.resolve(record);
				}
			    };
			}
		    }
		},
		{
		    processing: {
			target: 'preprocess',
			findRelatedRecords: false
		    }
		}
	    ).should.eventually.eql({
		processed: 2,
		skipped: 1,
		recordStore: {
		    created: 0,
		    updated: 0,
		    deleted: 0
		},
		transactions: []
	    });
	});

	it("Shouldn't filter out any records because of parameters passed to filter", function() {
	    return recordLoader(
		[1, 2, 3],
		{
		    recordSet: recordSet,
		    processors: {
			filter: function(passthrough) {
			    return {
				run: function(record){
				    return Promise.resolve(record === 2 && !passthrough ? undefined : record);
				}
			    };
			},
			preprocess: function(){
			    return {
				run: function(record){
				    return Promise.resolve(record);
				}
			    };
			}
		    }
		},
		{
		    processing: {
			target: 'preprocess',
			findRelatedRecords: false
		    },
		    moduleParameters: {
			processors: {
			    filter: 1
			}
		    }
		}
	    ).should.eventually.eql({
		processed: 3,
		skipped: 0,
		recordStore: {
		    created: 0,
		    updated: 0,
		    deleted: 0
		},
		transactions: []
	    });
	});

 	it('Should create a new preprocessed record in record store', function(done) {

	    var record_store;

	    recordLoader(
		[1],
		{
		    recordSet: recordSet,
		    recordStore: recordStore,
		    processors: {
			filter: function() {
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			preprocess: function()
			{
			    return {
				run: function(record)
				{
				    return Promise.resolve(record+1);
				}
			    };
			},
			match: function()
			{
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			},
			merge: function(){
			    return {
				run: function(record, matched_records)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			load: function(){
			    return {
				run: function(record)
				{
				    return record_store.create(record).then(function() {
					return {
					    created: 1
					};
				    });
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			}
		    }
		}
	    ).then(function(results) {

		try {
		    results.should.eql({
			processed: 1,
			skipped: 0,
			recordStore: {
			    created: 1,
			    updated: 0,
			    deleted: 0
			},
			transactions: []
		    });
		} catch (excp) {
		    return done(excp);
		}
		
		record_store.read().then(function(found_records) {
		    try {
			found_records.should.eql([2]);
			done();
		    } catch (excp_inner) {
			return done(excp_inner);
		    }
		}, done);
		
	    }, done);

	});

 	it('Should update a record in record store', function(done) {

	    var record_store;

	    recordLoader(
		[1, 2, 3],
		{
		    recordSet: recordSet,
		    recordStore: recordStore,
		    processors: {
			filter: function() {
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			preprocess: function()
			{
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			match: function()
			{
			    return {
				run: function(record)
				{					
				    return record_store.read(function(record_in_store) {
					return record_in_store === record;
				    }).then(function(found_records) {
					return [record, found_records];
				    });
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			},
			merge: function(){
			    return {
				run: function(record, matched_records)
				{
				    if (matched_records && matched_records.length > 0) {

					var record_merged = record + matched_records[0];
					
					return Promise.resolve([record_merged, {
					    update: function(record) {
						return record === matched_records[0];
					    }
					}]);

				    } else {
					return Promise.resolve(record);
				    }
				}
			    };
			},
			load: function(){
			    return {
				run: function(record, load_options)
				{
				    if (load_options) {
					return record_store.update(load_options.update, record).then(function() {
					    return {
						updated: 1
					    };
					});
				    } else {
					return record_store.create(record).then(function() {
					    return {
						created: 1
					    };
					});
				    }
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			}
		    }
		},
		{
		    moduleParameters: {
			recordStore: 3
		    }
		}
	    ).then(function(results) {
		
		try {
		    results.should.eql({
			processed: 3,
			skipped: 0,
			recordStore: {
			    created: 2,
			    updated: 1,
			    deleted: 0
			},
			transactions: []
		    });
		} catch (excp) {
		    return done(excp);
		}
		
		record_store.read(function(record) {
		    return record === 6;
		}).then(function(found_records) {
		    try {
			found_records.should.eql([6]);
			done();
		    } catch (excp) {
			return done(excp);
		    }
		}, done);
		
	    }, done);

	});

 	it('Should update and delete records in record store', function(done) {
	    
	    var record_store;

	    recordLoader(
		[1, 2, 3],
		{
		    recordSet: recordSet,
		    recordStore: recordStore,
		    processors: {
			filter: function() {
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			preprocess: function()
			{
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			match: function()
			{
			    return {
				run: function(record)
				{					
				    return record_store.read(function(record_in_store) {
					return record_in_store === record || Math.sqrt(record_in_store) === record;
				    }).then(function(found_records) {
					return [record, found_records];
				    });
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			},
			merge: function(){
			    return {
				run: function(record, matched_records)
				{
				    if (matched_records && matched_records.length > 0) {

					var record_merged = matched_records.reduce(function(total, matched_record) {
					    return total + matched_record;
					}, record);
					
					return Promise.resolve([record_merged, {
					    update: function(record) {
						return record === matched_records[0];
					    },
					    delete: function(record) {
						return record === matched_records[1];
					    }
					}]);

				    } else {
					return Promise.resolve(record);
				    }
				}
			    };
			},
			load: function(){
			    return {
				run: function(record, load_options)
				{
				    if (load_options) {
					return record_store.update(load_options.update, record).then(function() {
					    return record_store.delete(load_options.delete);
					}).then(function() {
					    return {
						updated: 1,
						deleted: 1
					    };
					});
				    } else {
					return record_store.create(record).then(function() {
					    return {
						created: 1
					    };
					});
				    }
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			}
		    }
		},
		{
		    moduleParameters: {
			recordStore: [3, 9]
		    }
		}
	    ).then(function(results) {
		
		try {
		    results.should.eql({
			processed: 3,
			skipped: 0,
			recordStore: {
			    created: 2,
			    updated: 1,
			    deleted: 1
			},
			transactions: []
		    });
		} catch (excp) {
		    return done(excp);
		}

		record_store.read(function(record) {
		    return record === 15;
		}).then(function(found_records) {

		    try {
			found_records.should.eql([15]);
		    } catch (excp) {
			return done(excp);
		    }

		    record_store.read().then(function(found_records) {
			try {
			    found_records.should.have.length.of(3);
			    done();
			} catch (excp) {
			    return done(excp);
			}
		    }, done);
		    
		}, done);
		
	    }, done);

	});

	it('Should reject because record conversion fails');

	it('Should reject because transaction fails', function(done) {

	    var record_store;

	    recordLoader(
		[1, 2, 3, 4, 5, 6, 7, 8, 9],
		{
		    recordSet: function() {
			
			var proto = recordSet();
			var parent_next = proto.next;
			var parent_initialise = proto.initialise;
			var records;

			proto.initialise = function(input_data)
			{
			    return parent_initialise(input_data).then(function() {
				records = JSON.parse(JSON.stringify(input_data));
			    });
			};

			proto.next = function()
			{
			    return parent_next().then(function(record_data) {

				var related_records = records.map(function(record, index) {
				    return Math.round(Math.sqrt(record)) === record_data.data && record_data.data === 3  ? {
					data: JSON.parse(JSON.stringify(record)),
					index: index
				    }
				    : undefined;
				}).filter(function(value) {
				    return value !== undefined;
				});

				return related_records.length === 0 ? record_data : [record_data].concat(related_records);

			    });
			};
			
			return proto;

		    },
		    recordStore: function(parameters) {

			var proto = recordStore(parameters);
			var parent_create = proto.create;
			var count = 0;

			proto.create = function(record, options)
			{
			    if (count > 4) {
				return Promise.reject(new Error('Creating record failed'));
			    } else {
				return parent_create(record, options).then(function() {
				    count++;
				});
			    }
			};

			return proto;

		    },
		    processors: {
			filter: function() {
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			preprocess: function()
			{
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			match: function()
			{
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			},
			merge: function(){
			    return {
				run: function(record, matched_records)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			load: function(){
			    return {
				run: function(record, load_options)
				{
				    return record_store.create(record).then(function() {
					return {
					    created: 1
					};
				    });
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			}
		    }
		}
	    ).then(
		function(result) {
		    done(result);
		},
		function(reason) {
		    try {
			reason.should.match(/Transaction failed: Creating record failed/);
			record_store.read().then(function(records) {
			    try {
				records.should.have.length(5);
				records.should.eql([1,2,3,7,8]);
				done();
			    } catch (excp_inner) {
				return done(excp_inner);
			    }
			});
		    } catch (excp) {
			return done(excp);
		    }
		});

	});

	it('Should roll back failed transaction and reject', function(done) {

	    var record_store;

	    recordLoader(
		[1, 2, 3, 4, 5, 6, 7, 8, 9],
		{
		    recordSet: function() {
			
			var proto = recordSet();
			var parent_next = proto.next;
			var parent_initialise = proto.initialise;
			var records;

			proto.initialise = function(input_data)
			{
			    return parent_initialise(input_data).then(function() {
				records = JSON.parse(JSON.stringify(input_data));
			    });
			};

			proto.next = function()
			{
			    return parent_next().then(function(record_data) {

				var related_records = records.map(function(record, index) {
				    return Math.round(Math.sqrt(record)) === record_data.data && record_data.data === 3  ? {
					data: JSON.parse(JSON.stringify(record)),
					index: index
				    }
				    : undefined;
				}).filter(function(value) {
				    return value !== undefined;
				});

				return related_records.length === 0 ? record_data : [record_data].concat(related_records);

			    });
			};
			
			return proto;

		    },
		    recordStore: function(parameters) {

			var proto = recordStore(parameters);
			var parent_create = proto.create;
			var count = 0;

			proto.create = function(record, options)
			{
			    if (count > 4) {
				return Promise.reject(new Error('Creating record failed'));
			    } else {
				return parent_create(record, options).then(function() {
				    count++;
				});
			    }
			};

			return proto;

		    },
		    processors: {
			filter: function() {
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			preprocess: function()
			{
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			match: function()
			{
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			},
			merge: function(){
			    return {
				run: function(record, matched_records)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			load: function(){
			    return {
				run: function(record, load_options)
				{
				    return record_store.create(record).then(function() {
					return {
					    created: 1
					};
				    });
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			}
		    }
		},
		{
		    processing: {
			target: 'load',
			findRelatedRecords: true,
			transaction: {
			    enableRollback: true,
			    retryAfterRollback: false
			}
		    }
		}
	    ).then(
		function(result) {
		    done(result);
		},
		function(reason) {
		    try {
			reason.should.match(/Transaction failed: Creating record failed/);
			record_store.read().then(function(records) {
			    try {
				records.should.have.length(2);
				records.should.eql([1,2]);
				done();
			    } catch (excp_inner) {
				return done(excp_inner);
			    }
			});
		    } catch (excp) {
			return done(excp);
		    }
		});

	});

	it('Should retry failed transaction two times and reject', function(done) {

	    var record_store;

	    recordLoader(
		[1, 2, 3, 4, 5, 6, 7, 8, 9],
		{
		    recordSet: function() {
			
			var proto = recordSet();
			var parent_next = proto.next;
			var parent_initialise = proto.initialise;
			var records;

			proto.initialise = function(input_data)
			{
			    return parent_initialise(input_data).then(function() {
				records = JSON.parse(JSON.stringify(input_data));
			    });
			};

			proto.next = function()
			{
			    return parent_next().then(function(record_data) {

				var related_records = records.map(function(record, index) {
				    return Math.round(Math.sqrt(record)) === record_data.data && record_data.data === 3  ? {
					data: JSON.parse(JSON.stringify(record)),
					index: index
				    }
				    : undefined;
				}).filter(function(value) {
				    return value !== undefined;
				});

				return related_records.length === 0 ? record_data : [record_data].concat(related_records);

			    });
			};
			
			return proto;

		    },
		    recordStore: function(parameters) {

			var proto = recordStore(parameters);
			var parent_create = proto.create;
			var count = 0;

			proto.create = function(record, options)
			{
			    if (count > 4) {
				return Promise.reject(new Error('Creating record failed'));
			    } else {
				return parent_create(record, options).then(function() {
				    count++;
				});
			    }
			};

			return proto;

		    },
		    processors: {
			filter: function() {
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			preprocess: function()
			{
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			match: function()
			{
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			},
			merge: function(){
			    return {
				run: function(record, matched_records)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			load: function(){
			    return {
				run: function(record, load_options)
				{
				    return record_store.create(record).then(function() {
					return {
					    created: 1
					};
				    });
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			}
		    }
		},
		{
		    processing: {
			target: 'load',
			findRelatedRecords: true,
			transaction: {
			    enableRollback: true,
			    retryAfterRollback: 2
			}
		    }
		}
	    ).then(
		function(result) {
		    done(result);
		},
		function(reason) {
		    try {
			reason.should.match(/Transaction failed after 2 retries: Creating record failed/);
			record_store.read().then(function(records) {
			    try {
				records.should.have.length(2);
				records.should.eql([1,2]);
				done();
			    } catch (excp_inner) {
				return done(excp_inner);
			    }
			});
		    } catch (excp) {
			return done(excp);
		    }
		});

	});

	it('Should roll back transaction and succeed after third retry', function() {

	    var record_store;

	    return recordLoader(
		[1, 2, 3, 4, 5, 6, 7, 8, 9],
		{
		    recordSet: function() {
			
			var proto = recordSet();
			var parent_next = proto.next;
			var parent_initialise = proto.initialise;
			var records;

			proto.initialise = function(input_data)
			{
			    return parent_initialise(input_data).then(function() {
				records = JSON.parse(JSON.stringify(input_data));
			    });
			};

			proto.next = function()
			{
			    return parent_next().then(function(record_data) {

				var related_records = records.map(function(record, index) {
				    return Math.round(Math.sqrt(record)) === record_data.data && record_data.data === 3  ? {
					data: JSON.parse(JSON.stringify(record)),
					index: index
				    }
				    : undefined;
				}).filter(function(value) {
				    return value !== undefined;
				});

				return related_records.length === 0 ? record_data : [record_data].concat(related_records);

			    });
			};
			
			return proto;

		    },
		    recordStore: function(parameters) {

			var proto = recordStore(parameters);
			var parent_create = proto.create;
			var disable_creation_semaphore = 0;
			var count_created = 0;
			var count_called = {};

			proto.create = function(record, options)
			{

			    if (count_called.hasOwnProperty(record)) {
				count_called[record]++;
			    } else {
				count_called[record] = 1;
			    }

			    if (count_created > 4) {
				if (count_called[record] > 4) {
				    disable_creation_semaphore = 1;
				}
			    }

			    if (!disable_creation_semaphore && count_created > 4) {
				return Promise.reject(new Error('Creating record failed'));
			    } else {
				return parent_create(record, options).then(function() {
				    count_created++;
				});
			    }
			};

			return proto;

		    },
		    processors: {
			filter: function() {
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			preprocess: function()
			{
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			match: function()
			{
			    return {
				run: function(record)
				{
				    return Promise.resolve(record);
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			},
			merge: function(){
			    return {
				run: function(record, matched_records)
				{
				    return Promise.resolve(record);
				}
			    };
			},
			load: function(){
			    return {
				run: function(record, load_options)
				{
				    return record_store.create(record).then(function() {
					return {
					    created: 1
					};
				    });
				},
				setRecordStore: function(record_store_arg)
				{
				    record_store = record_store_arg;
				}
			    };
			}
		    }
		},
		{
		    processing: {
			target: 'load',
			findRelatedRecords: true,
			transaction: {
			    enableRollback: true,
			    retryAfterRollback: true
			}
		    }
		}
	    ).should.eventually.eql({
		processed: 9,
		skipped: 0,
		recordStore: {
		    created: 9,
		    updated: 0,
		    deleted: 0
		},
		transactions: [{
		    retries: 4
		}]
	    });

	});

    });

}