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
      'chai/chai',
      'es6-polyfills/lib/polyfills/object',
      'simple-mock',
      '../lib/utils'
    ], factory);
  } else if (typeof module === 'object' && module.exports) {
    module.exports = factory(
      require('chai'),
      require('es6-polyfills/lib/polyfills/object'),
      require('simple-mock'),
      require('../lib/utils')
    );
  }

}(this, factory));

function factory(chai, Object, simple, utils)
{

  'use strict';

  var expect = chai.expect;

  describe('utils', function() {

    it('Should be the expected object', function() {
      expect(utils).to.be.an('object').and.to
        .respondTo('mergeObjects').and.to
        .respondTo('callFactories');
    });

    describe('#PROCESSING_STEPS', function() {

      it('Should be equal to the expected value', function() {
        expect(utils.PROCESSING_STEPS).to.be.eql(['filter', 'preprocess', 'match', 'merge', 'load']);
      });

    });

    describe.skip('#clone');

    describe('#undefineProperty', function() {

      it('It should undefine a property in an object', function() {

        var obj = {
          foo: 'bar'
        };

        utils.undefineProperty(obj, 'foo');

        expect(Object.keys(obj).length).to.equal(0);

      });

    });

    describe('#undefineProperties', function() {

      it('It should undefine multiple properties in an object', function() {

        var obj = {
          foo: 0,
          bar: 1
        };

        utils.undefineProperties(obj, ['foo', 'bar']);

        expect(Object.keys(obj).length).to.equal(0);

      });

    });

    describe('#mergeObjects', function() {
      
      it('Should merge the objects', function() {

        var a = {
          a: 0,
          b: 1,
          c: 2
        },
        b = {
          a: 1,
          b: 1,
          c: 2,
          d: 3
        },
        a_original = JSON.parse(JSON.stringify(a)),
        b_original = JSON.parse(JSON.stringify(b));

        expect(utils.mergeObjects(a, b)).to.eql({
          a: 1,
          b: 1,
          c: 2,
          d: 3
        });
        expect(a).to.eql(a_original);
        expect(b).to.eql(b_original);

      });

      it('Should merge the objects recursively', function() {

        var a = {
          a: {
            foo: 1,
            bar: 2,
            fubar: {}
          },
          b: 1,
          c: 2
        },
        b = {
          a: {
            foo: 0,
            fubar: {}     
          },
          b: 1,
          c: 2,
          d: 3
        },
        a_original = JSON.parse(JSON.stringify(a)),
        b_original = JSON.parse(JSON.stringify(b));

        expect(utils.mergeObjects(a, b)).to.eql({
          a: {
            foo: 0,
            bar: 2,
            fubar: {}
          },
          b: 1,
          c: 2,
          d: 3
        });
        expect(a).to.eql(a_original);
        expect(b).to.eql(b_original);

      });

    });

    describe('#callFactories', function() {

      it('Should call the factory functions and assign return values', function() {

        var factories = {
          a: function() {},
          b: function()
          {
            return {};
          }
        },
        factories_original = Object.keys(factories).reduce(function(result, key) {
          
          result[key] = factories[key];
          return result;

        }, {});

        expect(utils.callFactories(factories)).to.eql({
          a: undefined,
          b: {}
        });

      });

      it('Should call the factory functions and assign return values recursively', function() {

        function clone(obj)
        {
          return Object.keys(obj).reduce(function(result, key) {
          
          if (typeof obj[key] === 'object') {
            result[key] = clone(obj[key]);
          } else {
            result[key] = obj[key];
          }
            
          return result;

          }, {});
        }

        var factories = {
          a: function() {},
          b: function()
          {
            return {};
          },
          c: {
            d: function() {}
          }
        },
        options = {},
        factories_original = clone(factories);

        expect(utils.callFactories(factories, options)).to.eql({
          a: undefined,
          b: {},
          c: {
            d: undefined
          }
        });

      });

    });

    describe('#createError', function() {

      it('Should create the expected object', function() {
        expect(utils.createError('foobar')).to.eql({
          message: 'foobar',
          failed: true
        });
      });

      it('Should create the expected object (Using Error)', function() {

        var error = Object.assign(new Error('foobar'), {
          stack: 'foo'
        });

        expect(utils.createError(error)).to.eql({
          stack: 'foo',
          message: 'foobar',
          failed: true
        });

      });

      it('Should create the expected object (With additional properties)', function() {
        expect(utils.createError('foobar', {
          foo: 'bar'
        })).to.eql({
          message: 'foobar',
          failed: true,
          foo: 'bar'
        });
      });

    });
    
  });

}
