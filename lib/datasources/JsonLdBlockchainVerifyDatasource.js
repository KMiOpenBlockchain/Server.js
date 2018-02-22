/*! @license MIT ©2014-2016 Ruben Verborgh, Ghent University - imec */
/* An JsonLdDatasource fetches data from a JSON-LD document. */

var MemoryDatasource = require('./MemoryDatasource'),
    jsonld = require('jsonld');

var ACCEPT = 'application/ld+json;q=1.0,application/json;q=0.7';

var web3_extended;
var options = {};
var web3;
var EMPTY_ADDRESS = "0x0000000000000000000000000000000000000000"

// Creates a new JsonLdDatasource
function JsonLdDatasource(options) {
	if (!(this instanceof JsonLdDatasource))
		return new JsonLdDatasource(options);
		MemoryDatasource.call(this, options);
		this._url = options && (options.url || options.file);

		this.theOptions = options;

		N3Parser = require(this.theOptions.n3parser_path);
		web3_extended = require(this.theOptions.web3ipc_path);
		// Create web3 extended instance
		options = {
			host: this.theOptions.parityipc_path,
			ipc:true,
			personal: true,
			admin: true,
			debug: false
		};
		web3 = web3_extended.create(options);

		var rdfdataContract = web3.eth.contract(this.theOptions.contracts.ipfsstore.abi);
		var rdfdataInstance = rdfdataContract.at(this.theOptions.rdfdata_address);
		//var owner = rdfdataInstance.owner.call();

		var handler = function (e, result) {
			if (!e) {
				//console.log(result);
				this.owner = result;
			} else {
				console.error(e);
				return;
			}
		};
		rdfdataInstance.owner.call(handler);
}
MemoryDatasource.extend(JsonLdDatasource);

// Retrieves all triples from the document
JsonLdDatasource.prototype._getAllTriples = function (addTriple, done) {
	self = this;
	var theOptions = this.theOptions;


	// Read the JSON-LD document
	var json = '';
	var url = theOptions.baseurl + theOptions.hash;
	var completed = 0;

	var handler = function (e, result) {
		if (!e) {
			//console.log(result);
			if (result !=  EMPTY_ADDRESS) {
				completed = 1;
			} else {
				completed = 2;
			}
		} else {
			//console.error(e);
			completed = 2;
		}
		return;
	};

	var rdfdataContract = web3.eth.contract(theOptions.contracts.ipfsstore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	result = rdfdataInstance.getDatas.call("0x" + web3.sha3(theOptions.hash), handler);

	setTimeout(waitingForResult, 250, self, addTriple, done);

	function waitingForResult (self, addTriple, done) {

		//console.log(completed);
		if (completed == 0) {
			setTimeout(waitingForResult, 500, self, addTriple, done);
		} else if (completed == 1) {
			document = self._fetch({ url: url, headers: { accept: ACCEPT } });
			document.on('data', function (data) { json += data; });
			document.on('end', function () {
				// Parse the JSON document
				try { json = JSON.parse(json); }
				catch (error) { return done(error); }
				// Convert the JSON-LD to triples
				extractTriples(json, addTriple, done);
			});
		} else {
			done(new Error('Data set is not valid'));
			//extractTriples({}, addTriple, done);
			//throw new Error('Data set is not valid');
		}
	}

};

// Extracts triples from a JSON-LD document
function extractTriples(json, addTriple, done) {
	jsonld.toRDF(json, function (error, triples) {
		for (var graphName in triples) {
		  triples[graphName].forEach(function (triple) {
			addTriple(triple.subject.value,
					  triple.predicate.value,
					  convertEntity(triple.object));
		  });
		}
		done(error);
	});
}

// Converts a jsonld.js entity to the N3.js in-memory representation
function convertEntity(entity) {
  // Return IRIs and blank nodes as-is
  if (entity.type !== 'literal')
    return entity.value;
  else {
    // Add a language tag to the literal if present
    if ('language' in entity)
      return '"' + entity.value + '"@' + entity.language;
    // Add a datatype to the literal if present
    if (entity.datatype !== 'http://www.w3.org/2001/XMLSchema#string')
      return '"' + entity.value + '"^^' + entity.datatype;
    // Otherwise, return the regular literal
    return '"' + entity.value + '"';
  }
}

module.exports = JsonLdDatasource;
