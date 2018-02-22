/* A SparqlDatasource provides queryable access to a SPARQL endpoint. */

var Datasource = require('./Datasource'),
    N3 = require('n3'),
    LRU = require('lru-cache');

var ENDPOINT_ERROR = 'Error accessing SPARQL endpoint';
var INVALID_TURTLE_RESPONSE = 'The endpoint returned an invalid Turtle response.';

var web3_extended;
var options = {};
var web3;

// Creates a new SparqlBlockchainDatasource
function SparqlBlockchainDatasource(options) {
	if (!(this instanceof SparqlBlockchainDatasource))
		return new SparqlBlockchainDatasource(options);
	Datasource.call(this, options);
	
	this.theOptions = options;
	
	this._countCache = new LRU({ max: 1000, maxAge: 1000 * 60 * 60 * 3 });

	// Set endpoint URL and default graph
	//options = options || {};
	
	this._endpoint = this._endpointUrl = (this.theOptions.endpoint || '').replace(/[\?#][^]*$/, '');
	if (!this.theOptions.defaultGraph)
		this._endpointUrl += '?query=';
	else
		this._endpointUrl += '?default-graph-uri=' + encodeURIComponent(this.theOptions.defaultGraph) + '&query=';
		
		
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

	var rdfdataContract = web3.eth.contract(this.theOptions.contracts.rdfdatastore.abi);
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
Datasource.extend(SparqlBlockchainDatasource, ['triplePattern', 'limit', 'offset', 'totalCount']);

// Writes the results of the query to the given triple stream
SparqlBlockchainDatasource.prototype._executeQuery = function (query, settings, destination) {
	self = this;
	
	var theOptions = this.theOptions;
	
	var sourceObj = {};
	sourceObj.nodeHost = theOptions.node_address;
	sourceObj.nodePort = theOptions.node_port;
	sourceObj.blockchainType = theOptions.blockchain_type;
	sourceObj.dataAddress = theOptions.rdfdata_address;
	sourceObj.blockchainType = theOptions.blockchain_type;
	sourceObj.contractABI = theOptions.contracts.rdfdatastore.abi;	
	settings.sourceObj = sourceObj;;
	
	//console.log("CONSTANT = "+sourceObj.contractABI[0].constant);
	//console.log(query);
	//query = following json for open ended search
	/*
	{ 
		features: [ limit: true ], 
		datasource: 'ldf/dbpedia-100000-sparqlblockchain',
  		limit: 100,
  		patternString: '{ ?s ?p ?o }' 
  	}
  	*/

  	// Create the HTTP request
  	var sparqlPattern = this._createTriplePattern(query), self = this,
		constructQuery = this._createConstructQuery(sparqlPattern, query.offset, query.limit),
		request = { url: this._endpointUrl + encodeURIComponent(constructQuery),
			headers: { accept: 'text/turtle;q=1.0,application/n-triples;q=0.5,text/n3;q=0.3' },
		};
	var closeCondition = 0;
	var queryCount = 0;
  // Fetch and parse matching triples
  (new N3.Parser()).parse(this._request(request, emitError), function (error, triple) {
  
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	
	
    if (!error) {
		if (triple) {
			var quad = "<" + triple.subject + "> <" + triple.predicate + "> <"+triple.object + "> " + theOptions.graph_parameter + " .";
		  	//console.log("QUAD = "+quad);
		  	
			// check triple in contract
			// form triple string from triple.
			// graph = "<http://dbpedia.org>"
			//quad = triple.subject+" "triple.predicate+" "+triple.object+" "+triple.graph+" .";
			var handler = function (e, result) {
				if (!e) {
					//console.log(result);
        			if (result != "") {
        				destination._push(triple);
        			} else {
        				triple.graph = settings.fragment.pageUrl + "#unverified";
        				destination._push(triple);
        			}
					queryCount--;
        			if (closeCondition == 1 && queryCount == 0) {
        				destination.close();
        			}
				} else {
					queryCount--;
					//console.log(queryCount);
					console.error(e);
				}
				return;
			};
			queryCount++;
			//console.log(queryCount);
			//console.error("0x" + web3.sha3(quad));
			result = rdfdataInstance.getDataFromQuadHash.call("0x" + web3.sha3(quad), handler);


      	} else {
      		closeCondition = 1;
			if (closeCondition == 1 && queryCount == 0) {
				destination.close();
			}
    	}
    }

    // Virtuoso sometimes sends invalid Turtle, so try N-Triples.
    // We don't just accept N-Triples right away because it is slower,
    // and some Virtuoso versions don't support it and/or get conneg wrong.
    else {
		request.headers.accept = 'application/n-triples';
		return (new N3.Parser()).parse(self._request(request, emitError), function (error, triple) {
        if (error) {
			emitError(new Error(INVALID_TURTLE_RESPONSE));
        } else if (triple) {
			var quad = "<" + triple.subject + "> <" + triple.predicate + "> <"+triple.object + "> " + theOptions.graph_parameter + " .";
			var handler2 = function (e, result) {
				if (!e) {
        			if (result != "") {
        				destination._push(triple);
        			} else {
        				triple.graph = settings.fragment.pageUrl + "#unverified";
        				destination._push(triple);
        			}
					queryCount--;
        			if (closeCondition == 1 && queryCount == 0) {
        				destination.close();
        			}
				} else {
					queryCount--;
					console.error(e);
				}
				return;
			};
			queryCount++;
			//console.log(queryCount);
			result = rdfdataInstance.getDataFromQuadHash.call("0x" + web3.sha3(quad), handler2);
    	} else {
      		closeCondition = 1;
			if (closeCondition == 1 && queryCount == 0) {
				destination.close();
			}
    	}
      });
    }
  });

  // Determine the total number of matching triples
  this._getPatternCount(sparqlPattern, function (error, totalCount) {
    if (error)
      emitError(error);
    else if (typeof totalCount === 'number')
      destination.setProperty('metadata', { totalCount: totalCount, hasExactCount: true });
  });

  // Emits an error on the triple stream
  function emitError(error) {
    error && destination.emit('error', new Error(ENDPOINT_ERROR + ' ' + self._endpoint + ': ' + error.message));
  }
};

// Retrieves the (approximate) number of triples that match the SPARQL pattern
SparqlBlockchainDatasource.prototype._getPatternCount = function (sparqlPattern, callback) {
  // Try to find a cache match
  var cache = this._countCache, count = cache.get(sparqlPattern);
  if (count) return setImmediate(callback, null, count);

  // Execute the count query
  var countResponse = this._request({
    url: this._endpointUrl + encodeURIComponent(this._createCountQuery(sparqlPattern)),
    headers: { accept: 'text/csv' },
    timeout: 7500,
  }, callback);

  // Parse SPARQL response in CSV format (2 lines: variable name / count value)
  var csv = '';
  countResponse.on('data', function (data) { csv += data; });
  countResponse.on('end', function () {
    var countMatch = csv.match(/\d+/);
    if (!countMatch)
      callback(new Error('COUNT query failed.'));
    else {
      var count = parseInt(countMatch[0], 10);
      // Cache large values; small ones are calculated fast anyway
      if (count > 100000)
        cache.set(sparqlPattern, count);
      callback(null, count);
    }
  });
};

// Creates a CONSTRUCT query from the given SPARQL pattern
SparqlBlockchainDatasource.prototype._createConstructQuery =  function (sparqlPattern, offset, limit) {
  var query = ['CONSTRUCT', sparqlPattern, 'WHERE', sparqlPattern];
  // Even though the SPARQL spec indicates that
  // LIMIT and OFFSET might be meaningless without ORDER BY,
  // this doesn't seem a problem in practice.
  // Furthermore, sorting can be slow. Therefore, don't sort.
  limit  && query.push('LIMIT',  limit);
  offset && query.push('OFFSET', offset);
  return query.join(' ');
};

// Creates a SELECT COUNT(*) query from the given SPARQL pattern
SparqlBlockchainDatasource.prototype._createCountQuery = function (sparqlPattern) {
  return 'SELECT (COUNT(*) AS ?c) WHERE ' + sparqlPattern;
};

// Creates a SPARQL pattern for the given triple pattern
SparqlBlockchainDatasource.prototype._createTriplePattern = function (triple) {
  var query = ['{'], literalMatch;

  // Add a possible subject IRI
  triple.subject ? query.push('<', triple.subject, '> ') : query.push('?s ');

  // Add a possible predicate IRI
  triple.predicate ? query.push('<', triple.predicate, '> ') : query.push('?p ');

  // Add a possible object IRI or literal
  if (N3.Util.isIRI(triple.object))
    query.push('<', triple.object, '>');
  else if (!(literalMatch = /^"([^]*)"(?:(@[^"]+)|\^\^([^"]+))?$/.exec(triple.object)))
    query.push('?o');
  else {
    if (!/["\\]/.test(literalMatch[1]))
      query.push('"', literalMatch[1], '"');
    else
      query.push('"""', literalMatch[1].replace(/(["\\])/g, '\\$1'), '"""');
    literalMatch[2] ? query.push(literalMatch[2])
                    : literalMatch[3] && query.push('^^<', literalMatch[3], '>');
  }

  return query.push('}'), query.join('');
};

			
			

module.exports = SparqlBlockchainDatasource;
