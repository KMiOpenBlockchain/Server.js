/*********************************************************************************
* The MIT License (MIT)                                                          *
*                                                                                *
* Copyright (c) 2017 KMi, The Open University UK                                 *
*                                                                                *
* Permission is hereby granted, free of charge, to any person obtaining          *
* a copy of this software and associated documentation files (the "Software"),   *
* to deal in the Software without restriction, including without limitation      *
* the rights to use, copy, modify, merge, publish, distribute, sublicense,       *
* and/or sell copies of the Software, and to permit persons to whom the Software *
* is furnished to do so, subject to the following conditions:                    *
*                                                                                *
* The above copyright notice and this permission notice shall be included in     *
* all copies or substantial portions of the Software.                            *
*                                                                                *
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR     *
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,       *
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL        *
* THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER     *
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,  *
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN      *
* THE SOFTWARE.                                                                  *
*                                                                                *
**********************************************************************************/

var Datasource = require('./Datasource');

var N3Parser;

const fs = require('fs');

// Should this be web3 client side code?
var web3_extended;
var options = {};
var web3;

// Creates a new EthereumBlockchainDatasource
function EthereumBlockchainDatasource(options) {

	if (!(this instanceof EthereumBlockchainDatasource))
	return new EthereumBlockchainDatasource(options);
	Datasource.call(this, options);
	
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

Datasource.extend(EthereumBlockchainDatasource, ['triplePattern', 'limit', 'offset', 'totalCount']);

// Loads the Ethereum Blockchain datasource
/*EthereumBlockchainDatasource.prototype._initialize = function (done) {
	throw new Error('_initialize is not implemented');
};
*/

// Retrieves all triples in the datasource
/*EthereumBlockchainDatasource.prototype._getAllTriples = function (addTriple, done) {
	throw new Error('_getAllTriples is not implemented');
};
*/

// Writes the results of the query to the given triple stream
EthereumBlockchainDatasource.prototype._executeQuery = function (query, settings, destination) {
	self = this
	console.log(query);
	
	var quadResultArray = new Array;
	
	//{ features: [ limit: true ], datasource: 'dbpedia', limit: 100, patternString: '{ ?s ?p ?o }' }
	var defaultSubject = "?s";
	var subjectbit = (typeof query.subject === "undefined" ? defaultSubject : "<" + query.subject + ">");
	var defaultObject = "?o";
	var objectbit = (typeof query.object === "undefined" ? defaultObject : "<" + query.object + ">");
	var defaultPredicate = "?p";
	var predicatebit = (typeof query.predicate === "undefined" ? defaultPredicate : "<" + query.predicate + ">");
	var defaultGraph = "?g";
	var graphbit = (typeof query.graph === "undefined" ? defaultGraph : "<" + query.graph + ">");
	
	var defaultLimit = 50;
	var limit = (typeof query.limit === "undefined" ? defaultLimit : query.limit);
	
	var defaultOffset = 0;
	var offset = (typeof query.offset === "undefined" ? defaultOffset : query.offset);	
	
	var params = {subjectbit: subjectbit, objectbit: objectbit, predicatebit: predicatebit, graphbit: graphbit, limit: limit, offset: offset};
	
	subjectResults(params, quadResultArray, query, destination, this.theOptions);
	
  	//console.log("subjectbit = " + subjectbit + " | objectbit = " + objectbit + " | predicatebit = " + predicatebit + " | graphbit = " + graphbit);

};

function subjectResultsArrays(params, quadResultArray, query, destination, theOptions, count, limit) {
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	if (count > limit) {
		objectResults(params, quadResultArray, query, destination, theOptions);
	} else {
		var handler = function (e, result) {
			if (!e) {
				//console.log("RESULT SUBJECT = " + result);
				quadResultArray = quadResultArray.concat(result);
				count = count +1;
				subjectResultsArrays(params, quadResultArray, query, destination, theOptions, count, limit);
			} else {
				console.error(e);
				return;
			}
		};
		result = rdfdataInstance.getSubjectArray.call("0x" + web3.sha3(params.subjectbit), count, {gas:4000000000000}, handler);
	}
}

function subjectResults(params, quadResultArray, query, destination, theOptions) {
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	
	//console.log("QUERY **** " + query);
	//console.log("Subjectbit **** " + params.subjectbit);
	if (params.subjectbit.substring(0, 1) != "?") {
		var handler = function (e, result) {
			if (!e) {
				//console.log("RESULT SUBJECT = " + result);
				limit = 1 * result;
				subjectResultsArrays(params, quadResultArray, query, destination, theOptions, 0, limit);
			} else {
				console.error(e);
				return;
			}
		};
		result = rdfdataInstance.getSubjectChunkCount.call("0x" + web3.sha3(params.subjectbit), handler);
	} else {
		objectResults(params, quadResultArray, query, destination, theOptions);
	}
}

function objectResultsArrays(params, quadResultArray, query, destination, theOptions, count, limit, quadResultArrayTemp) {
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	if (count > limit) {
		if (params.subjectbit.substring(0, 1) == "?" && params.objectbit.substring(0, 1) != "?") {
			quadResultArray = quadResultArrayTemp;
		} else {
			quadResultArray = checkForArrayMatches(quadResultArray, quadResultArrayTemp);
		}
		predicateResults(params, quadResultArray, query, destination, theOptions);
	} else {
		var handler = function (e, result) {
			if (!e) {
				//console.log("RESULT OBJECT = " + result);
				quadResultArrayTemp = quadResultArrayTemp.concat(result);
				count = count +1;
				objectResultsArrays(params, quadResultArray, query, destination, theOptions, count, limit, quadResultArrayTemp);
			} else {
				console.error(e);
				return;
			}
		};
		result = rdfdataInstance.getObjectArray.call("0x" + web3.sha3(params.objectbit), count, {gas:4000000000000}, handler);
	}
}

function objectResults(params, quadResultArray, query, destination, theOptions) {
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	
	//console.log("QUERY LIMIT **** " + query.limit);
	//console.log("Objectbit **** " + params.objectbit);

	if (params.objectbit.substring(0, 1) != "?") {
		var handler = function (e, result) {
			if (!e) {
				//console.log("RESULT OBJECT 2a = " + result);
				limit = 1 * result;
				var quadResultArrayTemp = new Array();
				objectResultsArrays(params, quadResultArray, query, destination, theOptions, 0, limit, quadResultArrayTemp);
			} else {
				console.error(e);
				return;
			}
		};
		result = rdfdataInstance.getObjectChunkCount.call("0x" + web3.sha3(params.objectbit), handler);
	} else {
		predicateResults(params, quadResultArray, query, destination, theOptions);
	}
}



function predicateResultsArrays(params, quadResultArray, query, destination, theOptions, count, limit, quadResultArrayTemp) {
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	if (count > limit) {
		if (params.subjectbit.substring(0, 1) == "?" && params.objectbit.substring(0, 1) == "?" && params.predicatebit.substring(0, 1) != "?") {
			quadResultArray = quadResultArrayTemp;
		} else {
			quadResultArray = checkForArrayMatches(quadResultArray, quadResultArrayTemp);
		}
		graphResults(params, quadResultArray, query, destination, theOptions);
	} else {
		var handler = function (e, result) {
			if (!e) {
				//console.log("RESULT PREDICATE = " + result.length);
				quadResultArrayTemp = quadResultArrayTemp.concat(result);
				count = count +1;
				predicateResultsArrays(params, quadResultArray, query, destination, theOptions, count, limit, quadResultArrayTemp);
			} else {
				console.error(e);
				return;
			}
		};
		result = rdfdataInstance.getPredicateArray.call("0x" + web3.sha3(params.predicatebit), count, {gas:4000000000000}, handler);
	}
}

function predicateResults(params, quadResultArray, query, destination, theOptions) {
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	
	console.log("QUERY LIMIT **** " + query.limit);
	console.log("Predicatebit **** " + params.predicatebit);

	if (params.predicatebit.substring(0, 1) != "?") {
		var handler = function (e, result) {
			if (!e) {
				//console.log("RESULT OBJECT 2a = " + result);
				limit = 1 * result;
				var quadResultArrayTemp = new Array();
				predicateResultsArrays(params, quadResultArray, query, destination, theOptions, 0, limit, quadResultArrayTemp);
			} else {
				console.error(e);
				return;
			}
		};
		result = rdfdataInstance.getPredicateChunkCount.call("0x" + web3.sha3(params.predicatebit), handler);
	} else {
		graphResults(params, quadResultArray, query, destination, theOptions);
	}
}

function graphResults(params, quadResultArray, query, destination, theOptions) {
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	
	//console.log("QUERY LIMIT **** " + query.limit);
	//console.log("Graphbit **** " + params.graphbit);
	
	var handler = function (e, result) {
		if (!e) {
			var datasetTotal = result.toNumber();
			if (params.subjectbit.substring(0, 1) == "?" && params.objectbit.substring(0, 1) == "?" && params.predicatebit.substring(0, 1) == "?" && params.graphbit.substring(0, 1) == "?") {			
				//console.log("RESULTS 5a = " + totalcount);	
				var lastRecord = params.offset + params.limit;
				if (lastRecord > datasetTotal) lastRecord = datasetTotal;
				//console.log("RESULTS 5b = " + lastRecord + " " + params.offset + " " + params.limit + " " + quadResultArray.length);	
				getQuad(params.offset, lastRecord, params, quadResultArray, query, destination, datasetTotal, theOptions);
			} else {
				totalcount = quadResultArray.length;
				//console.log("RESULTS 5c = " + totalcount);
				quadResultArray = trimArray(quadResultArray, params);
				//console.log("RESULTS 5d = " + quadResultArray.length);
				var resultArray = new Array();
				retrieveData(0, resultArray, params, quadResultArray, query, destination, totalcount, theOptions);
			}			
		} else {
			console.error(e);
			return;
		}
	};
	result = rdfdataInstance.getDataLength.call(handler);
}

function getQuad(next, finish, params, quadResultArray, query, destination, datasetTotal, theOptions) {
	//console.log("RESULTS 6a = " + next + " " + finish + " " + quadResultArray.length);
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	if (next < finish) {
		var handler = function (e, result) {
			if (!e) {
				quadResultArray[quadResultArray.length] = result;
				next = next + 1;
				getQuad(next, finish, params, quadResultArray, query, destination, datasetTotal, theOptions);
			} else {
				console.error(e);
				return;
			}
		};
		result = rdfdataInstance.dataindex.call(next, handler);
	
	
	} else {
		//console.log("RESULT 6b " + quadResultArray.length);
		var resultArray = new Array();
		retrieveData(0, resultArray, params, quadResultArray, query, destination, datasetTotal, theOptions)
	}

}

function retrieveData(next, resultArray, params, quadResultArray, query, destination, datasetTotal, theOptions){
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	
	//console.log("RESULT 7a " + resultArray.length);
	
	if (quadResultArray.length > 0) {
		if (next < quadResultArray.length) {
			var handler = function (e, result) {
				if (!e) {
					resultArray[resultArray.length] = result;
					//fs.appendFile('/data/web/blockchain2.kmi.open.ac.uk/node/ldf-server/output.txt', result+"\n", function (err) {
						//if (err) throw err;
					//});
					next = next + 1;
					retrieveData(next, resultArray, params, quadResultArray, query, destination, datasetTotal, theOptions);
				} else {
					console.error(e);
					return;
				}
			};
			result = rdfdataInstance.getDataFromQuadHash.call(quadResultArray[next], handler);
		} else {
			finishOff(resultArray, query, destination, datasetTotal);
		}
	} else {
		finishOff(resultArray, query, destination, datasetTotal, theOptions);
	}
}

function finishOff(result, query, destination, datasetTotal, theOptions){
	//console.log("The End");
	
	var tripleCount = 0;
	var hasExactCount = true;
	var parser = new N3Parser();

	var count = result.length;
	var next = "";
	var waserror = false;
	for (var i=0; i<count; i++) {
		next = result[i];
		try {
			triple = parser.parse(next);
			delete triple[0].graph;
			//console.log(triple);
			tripleCount++;
			destination._push(triple[0]);
		}
		catch(error) {
			destination.emit('error', new Error('Invalid query result: ' + error.message));
			waserror =true;
			break;
		}		
	}
	if (!waserror) {
		destination.setProperty('metadata', { totalCount: datasetTotal, hasExactCount: hasExactCount });
		destination.close();
	}
}


function checkForArrayMatches(originalArray, compareArray){
	var EMPTY_ADDRESS = "0x0000000000000000000000000000000000000000";
	var EMPTY_BYTES = "0x0000000000000000000000000000000000000000000000000000000000000000";
	var counter = 0;
	var temp = new Array();
	for (i = 0; i < originalArray.length; ++i) {
		if (originalArray[i] == EMPTY_BYTES) {
			break;
		} else {	
			if (compareArray.indexOf(originalArray[i]) != -1) {
				temp[counter] = originalArray[i];
				counter = counter + 1;
			}
		}
	}
	originalArray = temp;
	return originalArray;
}


function trimArray(resultArray, params) {
	var temp = new Array();
	var counter = 0;
	if (resultArray.length > params.offset) {
		var lastRecord = params.offset + params.limit;
		if (lastRecord > resultArray.length) lastRecord = resultArray.length;
		for (i = params.offset; i < lastRecord; ++i) {
				temp[counter] = resultArray[i];
				counter = counter + 1;
		}
		resultArray = temp;
		//console.log("Results Trimmed = " + resultArray.length);		
	}
	return resultArray;
}

// Closes the data source
/*
EthereumBlockchainDatasource.prototype.close = function (done) {
	throw new Error('close is not implemented');
};
*/


module.exports = EthereumBlockchainDatasource;
