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

// Creates a new EthereumBlockchainDatasource2
function EthereumBlockchainDatasource2(options) {

	if (!(this instanceof EthereumBlockchainDatasource2))
	return new EthereumBlockchainDatasource2(options);
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

Datasource.extend(EthereumBlockchainDatasource2, ['triplePattern', 'limit', 'offset', 'totalCount']);

// Loads the Ethereum Blockchain datasource
/*EthereumBlockchainDatasource2.prototype._initialize = function (done) {
	throw new Error('_initialize is not implemented');
};
*/

// Retrieves all triples in the datasource
/*EthereumBlockchainDatasource2.prototype._getAllTriples = function (addTriple, done) {
	throw new Error('_getAllTriples is not implemented');
};
*/

var totaltime = 0;
//var rdfdataContract = undefined;
//var rdfdataInstance = undefined;

// Writes the results of the query to the given triple stream
EthereumBlockchainDatasource2.prototype._executeQuery = function (query, settings, destination) {
	self = this
	console.log(query);

	var theOptions = this.theOptions;
	
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);

	totaltime = process.hrtime();

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

	subjectResultsCount(params, quadResultArray, query, destination, this.theOptions);

  	//console.log("subjectbit = " + subjectbit + " | objectbit = " + objectbit + " | predicatebit = " + predicatebit + " | graphbit = " + graphbit);

};

/***** SUBJECT FUNCTIONS *****/

var timesubjectstep = 0;

function subjectResultsCount(params, quadResultArray, query, destination, theOptions) {
	console.log("QUERY **** " + query);
	console.log("Subjectbit **** " + params.subjectbit);
	
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);

	if (params.subjectbit.substring(0, 1) != "?") {
		var time = process.hrtime();
		var handler = function (e, result) {
			if (!e) {
				var diff = process.hrtime(time);
				var subjectcount = result.toNumber();
				console.log("subject data count: "+(diff[0]+(diff[1] / 1e9))+" for record count: "+subjectcount);

				if (subjectcount > 60000) {
					timesubjectstep = process.hrtime();

					var start = 0;
					var length = subjectcount;

					// is this is the only term being searched one, activate paging
					if (params.subjectbit.substring(0, 1) == "?" && params.objectbit.substring(0, 1) == "?"
							 && params.graphbit.substring(0, 1) == "?" && params.predicatebit.substring(0, 1) != "?") {

						start = params.offset;
						length = params.limit;
						params.totalcount = subjectcount;
					}
					var quadResultArrayTemp = new Array();
					subjectResultsStep(start, length, params, quadResultArray, quadResultArrayTemp, query, destination, theOptions);
				} else {
					subjectResults(params, quadResultArray, query, destination, theOptions);
				}
			} else {
				console.error(e);
				return;
			}
		};
		rdfdataInstance.subjectindexcount.call("0x" + web3.sha3(params.subjectbit), handler);
	} else {
		objectResultsCount(params, quadResultArray, query, destination, theOptions);
	}
}

function subjectResults(params, quadResultArray, query, destination, theOptions) {
	console.log("QUERY **** " + query);
	console.log("Subjectbit **** " + params.subjectbit);
	
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);

	if (params.subjectbit.substring(0, 1) != "?") {
		var time = process.hrtime();
		var handler = function (e, result) {
			if (!e) {
				var diff = process.hrtime(time);
				console.log("subject data retrieval: "+(diff[0]+(diff[1] / 1e9))+" for record count: "+result.length);
				//console.log("RESULT SUBJECT = " + result);
				quadResultArray = result;
				objectResultsCount(params, quadResultArray, query, destination, theOptions);
			} else {
				console.error(e);
				return;
			}
		};
		rdfdataInstance.getSubjectArray.call("0x" + web3.sha3(params.subjectbit), handler);
	} else {
		objectResultsCount(params, quadResultArray, query, destination, theOptions);
	}
}

function subjectResultsStep(index, subjectcount, params, quadResultArray, quadResultArrayTemp, query, destination, theOptions) {

	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	
	if (index < subjectcount) {
		var handler = function (e, result) {
			if (!e) {
				quadResultArrayTemp[quadResultArrayTemp.length] = result;
				index = index + 1;
				subjectResultsStep(index, subjectcount, params, quadResultArray, quadResultArrayTemp, query, destination, theOptions);
			} else {
				console.error(e);
				return;
			}
		};
		rdfdataInstance.getSubjectArrayEntry.call("0x" + web3.sha3(params.subjectbit), index, handler);
	} else {
		var diffsubjectstep = process.hrtime(timesubjectstep);
		console.log("get subject stepping: "+(diffsubjectstep[0]+(diffsubjectstep[1]))+" for results length: "+subjectcount);

		if (quadResultArray.length > 0) {
			var timecheck = process.hrtime();
			quadResultArray = checkForArrayMatches(quadResultArray, quadResultArrayTemp);
			var diffcheck = process.hrtime(timecheck);
			console.log("subject intersection: "+(diffcheck[0]+(diffcheck[1] / 1e9))+" final count: "+quadResultArray.length);
		} else {
			quadResultArray = quadResultArrayTemp;
		}

		objectResultsCount(params, quadResultArray, query, destination, theOptions);
	}
}

/***** OBJECT FUNCTIONS *****/

var timeobjectstep = 0;

function objectResultsCount(params, quadResultArray, query, destination, theOptions) {
	//console.log("QUERY LIMIT **** " + query.limit);
	console.log("Objectbit **** " + params.objectbit);
	
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);

	if (params.objectbit.substring(0, 1) != "?") {
		var time = process.hrtime();
		var handler = function (e, result) {
			if (!e) {
				var diff = process.hrtime(time);
				var objectcount = result.toNumber();
				console.log("object data count: "+(diff[0]+(diff[1] / 1e9))+" for record count: "+objectcount);

				if (objectcount > 60000) {
					timeobjectstep = process.hrtime();

					var start = 0;
					var length = objectcount;

					// is this is the only term being searched one, activate paging
					if (params.subjectbit.substring(0, 1) == "?" && params.objectbit.substring(0, 1) != "?"
							 && params.graphbit.substring(0, 1) == "?" && params.predicatebit.substring(0, 1) == "?") {

						start = params.offset;
						length = params.limit;
						params.totalcount = objectcount;
					}

					var quadResultArrayTemp = new Array();
					objectResultsStep(start, length, params, quadResultArray, quadResultArrayTemp, query, destination, theOptions);
				} else {
					objectResults(params, quadResultArray, query, destination, theOptions);
				}
			} else {
				console.error(e);
				return;
			}
		};
		result = rdfdataInstance.objectindexcount.call("0x" + web3.sha3(params.objectbit), handler);
	} else {
		predicateResultsCount(params, quadResultArray, query, destination, theOptions);
	}
}

function objectResults(params, quadResultArray, query, destination, theOptions) {

	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	
	if (params.objectbit.substring(0, 1) != "?") {
		var time = process.hrtime();
		var handler = function (e, result) {
			if (!e) {
				var diff = process.hrtime(time);
				console.log("object data retrieval: "+(diff[0]+(diff[1] / 1e9))+" for record count: "+result.length);
				//console.log("RESULT OBJECT 2b = " + result);
				quadResultArrayTemp = result;
				if (quadResultArray.length > 0) {
					var timecheck = process.hrtime();
					quadResultArray = checkForArrayMatches(quadResultArray, quadResultArrayTemp);
					var diffcheck = process.hrtime(timecheck);
					console.log("object intersection: "+(diffcheck[0]+(diffcheck[1] / 1e9))+" final count: "+quadResultArray.length);
					//console.log("RESULT OBJECT 2c = " + quadResultArray);
				} else {
					quadResultArray = quadResultArrayTemp;
				}
				predicateResultsCount(params, quadResultArray, query, destination, theOptions);
			} else {
				console.error(e);
				return;
			}
		};
		result = rdfdataInstance.getObjectArray.call("0x" + web3.sha3(params.objectbit), handler);
	} else {
		predicateResultsCount(params, quadResultArray, query, destination, theOptions);
	}
}

function objectResultsStep(index, objectcount, params, quadResultArray, quadResultArrayTemp, query, destination, theOptions) {

	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	
	if (index < objectcount) {
		var handler = function (e, result) {
			if (!e) {
				quadResultArrayTemp[quadResultArrayTemp.length] = result;
				index = index + 1;
				objectResultsStep(index, objectcount, params, quadResultArray, quadResultArrayTemp, query, destination, theOptions);
			} else {
				console.error(e);
				return;
			}
		};
		rdfdataInstance.getObjectArrayEntry.call("0x" + web3.sha3(params.objectbit), index, handler);
	} else {
		var diffobjectstep = process.hrtime(timeobjectstep);
		console.log("get object stepping: "+(diffobjectstep[0]+(diffobjectstep[1] / 1e9))+" for results length: "+objectcount);

		if (quadResultArray.length > 0) {
			var timecheck = process.hrtime();
			quadResultArray = checkForArrayMatches(quadResultArray, quadResultArrayTemp);
			var diffcheck = process.hrtime(timecheck);
			console.log("object intersection: "+(diffcheck[0]+(diffcheck[1] / 1e9))+" final count: "+quadResultArray.length);
		} else {
			quadResultArray = quadResultArrayTemp;
		}

		predicateResultsCount(params, quadResultArray, query, destination, theOptions);
	}
}

/***** PREDICATE FUNCTIONS *****/

var timepredicatestep = 0;

function predicateResultsCount(params, quadResultArray, query, destination, theOptions) {
	//console.log("QUERY LIMIT **** " + query.limit);
	console.log("Predicatebit **** " + params.predicatebit);
	
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);

	if (params.predicatebit.substring(0, 1) != "?") {
		var time = process.hrtime();
		var handler = function (e, result) {
			if (!e) {
				var diff = process.hrtime(time);
				var predicatecount = result.toNumber();
				console.log("predicate data count: "+(diff[0]+(diff[1] / 1e9))+" for record count: "+predicatecount);

				if (predicatecount > 60000) {
					timepredicatestep = process.hrtime();

					var start = 0;
					var length = predicatecount;

					// is this is the only term being searched one, activate paging
					if (params.subjectbit.substring(0, 1) == "?" && params.objectbit.substring(0, 1) == "?"
							 && params.graphbit.substring(0, 1) == "?" && params.predicatebit.substring(0, 1) != "?") {

						start = params.offset;
						length = params.limit;
						params.totalcount = predicatecount;
					}

					var quadResultArrayTemp = new Array();
					predicateResultsStep(start, length, params, quadResultArray, quadResultArrayTemp, query, destination, theOptions);
				} else {
					predicateResults(params, quadResultArray, query, destination, theOptions);
				}
			} else {
				console.error(e);
				return;
			}
		};
		result = rdfdataInstance.predicateindexcount.call("0x" + web3.sha3(params.predicatebit), handler);
	} else {
		graphResults(params, quadResultArray, query, destination, theOptions);
	}
}

function predicateResults(params, quadResultArray, query, destination, theOptions) {

	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	
	if (params.predicatebit.substring(0, 1) != "?") {
		var time = process.hrtime();
		var handler = function (e, result) {
			if (!e) {
				var diff = process.hrtime(time);
				console.log("predicate data retrieval: "+(diff[0]+(diff[1] / 1e9))+" for record count: "+result.length);
				//console.log("RESULT PREDICATE 3b = " + result);
				quadResultArrayTemp = result;
				if (quadResultArray.length > 0) {
					var timecheck = process.hrtime();
					quadResultArray = checkForArrayMatches(quadResultArray, quadResultArrayTemp);
					var diffcheck = process.hrtime(timecheck);
					console.log("predicate intersection: "+(diffcheck[0]+(diffcheck[1] / 1e9))+" final count: "+quadResultArray.length);
					//console.log("RESULT PREDICATE 3c = " + quadResultArray);
				} else {
					quadResultArray = quadResultArrayTemp;
				}
				graphResults(params, quadResultArray, query, destination, theOptions);
			} else {
				console.error(e);
				return;
			}
		};
		result = rdfdataInstance.getPredicateArray.call("0x" + web3.sha3(params.predicatebit), handler);
	} else {
		graphResults(params, quadResultArray, query, destination, theOptions);
	}
}

function predicateResultsStep(index, predicatecount, params, quadResultArray, quadResultArrayTemp, query, destination, theOptions) {

	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);
	
	if (index < predicatecount) {
		var handler = function (e, result) {
			if (!e) {
				quadResultArrayTemp[quadResultArrayTemp.length] = result;
				index = index + 1;
				predicateResultsStep(index, predicatecount, params, quadResultArray, quadResultArrayTemp, query, destination, theOptions);
			} else {
				console.error(e);
				return;
			}
		};
		rdfdataInstance.getPredicateArrayEntry.call("0x" + web3.sha3(params.predicatebit), index, handler);
	} else {
		var diffpredicatestep = process.hrtime(timepredicatestep);
		console.log("get predicate stepping: "+(diffpredicatestep[0]+(diffpredicatestep[1] / 1e9))+" for results length: "+predicatecount);

		if (quadResultArray.length > 0) {
			var timecheck = process.hrtime();
			quadResultArray = checkForArrayMatches(quadResultArray, quadResultArrayTemp);
			var diffcheck = process.hrtime(timecheck);
			console.log("predicate intersection: "+(diffcheck[0]+(diffcheck[1] / 1e9))+" final count: "+quadResultArray.length);
		} else {
			quadResultArray = quadResultArrayTemp;
		}

		graphResults(params, quadResultArray, query, destination, theOptions);
	}
}

/***** FINISH OFF FUNCTIONS *****/

var timequads;
var timedata;

function graphResults(params, quadResultArray, query, destination, theOptions) {
	//console.log("QUERY LIMIT **** " + query.limit);
	//console.log("Graphbit **** " + params.graphbit);
	
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);

	var handler = function (e, result) {
		if (!e) {
			var datasetTotal = result.toNumber();
			//console.log("RESULTS 5a = " + datasetTotal);

			if (params.subjectbit.substring(0, 1) == "?" && params.objectbit.substring(0, 1) == "?"
					&& params.predicatebit.substring(0, 1) == "?" && params.graphbit.substring(0, 1) == "?") {

				var lastRecord = params.offset + params.limit;
				if (lastRecord > datasetTotal) lastRecord = datasetTotal;
				//console.log("RESULTS 5b = " + lastRecord + " " + params.offset + " " + params.limit + " " + quadResultArray.length);
				timequads = process.hrtime()
				getQuad(params.offset, lastRecord, params, quadResultArray, query, destination, datasetTotal, theOptions);
			} else {
				var totalcount = quadResultArray.length;
				if (params.totalcount) {
					totalcount = params.totalcount;
				}

				//console.log("RESULTS 5c = " + totalcount);
				quadResultArray = trimArray(quadResultArray, params);
				//console.log("RESULTS 5d = " + quadResultArray.length);
				var resultArray = new Array();
				timedata = process.hrtime()
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
		rdfdataInstance.dataindex.call(next, handler);

	} else {
		var diffquads = process.hrtime(timequads);
		console.log("get result indexes: "+(diffquads[0]+(diffquads[1] / 1e9))+" for results length: "+quadResultArray.length);

		//console.log("RESULT 6b " + quadResultArray.length);
		var resultArray = new Array();
		timedata = process.hrtime()
		retrieveData(0, resultArray, params, quadResultArray, query, destination, datasetTotal, theOptions)
	}

}

function retrieveData(next, resultArray, params, quadResultArray, query, destination, datasetTotal, theOptions){
	//console.log("RESULT 7a " + resultArray.length);
	
	var rdfdataContract = web3.eth.contract(theOptions.contracts.rdfdatastore.abi);
	var rdfdataInstance = rdfdataContract.at(theOptions.rdfdata_address);

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
			var diffdata = process.hrtime(timedata);
			console.log("get result quads: "+(diffdata[0]+(diffdata[1] / 1e9))+" for results length: "+quadResultArray.length);
			finishOff(resultArray, query, destination, datasetTotal);
		}
	} else {
		var diffdata = process.hrtime(timedata);
		console.log("get result quads: "+(diffdata[0]+(diffdata[1] / 1e9))+" for results length: "+quadResultArray.length);
		finishOff(resultArray, query, destination, datasetTotal, theOptions);
	}
}

function finishOff(result, query, destination, datasetTotal, theOptions){
	//console.log("The End");

	var timefinish = process.hrtime()

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

	var difffinish = process.hrtime(timefinish);
	console.log("finishOff: "+(difffinish[0]+(difffinish[1] / 1e9))+" for results length: "+count);

	var diffcheck = process.hrtime(totaltime);
	console.log("total time: "+(diffcheck[0]+(diffcheck[1] / 1e9)));
}


function checkForArrayMatches(originalArray, compareArray){
	var EMPTY_BYTES = "0x0000000000000000000000000000000000000000000000000000000000000000";

	let a = new Set(originalArray);
	let b = new Set(compareArray);
	let c = new Set([EMPTY_BYTES]);

	var temp = new Set([...a].filter(x => b.has(x)));
	temp = new Set([...temp].filter(x => !c.has(x)));

	originalArray = [...temp];

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

module.exports = EthereumBlockchainDatasource2;
