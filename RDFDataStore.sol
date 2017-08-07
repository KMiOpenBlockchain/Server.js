pragma solidity ^0.4.2;

contract RDFDataStore {  // 
	
	address public owner;
	
	uint public counter = 0;
	
	address[] public authorisedWriters;
	
	mapping (bytes32 => string) public data;
	mapping (bytes32 => uint) public quadcheck;
	mapping (uint => bytes32) public dataindex;

	mapping (bytes32 => mapping (bytes32 => uint8)) public quadlookup;
	
	// mappings to act as indexes to the stored data
	mapping (bytes32 => bytes32[]) public graphindex;
	mapping (bytes32 => bytes32[]) public subjectindex;
	mapping (bytes32 => bytes32[]) public predicateindex;
	mapping (bytes32 => bytes32[]) public objectindex;	
	
	mapping (bytes32 => uint) public graphindexcount;
	mapping (bytes32 => uint) public subjectindexcount;
	mapping (bytes32 => uint) public predicateindexcount;
	mapping (bytes32 => uint) public objectindexcount;

	modifier onlyWriter() {
		uint i=0;
		uint count = authorisedWriters.length;
		bool authorised = false;
		for (i=0; i<count;i++) {
			if (authorisedWriters[i] == msg.sender) {
				authorised = true;
				break;
			}
		}

		if (authorised == false && msg.sender != owner) throw;
		_;
	}

	modifier onlyOwner() {
		if (msg.sender != owner) throw;
		_;
	}

	event itemAdded(string graph, string subject, string predicate, string object, string quad);	
	event writerAdded(address writer);	
	event writerRemoved(address writer);	
	event duplicateDetected(string quad);	
	event debug(string astring);	
		
	function RDFDataStore() {
		owner = msg.sender;
	}
	
	// add new element to the data store and update the index mappings
	function addData(string graph, string subject, string predicate, string object, string quad) public onlyWriter returns (bool){
		
		bytes32 hash = sha3(quad);
		
		// check to see if this data is already stored and throw if it does 
		if (quadcheck[hash] != 0) {
			duplicateDetected(quad);
			return false;
		} else {
			data[hash] = quad;						
			quadcheck[hash] = 1;
			dataindex[counter] = hash;
			counter++;
			
			graphindex[sha3(graph)].push(hash);
			graphindexcount[sha3(graph)] = graphindex[sha3(graph)].length;
			subjectindex[sha3(subject)].push(hash);
			subjectindexcount[sha3(subject)] = subjectindex[sha3(subject)].length;
			predicateindex[sha3(predicate)].push(hash);
			predicateindexcount[sha3(predicate)] = predicateindex[sha3(predicate)].length;
			objectindex[sha3(object)].push(hash);
			objectindexcount[sha3(object)] = objectindex[sha3(object)].length;
			
			quadlookup[sha3(graph)][hash] = 1;
			quadlookup[sha3(subject)][hash] = 1;
			quadlookup[sha3(predicate)][hash] = 1;
			quadlookup[sha3(object)][hash] = 1;
			
			itemAdded(graph, subject, predicate, object, quad);
			return true;
		}
	}
	
	// get the size of the data store
	function getDataLength() public constant returns (uint length) {
		length = counter;
	}
	
	// return a quad string given an index
	function getDataFromIndex(uint which) public returns (string quadstring) {		
		if (which >= counter) throw;
	
		bytes32 hash = dataindex[which];
		quadstring = data[hash];
	}
	
	// return a quad given it quadhash
	function getDataFromQuadHash(bytes32 hash) public constant returns (string quad) {	
		if (quadcheck[hash] == 0) throw;		
		quad = data[hash];
	}
	
	// return a bytes32 array of hashes matching a graph search hash
	function getGraphArray(bytes32 which) public constant returns (bytes32[]) {
		return graphindex[which];
	}
	
	// return a bytes32 hash matching a graph search hash and array index value
	function getGraphArrayEntry(bytes32 which, uint index) public constant returns (bytes32) {
		return graphindex[which][index];
	}		
	
	// return a bytes32 array of hashes matching a subject search hash
	function getSubjectArray(bytes32 which) public constant returns (bytes32[]) {
		return subjectindex[which];
	}
	
	// return a bytes32 hash matching a subject search hash and array index value
	function getSubjectArrayEntry(bytes32 which, uint index) public constant returns (bytes32) {
		return subjectindex[which][index];
	}	
	
	// return a bytes32 array of hashes matching a predicate search hash
	function getPredicateArray(bytes32 which) public constant returns (bytes32[]) {
		return predicateindex[which];
	}
	
	// return a bytes32 hash matching a predicate search hash and array index value
	function getPredicateArrayEntry(bytes32 which, uint index) public constant returns (bytes32) {
		return predicateindex[which][index];
	}
	
	// return a bytes32 array of hashes matching a object search hash
	function getObjectArray(bytes32 which) public constant returns (bytes32[]) {
		return objectindex[which];
	}
	
	// return a bytes32 hash matching a object search hash and array index value
	function getObjectArrayEntry(bytes32 which, uint index) public constant returns (bytes32) {
		return objectindex[which][index];
	}
	
	// add an authorised Writer
	function addWriter(address writer) public onlyOwner {
		uint i=0;
		uint count = authorisedWriters.length;
		bool isfound = false;
		for (i=0; i<count;i++) {
			if (authorisedWriters[i] == writer) {
				isfound = true;
				break;
			}
		}
		if (isfound == false) {
			authorisedWriters.push(writer);
			writerAdded(writer);
		}
	}

	// remove an authorised Writer
	function removeWriter(address writer) public onlyOwner {
		uint i=0;
		uint count = authorisedWriters.length;
		for (i=0; i<count;i++) {
			if (authorisedWriters[i] == writer) {
				if (i < authorisedWriters.length-1) {
					authorisedWriters[i] = authorisedWriters[authorisedWriters.length-1];
				}
				authorisedWriters.length--;
				writerRemoved(writer); 		
				break;
			}
		}
	}
	
		
	function() { throw; }
	
	function revoke() public onlyOwner {
		suicide(owner); // should I do this or set a flag to say it has been revoked?
	}	
}