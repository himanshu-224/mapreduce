#ifndef KEY_VALUE
#define KEY_VALUE

#include <iostream>
#include <stdio.h>
#include <deque>
#include <vector>
#include <stdint.h>

#include "mapReduce.h"
#include "logging.h"

using namespace std;

template <class K,class V>
class KeyValue
{
	friend class MapReduce;

private:
	// Add private members of class here
	MapReduce *mr;
	MPI_Comm comm;
	Logging *logobj;
	int me;
	
	//structure to store key values
	struct KValue {
		int keysize;
		int vsize;
		K key;
		V value;
	};
	
	deque<KValue> kv;
	int nkv;
	// Type codes char - 0, int - 1, float - 2, double - 3, string - 4
	int keytype;
	int valuetype;


	// file info

	char *filename;                   // filename to store KV if needed
	FILE *fp;                         // file ptr
	int fileflag;                     // 1 if file exists, 0 if not
	
	void setType();

public:
	KeyValue(class MapReduce *, MPI_Comm, Logging *);
	~KeyValue();
	
	void add(K, V);
};

#endif