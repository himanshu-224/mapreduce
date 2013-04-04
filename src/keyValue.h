#ifndef KEY_VALUE
#define KEY_VALUE

#include <iostream>
#include <stdio.h>
#include <deque>
#include <vector>
#include <stdint.h>
#include <mpi.h>
#include <stdlib.h>
#include <time.h>
#include <string>

#include "mapReduce.h"
#include "logging.h"

#define INT_MAX 0x7FFFFFFF

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
		int ksize;
		int vsize;
		K key;
		V value;
	};
	
	deque<KValue *> kv;
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
	KeyValue();
	KeyValue(class MapReduce *, MPI_Comm, Logging *);
	~KeyValue();
	
	void add(K, V);
	void printkv();
};

template <class K,class V>
KeyValue<K,V>::KeyValue()
{
	int curtime = time(NULL);

	int n = 100;
	filename = (char*)malloc(n);
	if(filename == NULL)
	{
		char str[255];
		char name[255];
		int namelen;
		MPI_Get_processor_name(name,&namelen);
		name[namelen]='\0';
		
		sprintf(str,"ERROR on proc %d (%s): Failed to allocate %d	 bytes for array filename\n",me,name,n);
		logobj->error(str);
	}
	nkv = 0;
	setType();
	
	sprintf(filename,"/export/mapReduce/keyValue/kv.%d",curtime);
	fileflag = 0;
	fp=NULL;
}
template <class K, class V>
KeyValue<K,V>::KeyValue(MapReduce *mr_caller, MPI_Comm communicator, Logging *log_caller)
{
	mr = mr_caller;
	comm = communicator;
	logobj = log_caller;
	MPI_Comm_rank(comm,&me);
	int curtime = time(NULL);

	int n = 100;
	filename = (char*)malloc(n);
	if(filename == NULL)
	{
		char str[255];
		char name[255];
		int namelen;
		MPI_Get_processor_name(name,&namelen);
		name[namelen]='\0';
		
		sprintf(str,"ERROR on proc %d (%s): Failed to allocate %d	 bytes for array filename\n",me,name,n);
		logobj->error(str);
	}
	nkv = 0;
	setType();
	
	sprintf(filename,"/export/mapReduce/keyValue/kv.%d",curtime);
	fileflag = 0;
	fp=NULL;
}

template <class K, class V>
KeyValue<K,V>::~KeyValue()
{
	if(fileflag)
	{
		remove(filename);
	}
	delete [] filename;
}

// set the type of key and value, which will be sent to reducer later
template <>
void KeyValue<char,char>::setType()
{
	keytype = 0;
	valuetype = 0;
}

template <>
void KeyValue<char,int>::setType()
{
	keytype = 0;
	valuetype = 1;
}

template <>
void KeyValue<char,float>::setType()
{
	keytype = 0;
	valuetype = 2;
}

template <>
void KeyValue<char,double>::setType()
{
	keytype = 0;
	valuetype = 3;
}

template <>
void KeyValue<char,string>::setType()
{
	keytype = 0;
	valuetype = 4;
}

template <>
void KeyValue<int,char>::setType()
{
	keytype = 1;
	valuetype = 0;
}

template <>
void KeyValue<int,int>::setType()
{
	keytype = 1;
	valuetype = 1;
}

template <>
void KeyValue<int,float>::setType()
{
	keytype = 1;
	valuetype = 2;
}

template <>
void KeyValue<int,double>::setType()
{
	keytype = 1;
	valuetype = 3;
}

template <>
void KeyValue<int,string>::setType()
{
	keytype = 1;
	valuetype = 4;
}

template <>
void KeyValue<float,char>::setType()
{
	keytype = 2;
	valuetype = 0;
}

template <>
void KeyValue<float,int>::setType()
{
	keytype = 2;
	valuetype = 1;
}

template <>
void KeyValue<float,float>::setType()
{
	keytype = 2;
	valuetype = 2;
}

template <>
void KeyValue<float,double>::setType()
{
	keytype = 2;
	valuetype = 3;
}

template <>
void KeyValue<float,string>::setType()
{
	keytype = 2;
	valuetype = 4;
}

template <>
void KeyValue<double,char>::setType()
{
	keytype = 3;
	valuetype = 0;
}

template <>
void KeyValue<double,int>::setType()
{
	keytype = 3;
	valuetype = 1;
}

template <>
void KeyValue<double,float>::setType()
{
	keytype = 3;
	valuetype = 2;
}

template <>
void KeyValue<double,double>::setType()
{
	keytype = 3;
	valuetype = 3;
}

template <>
void KeyValue<double,string>::setType()
{
	keytype = 3;
	valuetype = 4;
}

template <>
void KeyValue<string,char>::setType()
{
	keytype = 4;
	valuetype = 0;
}

template <>
void KeyValue<string,int>::setType()
{
	keytype = 4;
	valuetype = 1;
}

template <>
void KeyValue<string,float>::setType()
{
	keytype = 4;
	valuetype = 2;
}

template <>
void KeyValue<string,double>::setType()
{
	keytype = 4;
	valuetype = 3;
}

template <>
void KeyValue<string,string>::setType()
{
	keytype = 4;
	valuetype = 4;
}

// Add a single key value pair 

template <class K, class V>
void KeyValue<K,V>::add(K key, V value)
{
	KValue *newkv = new KValue;
	newkv->key = key;
	newkv->value = value;
	/*if(keytype==4){
		newkv.ksize = key.size();
	}
	else{
		newkv.ksize = sizeof(K);
	}
	if(valuetype==4){
		newkv.vsize = value.size();
	}
	else{
		newkv.vsize = sizeof(V);
	}*/
	newkv->ksize = sizeof(key);
	newkv->vsize = sizeof(value);
	long tsize = newkv->ksize+newkv->vsize;
	if(tsize > INT_MAX)					// A limit on size of key value pair
		logobj->error("Single Key Value pair size exceeds int size");
	kv.push_back(newkv);
	nkv++;
}

//test function
template <class K, class V>
void KeyValue<K,V>::printkv()
{
	for(int index=0;index < kv.size(); ++index){
		cout<<index+1<<"\tKey: "<<kv.at(index)->key<<"\tValue: "<<kv.at(index)->value<<endl;
	}
}


#endif