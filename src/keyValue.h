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
#include<string.h>
#include <algorithm>
#include <functional>
#include <iterator>

#include "logging.h"

#define INT_MAX 0x7FFFFFFF
#define STR_MAX 0x7FFFF
#define NUM_KEY 2
#define SIZE_NXTMSG 3
#define KEYVALUE 4
#define END_MAP_TASK 5
#define END_MAP 6
#define NUM_SFILE 10

using namespace std;


template <class K,class V>
class KeyValue
{

private:
	// Add private members of class here
	MPI_Comm comm;
	Logging logobj;
	int me;
	
	string kvDir;
	//structure to store key values
	struct KValue {
		int ksize;
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

	/*char *filename;                   // filename to store KV if needed
	FILE *fp;                         // file ptr
	int fileflag;                     // 1 if file exists, 0 if not
	*/
	deque<string> filename;
	string kvfile;
	//deque<FILE *> fp;
	int recvcomp;
	
	void setType();
	
public:
	KeyValue();
	KeyValue(MPI_Comm, Logging &, string);
	~KeyValue();
	
	void add(K, V);
	void printkv();
	void printkv(deque<KValue> tempkv);
	void sortkv(int);
	int sortkv();
	static bool compkv(const KValue&, const KValue&);
	void partitionkv(int, int, int(*hashfunc)(K, int));
	void partitionkv(int, int);
	int defaulthash(K, int);
	void copykv(KValue *, KValue);
	string encodekv(KValue);
	void decodekv(KValue *, string);
	
	void receivekv(int);
	void sortfiles();
	string getkvDir()
	{
		return kvDir;
	}
};

//Implementation part

string itos(int num);
vector<string> split(string s, char delim);

string getkvstr(ifstream filep);

template <class K, class V>
string getfilename(KeyValue<K,V> *,int);

template <class K, class V>
string getfilename(KeyValue<K,V> *obj, int rank)
{
	string filename;
	int curtime = time(NULL);
	filename = obj->getkvDir() + "kv_" + itos(rank) + "." + itos(curtime) +".txt";
	return filename;
}

string getkvstr(ifstream filep)
{
	string str;
	char c;
	int bsize;
	char *buffer;
	while(filep.good())
	{
		c = filep.get();
		if (c==':'){
			bsize = atoi(str.c_str());
			buffer = (char*)malloc(bsize);
			filep.read(buffer,bsize);
			str.clear();
			str = string(buffer,bsize);
			delete [] buffer;
			return str;
		}
		else{
			str+=string(1,c);
		}
	}
}
/*template <class K,class V>
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
		logobj.error(str);
	}
	nkv = 0;
	setType();
	
	sprintf(filename,"/export/mapReduce/keyValue/kv.%d",curtime);
	fileflag = 0;
	fp=NULL;
}*/
template <class K, class V>
KeyValue<K,V>::KeyValue(MPI_Comm communicator, Logging &log_caller, string kvDir_caller)
{
	comm = communicator;
	logobj = log_caller;
	MPI_Comm_rank(comm,&me);
	kvDir = kvDir_caller;
	/*int curtime = time(NULL);

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
		logobj.error(str);
	}*/
	nkv = 0;
	setType();
	
	//sprintf(filename,"/export/mapReduce/keyValue/kv.%d",curtime);
	//fileflag = 0;
	//fp=NULL;
}

template <class K, class V>
KeyValue<K,V>::~KeyValue()
{
	/*if(fileflag)
	{
		remove(filename);
	}
	delete [] filename;*/
}

// set the type of key and value, which will be sent to reducer later
template <>
inline void KeyValue<char,char>::setType()
{
	keytype = 0;
	valuetype = 0;
}

template <>
inline void KeyValue<char,int>::setType()
{
	keytype = 0;
	valuetype = 1;
}

template <>
inline void KeyValue<char,float>::setType()
{
	keytype = 0;
	valuetype = 2;
}

template <>
inline void KeyValue<char,double>::setType()
{
	keytype = 0;
	valuetype = 3;
}

template <>
inline void KeyValue<char,string>::setType()
{
	keytype = 0;
	valuetype = 4;
}

template <>
inline void KeyValue<int,char>::setType()
{
	keytype = 1;
	valuetype = 0;
}

template <>
inline void KeyValue<int,int>::setType()
{
	keytype = 1;
	valuetype = 1;
}

template <>
inline void KeyValue<int,float>::setType()
{
	keytype = 1;
	valuetype = 2;
}

template <>
inline void KeyValue<int,double>::setType()
{
	keytype = 1;
	valuetype = 3;
}

template <>
inline void KeyValue<int,string>::setType()
{
	keytype = 1;
	valuetype = 4;
}

template <>
inline void KeyValue<float,char>::setType()
{
	keytype = 2;
	valuetype = 0;
}

template <>
inline void KeyValue<float,int>::setType()
{
	keytype = 2;
	valuetype = 1;
}

template <>
inline void KeyValue<float,float>::setType()
{
	keytype = 2;
	valuetype = 2;
}

template <>
inline void KeyValue<float,double>::setType()
{
	keytype = 2;
	valuetype = 3;
}

template <>
inline void KeyValue<float,string>::setType()
{
	keytype = 2;
	valuetype = 4;
}

template <>
inline void KeyValue<double,char>::setType()
{
	keytype = 3;
	valuetype = 0;
}

template <>
inline void KeyValue<double,int>::setType()
{
	keytype = 3;
	valuetype = 1;
}

template <>
inline void KeyValue<double,float>::setType()
{
	keytype = 3;
	valuetype = 2;
}

template <>
inline void KeyValue<double,double>::setType()
{
	keytype = 3;
	valuetype = 3;
}

template <>
inline void KeyValue<double,string>::setType()
{
	keytype = 3;
	valuetype = 4;
}

template <>
inline void KeyValue<string,char>::setType()
{
	keytype = 4;
	valuetype = 0;
}

template <>
inline void KeyValue<string,int>::setType()
{
	keytype = 4;
	valuetype = 1;
}

template <>
inline void KeyValue<string,float>::setType()
{
	keytype = 4;
	valuetype = 2;
}

template <>
inline void KeyValue<string,double>::setType()
{
	keytype = 4;
	valuetype = 3;
}

template <>
inline void KeyValue<string,string>::setType()
{
	keytype = 4;
	valuetype = 4;
}

// Add a single key value pair 

template <class K, class V>
void KeyValue<K,V>::add(K key, V value)
{
	KValue newkv;
	newkv.key = key;
	newkv.value = value;
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
	newkv.ksize = sizeof(key);
	newkv.vsize = sizeof(value);
	long tsize = newkv.ksize+newkv.vsize;
	if(tsize > INT_MAX)					// A limit on size of key value pair
		logobj.error("Single Key Value pair size exceeds int size");
	kv.push_back(newkv);
	nkv++;
}

//test function
template <class K, class V>
void KeyValue<K,V>::printkv()
{
	for(int index=0;index < kv.size(); ++index){
		cout<<index+1<<"\tKey: "<<kv.at(index).key<<"\tValue: "<<kv.at(index).value<<endl;
	}
}

template <class K,class V>
void KeyValue<K,V>::printkv(deque<KValue> tempkv)
{
	for(int index=0;index < tempkv.size(); ++index){
		cout<<index+1<<"\tKey: "<<tempkv.at(index).key<<"\tValue: "<<tempkv.at(index).value<<endl;
	}
}
//sort the key values according to key and value
template <class K, class V>
void KeyValue<K,V>::sortkv(int numkey)
{
	sort(kv.begin(),kv.begin()+numkey,compkv);
}

template <class K,class V>
int KeyValue<K,V>::sortkv()
{
	int numkey = nkv;
	sort(kv.begin(),kv.begin()+numkey,compkv);
    logobj.localLog("check");
	return numkey;
}

template <>
inline bool KeyValue<char,char>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<char,int>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<char,float>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<char,double>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<char,string>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return lexicographical_compare(k1.value.begin(),k1.value.end(),k2.value.begin(),k2.value.end());
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<int,char>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<int,int>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<int,float>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<int,double>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<int,string>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return lexicographical_compare(k1.value.begin(),k1.value.end(),k2.value.begin(),k2.value.end());
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<float,char>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<float,int>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<float,float>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<float,double>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<float,string>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return lexicographical_compare(k1.value.begin(),k1.value.end(),k2.value.begin(),k2.value.end());
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<double,char>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<double,int>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<double,float>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<double,double>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<double,string>::compkv(const KValue& k1, const KValue& k2)
{
	if(k1.key==k2.key)
		return lexicographical_compare(k1.value.begin(),k1.value.end(),k2.value.begin(),k2.value.end());
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<string,char>::compkv(const KValue& k1, const KValue& k2)
{
	if (lexicographical_compare(k1.key.begin(),k1.key.end(),k2.key.begin(),k2.key.end()))
		return true;
	else if(lexicographical_compare(k2.key.begin(),k2.key.end(),k1.key.begin(),k1.key.end()))
		return false;
	else
		return (k1.value<k2.value);
}

template <>
inline bool KeyValue<string,int>::compkv(const KValue& k1, const KValue& k2)
{
	if (lexicographical_compare(k1.key.begin(),k1.key.end(),k2.key.begin(),k2.key.end()))
		return true;
	else if(lexicographical_compare(k2.key.begin(),k2.key.end(),k1.key.begin(),k1.key.end()))
		return false;
	else
		return (k1.value<k2.value);
}

template <>
inline bool KeyValue<string,float>::compkv(const KValue& k1, const KValue& k2)
{
	if (lexicographical_compare(k1.key.begin(),k1.key.end(),k2.key.begin(),k2.key.end()))
		return true;
	else if(lexicographical_compare(k2.key.begin(),k2.key.end(),k1.key.begin(),k1.key.end()))
		return false;
	else
		return (k1.value<k2.value);
}

template <>
inline bool KeyValue<string,double>::compkv(const KValue& k1, const KValue& k2)
{
	if (lexicographical_compare(k1.key.begin(),k1.key.end(),k2.key.begin(),k2.key.end()))
		return true;
	else if(lexicographical_compare(k2.key.begin(),k2.key.end(),k1.key.begin(),k1.key.end()))
		return false;
	else
		return (k1.value<k2.value);
}

template <>
inline bool KeyValue<string,string>::compkv(const KValue& k1, const KValue& k2)
{
	if (lexicographical_compare(k1.key.begin(),k1.key.end(),k2.key.begin(),k2.key.end()))
		return true;
	else if(lexicographical_compare(k2.key.begin(),k2.key.end(),k1.key.begin(),k1.key.end()))
		return false;
	else
		return lexicographical_compare(k1.value.begin(),k1.value.end(),k2.value.begin(),k2.value.end());
}

template <class	K, class V>
void KeyValue<K,V>::copykv(KValue *k1, KValue k2)
{
	k1->key = k2.key;
	k1->value = k2.value;
	k1->ksize = k2.ksize;
	k1->vsize = k2.vsize;
	
}

// PArtition function starts here
template <class K, class V>
void KeyValue<K,V>::partitionkv(int nump, int numkey, int(*hashfunc)(K key, int nump2))
{
	logobj.localLog("Entered partitionkv");
	KValue *kvalue= new KValue;
	vector<deque<KValue>> tempkv;
	vector<int> tempnkv(nump,0);
	int i,hvalue,j;
	string str,str2;
	int len;
	tempkv.resize(nump);
	if (nump ==1){
		for(i=0;i<numkey;i++){
			tempkv[0].push_back(kv.front());
			kv.pop_front();
			nkv--;
		}
		tempnkv[0] = numkey;
	}
	else
	{
		for(i=0; i<numkey; i++)
		{
			copykv(kvalue,kv.front());
			kv.pop_front();
			nkv--;
			hvalue = hashfunc(kvalue->key, nump);
			tempkv[hvalue].push_back(*kvalue);
			tempnkv[hvalue]++;
		}
	}
	logobj.localLog("KeyValue pair partiotioned");
	/*for(i=0;i<nump;i++)
	{
		cout<<"Proc "<<i<<endl;
		printkv(tempkv[i]);
	}*/
	for(i=0;i<nump;i++)
	{
		MPI_Send(&tempnkv[i],1,MPI_INT,i,NUM_KEY,comm);
		str.clear();
		for(j=0;j<tempnkv[i];j++)
		{
			str2=encodekv(tempkv[i].front());
			tempkv[i].pop_front();
			if (str.length()+str2.length() > STR_MAX)
			{
				char *buffer = strdup(str.c_str());
				len = str.length();
				MPI_Send(&len,1,MPI_INT,i,SIZE_NXTMSG,comm);
				MPI_Send(buffer,str.length(),MPI_CHAR,i,KEYVALUE,comm);
				str.clear();
			}
			str+=str2;
		}
		if(!str.empty())
		{
			len = str.length();
			MPI_Send(&len,1,MPI_INT,i,SIZE_NXTMSG,comm);
			MPI_Send(strdup(str.c_str()),str.length(),MPI_CHAR,i,KEYVALUE,comm);
		}
		len = 0;
		MPI_Send(&len,1,MPI_INT,i,END_MAP_TASK,comm);
	}
}

template <class K, class V>
void KeyValue<K,V>::partitionkv(int nump, int numkey)
{
	logobj.localLog("Entered partitionkv");
    //usleep(1000000);
	KValue *kvalue= new KValue;
	vector<deque<KValue>> tempkv;
	vector<int> tempnkv(nump,0);
	int i,hvalue,j,len;
	string str,str2;
	tempkv.resize(nump);
	if (nump ==1){
		for(i=0;i<numkey;i++){
			tempkv[0].push_back(kv.front());
			kv.pop_front();
			nkv--;
		}
		tempnkv[0] = numkey;
	}
	else{
		for(i=0; i<numkey; i++)
		{
			copykv(kvalue,kv.front());
			kv.pop_front();
			nkv--;
			hvalue = defaulthash(kvalue->key, nump);
			tempkv[hvalue].push_back(*kvalue);
			tempnkv[hvalue]++;
		}
	}
	logobj.localLog("KeyValue pair partiotioned");
	/*for(i=0;i<nump;i++)
	{
		cout<<"Proc "<<i<<endl;
		printkv(tempkv[i]);
	}*/
	for(i=0;i<nump;i++)
	{
		MPI_Send(&tempnkv[i],1,MPI_INT,i,NUM_KEY,comm);
		str.clear();
		for(j=0;j<tempnkv[i];j++)
		{
			str2=encodekv(tempkv[i].front());
			tempkv[i].pop_front();
			if (str.length()+str2.length() > STR_MAX)
			{
				char *buffer = strdup(str.c_str());
				len = str.length();
				MPI_Send(&len,1,MPI_INT,i,SIZE_NXTMSG,comm);
				MPI_Send(buffer,str.length(),MPI_CHAR,i,KEYVALUE,comm);
				str.clear();
			}
			str+=str2;
		}
		if(!str.empty())
		{
			len = str.length();
			MPI_Send(&len,1,MPI_INT,i,SIZE_NXTMSG,comm);
			MPI_Send(strdup(str.c_str()),str.length(),MPI_CHAR,i,KEYVALUE,comm);
		}
		len = 0;
		MPI_Send(&len,1,MPI_INT,i,END_MAP_TASK,comm);
	}
}

template <class K, class V>
int KeyValue<K,V>::defaulthash(K key, int nump)
{
	hash<K> hash_fn;
	size_t v = hash_fn(key)%nump + 1;
	//v = rand()%nump + 1;
	return (int)v;
}

template <>
inline string KeyValue<int,int>::encodekv(KValue k)
{
	string str = itos(k.key) + "#" + itos(k.ksize) + "#" + itos(k.value) + "#" + itos(k.vsize) + "\n";
	str = itos(str.length())+":"+str;
	return str;
}

template <>
inline void KeyValue<int,int>::decodekv(KValue *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->key = atoi(vstr[0].c_str());
	k1->ksize = atoi(vstr[1].c_str());
	k1->value = atoi(vstr[2].c_str());
	k1->vsize = atoi(vstr[3].substr(0,vstr[3].length()-1).c_str());
}

//Receive key value pair from other process
template <class K, class V>
void KeyValue<K,V>::receivekv(int nump)
{
	recvcomp = 0;
	vector<string> procfile;
	procfile.resize(nump);
	//vector<ofstream> procfp;
	ofstream procfp;
	vector<int> proclen(nump,-1);
	//procfp.resize(nump);
	MPI_Status status;
	int source;
	int nkv;
	char *buffer;
	if(nump < 1){
		string err = "Error: Total number of map process is less than 1. Exiting!!";
		logobj.error(err);
	}
	while(1)
	{
		MPI_Probe(MPI_ANY_SOURCE,MPI_ANY_TAG,comm,&status);
		source = status.MPI_SOURCE;
		switch(status.MPI_TAG)
		{
			case NUM_KEY:
				if(!procfile[source].empty()){
					string err = "Message for end of previous maptask of process " + itos(source)+" is not completed.";
					err+="Exiting!!";
					logobj.error(err);
				}
				procfile[source] = getfilename(this, source);
				//procfp[source].open(procfile[source].c_str(), ios::out | ios::app | ios::binary);
				MPI_Recv(&nkv,1,MPI_INT,source,NUM_KEY,comm,&status);
				break;
			
			case SIZE_NXTMSG:
				MPI_Recv(&proclen[source],1,MPI_INT,source,SIZE_NXTMSG,comm,&status);
				break;
				
			case KEYVALUE:
				if(proclen[source] == -1){
					string err;
					err = "Receiving data from rank:"+itos(source)+" Could not receive size of data. Exiting!!";
					logobj.error(err);
				}
				buffer = (char*)malloc(proclen[source]);
				MPI_Recv(buffer,proclen[source],MPI_CHAR,source,KEYVALUE,comm,&status);
				procfp.open(procfile[source].c_str(), ios::out | ios::app | ios::binary);
				procfp.write(buffer,proclen[source]);
				procfp.close();
				delete [] buffer;
				proclen[source] = -1;
				break;
				
			case END_MAP_TASK:
				if(proclen[source] != -1){
					string err;
					err = "Rank:"+itos(source)+" Could not receive some key value pair. Exiting!!";
					logobj.error(err);
				}
				MPI_Recv(NULL,0,MPI_INT,source,END_MAP_TASK,comm,&status);
				//procfp[source].close();
				filename.push_back(procfile[source]);
				procfile[source].clear();
				break;
				
			case END_MAP:
				if(!procfile[source].empty()){
					string err = "Rank:"+itos(source)+" Some map task still not completed.";
					err+=" Received end of mapping part message. Exiting!!";
					logobj.error(err);
				}
				MPI_Recv(NULL,0,MPI_INT,source,END_MAP,comm,&status);
				nump--;
				if(nump==0){
					recvcomp = 1;
					return;
				}
				break;
				
			default:
				string err;
				err = "Rank:" + itos(source)+ " Message of unknowntag received. Ignoring!!";
				usleep(10000);
				break;
		}
	}
}

template <class K, class V>
void KeyValue<K,V>::sortfiles()
{
	string kvfilename[NUM_SFILE];
	ifstream kvfilep[NUM_SFILE];
	string newfile;
	ofstream newfilep;
	vector<KValue> tempkv;
	vector<int> index(NUM_SFILE,-1);
	tempkv.resize(NUM_SFILE);
	int i,minpos,numfiles;
	string buffer;
	KValue temp;
	while(1){
		if((filename.size() < NUM_SFILE) || (recvcomp == 0)){
			usleep(100000);
			continue;
		}
		numfiles = min((int)filename.size(),NUM_SFILE);
		for(i=0;i<numfiles;i++){
			kvfilename[i] = filename.front();
			filename.pop_front();
			kvfilep[i].open(kvfilename[i].c_str());
		}
		newfile = getfilename(this,me);
		newfilep.open(newfile.c_str(), ios::out | ios::app | ios::binary);
		for(i=0;i<numfiles;i++){
			buffer = getkvstr(kvfilep[i]);
			decodekv(&temp,buffer);
			copykv(&tempkv[i],temp);
			index[i] = i;
		}
		while(1){
			minpos = distance(tempkv.begin(),min_element(tempkv.begin(),tempkv.end(),compkv));
			buffer.clear();
			buffer = encodekv(tempkv.at(minpos));
			newfilep.write(strdup(buffer.c_str()),buffer.length());
			buffer.clear();
			if(kvfilep[index[minpos]].good()){
				buffer = getkvstr(kvfilep[index[minpos]]);
				decodekv(&temp,buffer);
				copykv(&tempkv[minpos],temp);
			}
			else{
				tempkv.erase(tempkv.begin() + minpos);
				kvfilep[index[minpos]].close();
				remove(kvfilename[index[minpos]].c_str());
				index.erase(index.begin() + minpos);
				if (tempkv.size() == 0){
					newfilep.close();
					filename.push_front(newfile);
					break;
				}
			}
		}
		if ((recvcomp == 1) && (filename.size() == 1)){
			kvfile = filename.front();
			filename.pop_front();
			return;
		}
	}
}
#endif