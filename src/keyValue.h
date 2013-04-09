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
#include <err.h>

#include "dataStruc.h"
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

	
	deque<KValue<K,V> > kv;
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
	//deque<FILE *> fp;
	int recvcomp;
	
	void setType();
	
public:
    string kvfile;
    int counter;
    
	KeyValue();
	KeyValue(MPI_Comm, Logging &, string);
	~KeyValue();
	
	void add(K, V);
	void printkv();
	void printkv(KValue<K,V> tempkv);
	void printkv(deque<KValue<K,V> > tempkv);
	void sortkv(int);
	int sortkv();
	static bool compkv(const KValue<K,V>&, const KValue<K,V>&);
	void partitionkv(int, int, int(*hashfunc)(K, int));
	void partitionkv(int, int);
	int defaulthash(K, int);
	void copykv(KValue<K,V>*, KValue<K,V>);
	string encodekv(KValue<K,V>);
	void decodekv(KValue<K,V> *, string);
	
	void receivekv(int);
	void sortfiles();
	string getkvstr(ifstream& filep);
	int getnkv(ifstream& filep);
	
	string getkvDir()
	{
		return kvDir;
	}
	int getrank()
	{
		return me;
	}
};

//Implementation part

string itos(int num);
vector<string> split(string s, char delim);


template <class K, class V>
string getfilename(KeyValue<K,V> *,int);

template <class K, class V>
string getfilename(KeyValue<K,V> *obj,int rank)
{
	string filename;
	int curtime = time(NULL);
	filename = obj->getkvDir() + "kv_" + itos(obj->getrank())+"_"+itos(rank) + "." + itos(obj->counter++);
	//cout<<"Rank:"<<obj->getrank()<<"\t"<<obj->getkvDir()<<endl;
	return filename;
}

template <class K, class V>
string getfilename(KeyValue<K,V> *,int,int);

template <class K, class V>
string getfilename(KeyValue<K,V> *obj,int rank,int rank2)
{
	string filename;
	filename = obj->getkvDir() + "kv_" +itos(rank) + "." + itos(obj->counter++);
	//cout<<"Rank:"<<obj->getrank()<<"\t"<<obj->getkvDir()<<endl;
	return filename;
}

template <class K, class V>
string KeyValue<K,V>::getkvstr(ifstream& filep)
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
            if (buffer==NULL)
            {
                logobj.error("Failed to allocate "+itos(bsize)+" bytes for buffer to get keyvalue string from file");
            }
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

template <class K, class V>
int KeyValue<K,V>::getnkv(ifstream& filep)
{
	string str;
	char c;
	int numkv;
	while(filep.good())
	{
		c=filep.get();
		if(c=='\n'){
			numkv = atoi(str.c_str());
			return numkv;
		}
		str+=string(1,c);
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
    counter=0;
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
	KValue<K,V>  newkv;
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
void KeyValue<K,V>::printkv(KValue<K,V> tempkv)
{
	cout<<"Key: "<<tempkv.key<<"\tValue: "<<tempkv.value<<endl;
}

template <class K,class V>
void KeyValue<K,V>::printkv(deque<KValue<K,V> > tempkv)
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
inline bool KeyValue<char,char>::compkv(const KValue<char,char> & k1, const KValue<char,char> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<char,int>::compkv(const KValue<char,int> & k1, const KValue<char,int> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<char,float>::compkv(const KValue<char,float> & k1, const KValue<char,float> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<char,double>::compkv(const KValue<char,double> &k1, const KValue<char,double> &k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<char,string>::compkv(const KValue<char,string> & k1, const KValue<char,string> & k2)
{
	if(k1.key==k2.key)
		return lexicographical_compare(k1.value.begin(),k1.value.end(),k2.value.begin(),k2.value.end());
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<int,char>::compkv(const KValue<int,char> & k1, const KValue<int,char> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<int,int>::compkv(const KValue<int,int> & k1, const KValue<int,int> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<int,float>::compkv(const KValue<int,float> & k1, const KValue<int,float> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<int,double>::compkv(const KValue<int,double> & k1, const KValue<int,double> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<int,string>::compkv(const KValue<int,string> & k1, const KValue<int,string> & k2)
{
	if(k1.key==k2.key)
		return lexicographical_compare(k1.value.begin(),k1.value.end(),k2.value.begin(),k2.value.end());
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<float,char>::compkv(const KValue<float,char> & k1, const KValue<float,char> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<float,int>::compkv(const KValue<float,int> & k1, const KValue<float,int> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<float,float>::compkv(const KValue<float,float> & k1, const KValue<float,float> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<float,double>::compkv(const KValue<float,double> & k1, const KValue<float,double> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<float,string>::compkv(const KValue<float,string> & k1, const KValue<float,string> & k2)
{
	if(k1.key==k2.key)
		return lexicographical_compare(k1.value.begin(),k1.value.end(),k2.value.begin(),k2.value.end());
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<double,char>::compkv(const KValue<double,char> & k1, const KValue<double,char> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<double,int>::compkv(const KValue<double,int> & k1, const KValue<double,int> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<double,float>::compkv(const KValue<double,float> & k1, const KValue<double,float> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<double,double>::compkv(const KValue<double,double> & k1, const KValue<double,double> & k2)
{
	if(k1.key==k2.key)
		return (k1.value<k2.value);
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<double,string>::compkv(const KValue<double,string> & k1, const KValue<double,string> & k2)
{
	if(k1.key==k2.key)
		return lexicographical_compare(k1.value.begin(),k1.value.end(),k2.value.begin(),k2.value.end());
	else
		return (k1.key<k2.key);
}

template <>
inline bool KeyValue<string,char>::compkv(const KValue<string,char> & k1, const KValue<string,char> & k2)
{
	if (lexicographical_compare(k1.key.begin(),k1.key.end(),k2.key.begin(),k2.key.end()))
		return true;
	else if(lexicographical_compare(k2.key.begin(),k2.key.end(),k1.key.begin(),k1.key.end()))
		return false;
	else
		return (k1.value<k2.value);
}

template <>
inline bool KeyValue<string,int>::compkv(const KValue<string,int> & k1, const KValue<string,int> & k2)
{
	if (lexicographical_compare(k1.key.begin(),k1.key.end(),k2.key.begin(),k2.key.end()))
		return true;
	else if(lexicographical_compare(k2.key.begin(),k2.key.end(),k1.key.begin(),k1.key.end()))
		return false;
	else
		return (k1.value<k2.value);
}

template <>
inline bool KeyValue<string,float>::compkv(const KValue<string,float> & k1, const KValue<string,float> & k2)
{
	if (lexicographical_compare(k1.key.begin(),k1.key.end(),k2.key.begin(),k2.key.end()))
		return true;
	else if(lexicographical_compare(k2.key.begin(),k2.key.end(),k1.key.begin(),k1.key.end()))
		return false;
	else
		return (k1.value<k2.value);
}

template <>
inline bool KeyValue<string,double>::compkv(const KValue<string,double> & k1, const KValue<string,double> & k2)
{
	if (lexicographical_compare(k1.key.begin(),k1.key.end(),k2.key.begin(),k2.key.end()))
		return true;
	else if(lexicographical_compare(k2.key.begin(),k2.key.end(),k1.key.begin(),k1.key.end()))
		return false;
	else
		return (k1.value<k2.value);
}

template <>
inline bool KeyValue<string,string>::compkv(const KValue<string,string> & k1, const KValue<string,string> & k2)
{
	if (lexicographical_compare(k1.key.begin(),k1.key.end(),k2.key.begin(),k2.key.end()))
		return true;
	else if(lexicographical_compare(k2.key.begin(),k2.key.end(),k1.key.begin(),k1.key.end()))
		return false;
	else
		return lexicographical_compare(k1.value.begin(),k1.value.end(),k2.value.begin(),k2.value.end());
}

template <class	K, class V>
void KeyValue<K,V>::copykv(KValue<K,V>  *k1, KValue<K,V>  k2)
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
	KValue<K,V>  *kvalue= new KValue<K,V> ;
	vector<deque<KValue<K,V> >> tempkv;
	vector<int> tempnkv(nump,0);
	int i,hvalue,j;
	string str,str2;
	int len;
	tempkv.resize(nump);
    MPI_Request request;
    int rvalue;
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
	logobj.localLog("KeyValue pair partitioned");
	/*for(i=0;i<nump;i++)
	{
		cout<<"Proc "<<i<<endl;
		printkv(tempkv[i]);
	}*/
    string log;
	for(i=0;i<nump;i++)
	{
        if(tempnkv[i] == 0)
            continue;
        log = "Sending signal to START keyvalue transfer to process:"+itos(i);
        logobj.localLog(log);
        log.clear();
		rvalue=MPI_Send(&tempnkv[i],1,MPI_INT,i,NUM_KEY,comm);
		if (rvalue!=0)
            logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
        str.clear();
		for(j=0;j<tempnkv[i];j++)
		{
			str2=encodekv(tempkv[i].front());
			tempkv[i].pop_front();
			if (str.length()+str2.length() > STR_MAX)
			{
				char *buffer = strdup(str.c_str());
				len = str.length();
				rvalue=MPI_Send(&len,1,MPI_INT,i,SIZE_NXTMSG,comm);
                if (rvalue!=0)
                    logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
				log = "Sending keyvalue pair to process:"+itos(i);
                logobj.localLog(log);
                log.clear();
                rvalue=MPI_Send(buffer,str.length(),MPI_CHAR,i,KEYVALUE,comm);
                if (rvalue!=0)
                    logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
				str.clear();
			}
			str+=str2;
		}
		if(!str.empty())
		{
			len = str.length();
			rvalue=MPI_Send(&len,1,MPI_INT,i,SIZE_NXTMSG,comm);
            if (rvalue!=0)
                logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
			log = "Sending keyvalue pair to process:"+itos(i);
            logobj.localLog(log);
            log.clear();
            rvalue=MPI_Send(strdup(str.c_str()),str.length(),MPI_CHAR,i,KEYVALUE,comm);
            if (rvalue!=0)
                logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
		}
		len = 0;
        log = "Sending signal for END of maptask to process:"+itos(i);
        logobj.localLog(log);
        log.clear();
		rvalue=MPI_Send(NULL,0,MPI_INT,i,END_MAP_TASK,comm);
        if (rvalue!=0)
            logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
	}
}

template <class K, class V>
void KeyValue<K,V>::partitionkv(int nump, int numkey)
{
	logobj.localLog("Entered partitionkv");
    //usleep(1000000);
	KValue<K,V>  *kvalue= new KValue<K,V> ;
	vector<deque<KValue<K,V> >> tempkv;
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
	logobj.localLog("KeyValue pair partitioned");
	/*for(i=0;i<nump;i++)
	{
		cout<<"Proc "<<i<<endl;
		printkv(tempkv[i]);
	}*/
    string log;
	for(i=0;i<nump;i++)
	{
        if(tempnkv[i] == 0)
            continue;
        log = "Sending signal to START keyvalue transfer to process:"+itos(i);
        logobj.localLog(log);
        log.clear();
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
                log = "Sending keyvalue pair to process:"+itos(i);
                logobj.localLog(log);
                log.clear();
				MPI_Send(buffer,str.length(),MPI_CHAR,i,KEYVALUE,comm);
				str.clear();
			}
			str+=str2;
		}
		if(!str.empty())
		{
			len = str.length();
			MPI_Send(&len,1,MPI_INT,i,SIZE_NXTMSG,comm);
            log = "Sending keyvalue pair to process:"+itos(i);
            logobj.localLog(log);
            log.clear();
			MPI_Send(strdup(str.c_str()),str.length(),MPI_CHAR,i,KEYVALUE,comm);
		}
		len = 0;
		MPI_Send(NULL,0,MPI_INT,i,END_MAP_TASK,comm);
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
inline string KeyValue<int,int>::encodekv(KValue<int,int>  k)
{
	string str = itos(k.key) + "#" + itos(k.ksize) + "#" + itos(k.value) + "#" + itos(k.vsize) + "\n";
	str = itos(str.length())+":"+str;
	return str;
}

template <>
inline void KeyValue<int,int>::decodekv(KValue<int,int>  *k1, string str)
{
	//logobj.localLog("\t\tstring to be decoded is :::"+str);
    //cout<<str<<endl;
	vector<string> vstr = split(str,'#');
	k1->key = atoi(vstr[0].c_str());
	k1->ksize = atoi(vstr[1].c_str());
	k1->value = atoi(vstr[2].c_str());
	k1->vsize = atoi(vstr[3].substr(0,vstr[3].length()-1).c_str());
    vstr.clear();
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
	int source,flag;
	int nkv,rvalue;
	char *buffer;
	string msg,strnkv;
	if(nump < 1){
		string err = "Error: Total number of map process is less than 1. Exiting!!";
		logobj.error(err);
	}
	logobj.localLog("Variables defined in receivekv");
	while(1)
	{
		rvalue = MPI_Iprobe(MPI_ANY_SOURCE,MPI_ANY_TAG,comm,&flag,&status);
		if (rvalue!=0)
			logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
		source = status.MPI_SOURCE;
		if (!flag){
			usleep(1000);
			continue;
		}
		logobj.localLog("Some message received");
		switch(status.MPI_TAG)
		{
			case NUM_KEY:
				if(!procfile[source].empty()){
					string err = "Message for end of previous maptask of process " + itos(source)+" is not completed.";
					err+="Exiting!!";
					logobj.error(err);
				}
				msg = "Received start of keyvalue pair transfer for reducer from rank:" + itos(source);
				logobj.localLog(msg);
				msg.clear();
				procfile[source] = getfilename(this, source);
				//procfp[source].open(procfile[source].c_str(), ios::out | ios::app | ios::binary);
				rvalue = MPI_Recv(&nkv,1,MPI_INT,source,NUM_KEY,comm,&status);
				if (rvalue!=0)
					logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
				procfp.open(procfile[source].c_str(), ios::out | ios::app | ios::binary);
				strnkv = ""+itos(nkv)+"\n";
				procfp.write(strdup(strnkv.c_str()),strnkv.length());
				procfp.close();
				break;
			
			case SIZE_NXTMSG:
				rvalue = MPI_Recv(&proclen[source],1,MPI_INT,source,SIZE_NXTMSG,comm,&status);
                if (rvalue!=0)
                    logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
                msg = "Received SIZE of keyvalue pair from rank:"+itos(source);
                logobj.localLog(msg);
                msg.clear();
				break;
				
			case KEYVALUE:
				if(proclen[source] == -1){
					string err;
					err = "Receiving data from rank:"+itos(source)+" Could not receive size of data. Exiting!!";
					logobj.error(err);
				}
				msg = "Received keyvalue pair from rank:"+itos(source);
				logobj.localLog(msg);
				msg.clear();
				buffer = (char*)malloc(proclen[source]);
				if (buffer==NULL)
				{
				logobj.error("Failed to allocate "+itos(proclen[source])+" bytes for buffer to get keyvalue string from file");
				}
						rvalue = MPI_Recv(buffer,proclen[source],MPI_CHAR,source,KEYVALUE,comm,&status);
				if (rvalue!=0)
					logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
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
				rvalue=MPI_Recv(NULL,0,MPI_INT,source,END_MAP_TASK,comm,&status);
				if (rvalue!=0)
					logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
				//procfp[source].close();
				msg = "Received End of map task message from rank:"+itos(source);
				logobj.localLog(msg);
				msg.clear();
				filename.push_back(procfile[source]);
				procfile[source].clear();
				break;
				
			case END_MAP:
				if(!procfile[source].empty()){
					string err = "Rank:"+itos(source)+" Some map task still not completed.";
					err+=" Received end of mapping part message. Exiting!!";
					logobj.error(err);
				}
				rvalue = MPI_Recv(NULL,0,MPI_INT,source,END_MAP,comm,&status);
				if (rvalue!=0)
					logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
				nump--;
				msg = "Received end of map phase from rank:"+itos(source);
				msg+="\nNumber of process left is "+itos(nump);
				logobj.localLog(msg);
				msg.clear();
				if(nump==0){
					recvcomp = 1;
					logobj.localLog("\trecvcomp is set to be 1\t number of files = "+itos((int)filename.size()));
					return;
				}
				break;
				
			default:
				string err;
				err = "Rank:" + itos(source)+ " Message of unknowntag received. Ignoring!!";
				logobj.warning(err);
				usleep(1000);
				break;
		}
	}
}

template <class K, class V>
void KeyValue<K,V>::sortfiles()
{
	logobj.localLog("\tEntered sortfiles part");
	string kvfilename[NUM_SFILE];
	//ifstream kvfilep[NUM_SFILE];
	ifstream kvfilep;
	int kvpos[NUM_SFILE];
	int nkv[NUM_SFILE],tnkv;
	string newfile;
	ofstream newfilep;
	vector<KValue<K,V> > tempkv;
	vector<int> index;
	int i,minpos,numfiles;
	string buffer;
	KValue<K,V>  temp;
    
	logobj.localLog("\tVariables defined for sortfile part");
	while(1){
		if((filename.size() < NUM_SFILE) && (recvcomp == 0)){
			usleep(1000);
			continue;
		}
		numfiles = min((int)filename.size(),NUM_SFILE);
		logobj.localLog("\tStarted file sorting. recvcomp="+itos(recvcomp)+" and numfiles="+itos(numfiles));
		//tempkv.resize(numfiles);
		//index.resize(numfiles);
		for(i=0;i<numfiles;i++){
			kvfilename[i] = filename.front();
			filename.pop_front();
			//kvfilep[i].open(kvfilename[i].c_str());
		}
		logobj.localLog("\tRead all filesname for sorting");
		newfile = getfilename(this,me,me);
		tnkv=0;
		for(i=0;i<numfiles;i++){
			kvfilep.open(kvfilename[i].c_str());
			nkv[i] = getnkv(kvfilep);
			tnkv+=nkv[i];
			buffer = getkvstr(kvfilep);
			logobj.localLog("String to be decoded :::"+buffer+" from file::"+kvfilename[i]);
			decodekv(&temp,buffer);
			tempkv.push_back(temp);
			index.push_back(i);
			kvpos[i] = (int)kvfilep.tellg();
			kvfilep.close();
			nkv[i]--;
		}
		logobj.localLog("\tRead atleast 1 keyvalue from each file");
		buffer.clear();
		buffer = ""+itos(tnkv)+"\n";
		newfilep.open(newfile.c_str(), ios::out | ios::app | ios::binary);
		newfilep.write(strdup(buffer.c_str()),buffer.length());
		newfilep.close();
		while(1){
			minpos = distance(tempkv.begin(),min_element(tempkv.begin(),tempkv.end(),compkv));
			//logobj.localLog("Minpos : "+itos(minpos));
			buffer.clear();
			//printkv(tempkv.at(minpos));
			buffer = encodekv(tempkv.at(minpos));
			newfilep.open(newfile.c_str(), ios::out | ios::app | ios::binary);
			newfilep.write(strdup(buffer.c_str()),buffer.length());
			newfilep.close();
			buffer.clear();
			if(nkv[index[minpos]]!=0){
				//cout<<minpos<<"\t"<<index[minpos]<<endl;
				kvfilep.open(kvfilename[index[minpos]].c_str());
				kvfilep.seekg(kvpos[index[minpos]],ios::beg);
				buffer = getkvstr(kvfilep);
				logobj.localLog("String to be decoded :::"+buffer+" from file::"+kvfilename[index[minpos]]);
				decodekv(&temp,buffer);
				copykv(&tempkv[minpos],temp);
				kvpos[index[minpos]] = (int)kvfilep.tellg();
				kvfilep.close();
				nkv[index[minpos]]--;
			}
			else{
				tempkv.erase(tempkv.begin() + minpos);
				remove(kvfilename[index[minpos]].c_str());
				logobj.localLog("Index -> size:"+itos(index.size()));
				logobj.localLog("Index -> minpos:"+itos(minpos));
				index.erase(index.begin() + minpos);
				//logobj.localLog("Read all keyvalue pair from "+kvfilename[index[minpos]]);
				if (tempkv.size() == 0){
					newfilep.close();
					filename.push_front(newfile);
					break;
				}
			}
		}
		logobj.localLog("\tEnd of sorting file loop");
		if ((recvcomp == 1) && (filename.size() == 1)){
			kvfile = filename.front();
			filename.pop_front();
			logobj.localLog("\tAll files sorted in a single file");
			return;
		}
	}
}
#endif