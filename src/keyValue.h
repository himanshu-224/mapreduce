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
#define STR_MAX 5120
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
	deque<KValue<K,V> > kv;
	int nkv;

	deque<string> filename;
	int recvcomp;
	
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

//### Helper Function declaration starts here
string itos(int num);
string itos(float num);
string itos(double num);
vector<string> split(string s, char delim);

template <class K, class V>
string getfilename(KeyValue<K,V> *,int);

template <class K, class V>
string getfilename(KeyValue<K,V> *,int,int);

template <class K>
int getsize(K var);

//### Helper Function defination starts here

string itos(float num)
{
    return static_cast<ostringstream*>( &(ostringstream() << num) )->str();
}
string itos(double num)
{
    return static_cast<ostringstream*>( &(ostringstream() << num) )->str();
}

template <class K, class V>
string getfilename(KeyValue<K,V> *obj,int rank)
{
	string filename;
	int curtime = time(NULL);
	filename = obj->getkvDir() + "kv_" + itos(obj->getrank())+"_"+itos(rank) + "." + itos(obj->counter++);
	return filename;
}

template <class K, class V>
string getfilename(KeyValue<K,V> *obj,int rank,int rank2)
{
	string filename;
	filename = obj->getkvDir() + "kv_" +itos(rank) + "." + itos(obj->counter++);
	return filename;
}

template <>
int getsize<char>(char var)
{
    return (int)sizeof(char);
}

template <>
int getsize<int>(int var)
{
    return (int)sizeof(int);
}

template <>
int getsize<float>(float var)
{
    return (int)sizeof(float);
}

template <>
int getsize<double>(double var)
{
    return (int)sizeof(double);
}

template <>
int getsize<string>(string var)
{
    return (int)var.length();
}

//### Helper function defination ends here

//### class KeyValue member function defination starts here

//Template specialisation defination for compkv
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

//Encode the KeyValue pair into human readable text
template <>
inline string KeyValue<char,char>::encodekv(KValue<char,char> k)
{
	string str = itos(k.ksize) + "#" + string(1,k.key) + "#" + itos(k.vsize) + "#" + string(1,k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<char,int>::encodekv(KValue<char,int> k)
{
	string str = itos(k.ksize) + "#" + string(1,k.key) + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<char,float>::encodekv(KValue<char,float> k)
{
	string str = itos(k.ksize) + "#" + string(1,k.key) + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<char,double>::encodekv(KValue<char,double> k)
{
	string str = itos(k.ksize) + "#" + string(1,k.key) + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<char,string>::encodekv(KValue<char,string> k)
{
	string str = itos(k.ksize) + "#" + string(1,k.key) + "#" + itos(k.vsize) + "#" + k.value + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<int,char>::encodekv(KValue<int,char> k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + string(1,k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<int,int>::encodekv(KValue<int,int>  k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<int,float>::encodekv(KValue<int,float>  k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<int,double>::encodekv(KValue<int,double>  k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<int,string>::encodekv(KValue<int,string>  k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + k.value + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<float,char>::encodekv(KValue<float,char> k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + string(1,k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<float,int>::encodekv(KValue<float,int>  k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<float,float>::encodekv(KValue<float,float>  k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<float,double>::encodekv(KValue<float,double>  k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<float,string>::encodekv(KValue<float,string>  k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + k.value + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<double,char>::encodekv(KValue<double,char> k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + string(1,k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<double,int>::encodekv(KValue<double,int>  k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<double,float>::encodekv(KValue<double,float>  k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<double,double>::encodekv(KValue<double,double>  k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<double,string>::encodekv(KValue<double,string>  k)
{
	string str = itos(k.ksize) + "#" + itos(k.key) + "#" + itos(k.vsize) + "#" + k.value + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<string,char>::encodekv(KValue<string,char> k)
{
	string str = itos(k.ksize) + "#" + k.key + "#" + itos(k.vsize) + "#" + string(1,k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<string,int>::encodekv(KValue<string,int>  k)
{
	string str = itos(k.ksize) + "#" + k.key + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<string,float>::encodekv(KValue<string,float>  k)
{
	string str = itos(k.ksize) + "#" + k.key + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<string,double>::encodekv(KValue<string,double>  k)
{
	string str = itos(k.ksize) + "#" + k.key + "#" + itos(k.vsize) + "#" + itos(k.value) + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}
template <>
inline string KeyValue<string,string>::encodekv(KValue<string,string>  k)
{
	string str = itos(k.ksize) + "#" + k.key + "#" + itos(k.vsize) + "#" + k.value + "\n";
	str = itos((int)str.length())+":"+str;
	return str;
}

template <>
inline void KeyValue<char,char>::decodekv(KValue<char,char>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(char))||(k1->vsize!=sizeof(char)))
		logobj.error("Incompatible key or value size for char key and char value.Exiting!!");
	k1->key = (char)vstr[1][0];
	k1->value = (char)vstr[3][0];
	vstr.clear();
}
template <>
inline void KeyValue<char,int>::decodekv(KValue<char,int>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(char))||(k1->vsize!=sizeof(int)))
		logobj.error("Incompatible key or value size for char key and int value.Exiting!!");
	k1->key = (char)vstr[1][0];
	k1->value = atoi(vstr[3].substr(0,vstr[3].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<char,float>::decodekv(KValue<char,float>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(char))||(k1->vsize!=sizeof(float)))
		logobj.error("Incompatible key or value size for char key and float value.Exiting!!");
	k1->key = (char)vstr[1][0];
	k1->value = (float)atof(vstr[3].substr(0,vstr[3].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<char,double>::decodekv(KValue<char,double>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(char))||(k1->vsize!=sizeof(double)))
		logobj.error("Incompatible key or value size for char key and double value.Exiting!!");
	k1->key = (char)vstr[1][0];
	k1->value = atof(vstr[3].substr(0,vstr[3].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<char,string>::decodekv(KValue<char,string>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if(k1->ksize!= sizeof(char))
		logobj.error("Incompatible key or value size for char key and string value.Exiting!!");
	k1->key = (char)vstr[1][0];
	int i=3;
	int size = k1->vsize;
	k1->value.clear();
	while(1){
		if(vstr[i].length() > size){
			k1->value+=vstr[i].substr(0,vstr[i].length()-1);
			break;
		}
		k1->value+=vstr[i]+"#";
		size=size-vstr[i].length()-1;
		i++;
	}
	if(k1->vsize!=k1->value.length())
		logobj.error("Incompatible key or value size for char key and string value.Exiting!!");
	vstr.clear();
}
template <>
inline void KeyValue<int,char>::decodekv(KValue<int,char>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(int))||(k1->vsize!=sizeof(char)))
		logobj.error("Incompatible key or value size for int key and char value.Exiting!!");
	k1->key = atoi(vstr[1].c_str());
	k1->value = (char)vstr[3][0];
	vstr.clear();
}
template <>
inline void KeyValue<int,int>::decodekv(KValue<int,int>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(int))||(k1->vsize!=sizeof(int)))
		logobj.error("Incompatible key or value size for int key and int value.Exiting!!");
	k1->key = atoi(vstr[1].c_str());
	k1->value = atoi(vstr[3].substr(0,vstr[3].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<int,float>::decodekv(KValue<int,float>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(int))||(k1->vsize!=sizeof(float)))
		logobj.error("Incompatible key or value size for int key and float value.Exiting!!");
	k1->key = atoi(vstr[1].c_str());
	k1->value = (float)atof(vstr[3].substr(0,vstr[3].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<int,double>::decodekv(KValue<int,double>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(int))||(k1->vsize!=sizeof(double)))
		logobj.error("Incompatible key or value size for int key and double value.Exiting!!");
	k1->key = atoi(vstr[1].c_str());
	k1->value = atof(vstr[3].substr(0,vstr[3].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<int,string>::decodekv(KValue<int,string>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if(k1->ksize!= sizeof(int))
		logobj.error("Incompatible key or value size for int key and string value.Exiting!!");
	k1->key = atoi(vstr[1].c_str());
	int i=3;
	int size = k1->vsize;
	k1->value.clear();
	while(1){
		if(vstr[i].length() > size){
			k1->value+=vstr[i].substr(0,vstr[i].length()-1);
			break;
		}
		k1->value+=vstr[i]+"#";
		size=size-vstr[i].length()-1;
		i++;
	}
	if(k1->vsize!=k1->value.length())
		logobj.error("Incompatible key or value size for int key and string value.Exiting!!");
	vstr.clear();
}
template <>
inline void KeyValue<float,char>::decodekv(KValue<float,char>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(float))||(k1->vsize!=sizeof(char)))
		logobj.error("Incompatible key or value size for float key and char value.Exiting!!");
	k1->key = (float)atof(vstr[1].c_str());
	k1->value = (char)vstr[3][0];
	vstr.clear();
}
template <>
inline void KeyValue<float,int>::decodekv(KValue<float,int>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(float))||(k1->vsize!=sizeof(int)))
		logobj.error("Incompatible key or value size for float key and int value.Exiting!!");
	k1->key = (float)atof(vstr[1].c_str());
	k1->value = atoi(vstr[3].substr(0,vstr[3].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<float,float>::decodekv(KValue<float,float>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(float))||(k1->vsize!=sizeof(float)))
		logobj.error("Incompatible key or value size for float key and float value.Exiting!!");
	k1->key = (float)atof(vstr[1].c_str());
	k1->value = (float)atof(vstr[3].substr(0,vstr[3].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<float,double>::decodekv(KValue<float,double>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(float))||(k1->vsize!=sizeof(double)))
		logobj.error("Incompatible key or value size for float key and double value.Exiting!!");
	k1->key = (float)atof(vstr[1].c_str());
	k1->value = atof(vstr[3].substr(0,vstr[3].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<float,string>::decodekv(KValue<float,string>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if(k1->ksize!= sizeof(float))
		logobj.error("Incompatible key or value size for float key and string value.Exiting!!");
	k1->key = (float)atof(vstr[1].c_str());
	int i=3;
	int size = k1->vsize;
	k1->value.clear();
	while(1){
		if(vstr[i].length() > size){
			k1->value+=vstr[i].substr(0,vstr[i].length()-1);
			break;
		}
		k1->value+=vstr[i]+"#";
		size=size-vstr[i].length()-1;
		i++;
	}
	if(k1->vsize!=k1->value.length())
		logobj.error("Incompatible key or value size for float key and string value.Exiting!!");
	vstr.clear();
}
template <>
inline void KeyValue<double,char>::decodekv(KValue<double,char>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(double))||(k1->vsize!=sizeof(char)))
		logobj.error("Incompatible key or value size for double key and char value.Exiting!!");
	k1->key = atof(vstr[1].c_str());
	k1->value = (char)vstr[3][0];
	vstr.clear();
}
template <>
inline void KeyValue<double,int>::decodekv(KValue<double,int>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(double))||(k1->vsize!=sizeof(int)))
		logobj.error("Incompatible key or value size for double key and int value.Exiting!!");
	k1->key = atof(vstr[1].c_str());
	k1->value = atoi(vstr[3].substr(0,vstr[3].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<double,float>::decodekv(KValue<double,float>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(double))||(k1->vsize!=sizeof(float)))
		logobj.error("Incompatible key or value size for double key and float value.Exiting!!");
	k1->key = atof(vstr[1].c_str());
	k1->value = (float)atof(vstr[3].substr(0,vstr[3].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<double,double>::decodekv(KValue<double,double>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if((k1->ksize!= sizeof(double))||(k1->vsize!=sizeof(double)))
		logobj.error("Incompatible key or value size for double key and double value.Exiting!!");
	k1->key = atof(vstr[1].c_str());
	k1->value = atof(vstr[3].substr(0,vstr[3].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<double,string>::decodekv(KValue<double,string>  *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	k1->vsize = atoi(vstr[2].c_str());
	if(k1->ksize!= sizeof(double))
		logobj.error("Incompatible key or value size for double key and string value.Exiting!!");
	k1->key = atof(vstr[1].c_str());
	int i=3;
	int size = k1->vsize;
	k1->value.clear();
	while(1){
		if(vstr[i].length() > size){
			k1->value+=vstr[i].substr(0,vstr[i].length()-1);
			break;
		}
		k1->value+=vstr[i]+"#";
		size=size-vstr[i].length()-1;
		i++;
	}
	if(k1->vsize!=k1->value.length())
		logobj.error("Incompatible key or value size for double key and string value.Exiting!!");
	vstr.clear();
}
template <>
inline void KeyValue<string,char>::decodekv(KValue<string,char> *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	int i =1;
	int size = k1->ksize;
	k1->key.clear();
	while(1){
		if(vstr[i].length() >= size){
			k1->key+=vstr[i];
			i++;
			break;
		}
		k1->key+=vstr[i]+"#";
		size=size-vstr[i].length()-1;
		i++;
	}
	if(k1->ksize!=k1->key.length())
		logobj.error("Incompatible key or value size for string key and char value.Exiting!!");
	k1->vsize = atoi(vstr[i].c_str());
	if(k1->vsize!=sizeof(char))
		logobj.error("Incompatible key or value size for string key and char value.Exiting!!");
	i++;
	k1->value = (char)vstr[i][0];
	vstr.clear();
}
template <>
inline void KeyValue<string,int>::decodekv(KValue<string,int> *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	int i =1;
	int size = k1->ksize;
	k1->key.clear();
	while(1){
		if(vstr[i].length() >= size){
			k1->key+=vstr[i];
			i++;
			break;
		}
		k1->key+=vstr[i]+"#";
		size=size-vstr[i].length()-1;
		i++;
	}
	if(k1->ksize!=k1->key.length())
		logobj.error("Incompatible key or value size for string key and int value.Exiting!!");
	k1->vsize = atoi(vstr[i].c_str());
	if(k1->vsize!=sizeof(int))
		logobj.error("Incompatible key or value size for string key and int value.Exiting!!");
	i++;
	k1->value = atoi(vstr[i].substr(0,vstr[i].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<string,float>::decodekv(KValue<string,float> *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	int i =1;
	int size = k1->ksize;
	k1->key.clear();
	while(1){
		if(vstr[i].length() >= size){
			k1->key+=vstr[i];
			i++;
			break;
		}
		k1->key+=vstr[i]+"#";
		size=size-vstr[i].length()-1;
		i++;
	}
	if(k1->ksize!=k1->key.length())
		logobj.error("Incompatible key or value size for string key and float value.Exiting!!");
	k1->vsize = atoi(vstr[i].c_str());
	if(k1->vsize!=sizeof(float))
		logobj.error("Incompatible key or value size for string key and float value.Exiting!!");
	i++;
	k1->value = (float)atof(vstr[i].substr(0,vstr[i].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<string,double>::decodekv(KValue<string,double> *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	int i =1;
	int size = k1->ksize;
	k1->key.clear();
	while(1){
		if(vstr[i].length() >= size){
			k1->key+=vstr[i];
			i++;
			break;
		}
		k1->key+=vstr[i]+"#";
		size=size-vstr[i].length()-1;
		i++;
	}
	if(k1->ksize!=k1->key.length())
		logobj.error("Incompatible key or value size for string key and double value.Exiting!!");
	k1->vsize = atoi(vstr[i].c_str());
	if(k1->vsize!=sizeof(double))
		logobj.error("Incompatible key or value size for string key and double value.Exiting!!");
	i++;
	k1->value = atof(vstr[i].substr(0,vstr[i].length()-1).c_str());
	vstr.clear();
}
template <>
inline void KeyValue<string,string>::decodekv(KValue<string,string> *k1, string str)
{
	vector<string> vstr = split(str,'#');
	k1->ksize = atoi(vstr[0].c_str());
	int i =1;
	int size = k1->ksize;
	k1->key.clear();
	while(1){
		if(vstr[i].length() >= size){
			k1->key+=vstr[i];
			i++;
			break;
		}
		k1->key+=vstr[i]+"#";
		size=size-vstr[i].length()-1;
		i++;
	}
	if(k1->ksize!=k1->key.length())
		logobj.error("Incompatible key or value size for string key and string value.Exiting!!");
	k1->vsize = atoi(vstr[i].c_str());
	i++;
	size = k1->vsize;
	while(1){
		if(vstr[i].length() > size){
			k1->value+=vstr[i].substr(0,vstr[i].length()-1);
			break;
		}
		k1->value+=vstr[i]+"#";
		size=size-vstr[i].length()-1;
		i++;
	}
	if(k1->vsize!=k1->value.length())
		logobj.error("Incompatible key or value size for string key and string value.Exiting!!");
	vstr.clear();
}
//Function to get the encoded string for each keyvalue pair from a file
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

//Function to get the number of keyvalue pair present in the file. Need to be called first before any getkvstr() call
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

//Constructor defined
template <class K, class V>
KeyValue<K,V>::KeyValue(MPI_Comm communicator, Logging &log_caller, string kvDir_caller)
{
	comm = communicator;
	logobj = log_caller;
	MPI_Comm_rank(comm,&me);
	kvDir = kvDir_caller;
    counter=0;
	nkv = 0;
	recvcomp=0;
}

template <class K, class V>
KeyValue<K,V>::~KeyValue()
{
}

// Add a single key value pair 
template <class K, class V>
void KeyValue<K,V>::add(K key, V value)
{
	KValue<K,V>  newkv;
	newkv.key = key;
	newkv.value = value;
	newkv.ksize = getsize(key);
	newkv.vsize = getsize(value);
	long tsize = newkv.ksize+newkv.vsize;
	if(tsize > INT_MAX)					// A limit on size of key value pair
		logobj.error("Single Key Value pair size exceeds int size");
	kv.push_back(newkv);
	nkv++;
}

//Function to print the keyvalue pair
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
	return numkey;
}

//copy the keyvalue pair from one structure to another
template <class	K, class V>
void KeyValue<K,V>::copykv(KValue<K,V>  *k1, KValue<K,V>  k2)
{
	k1->key = k2.key;
	k1->value = k2.value;
	k1->ksize = k2.ksize;
	k1->vsize = k2.vsize;
	
}

// Function to partition the keyvalue pair according to reducer to be assigned and send the keyvalue pair
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
	
    string log;
	int flag;
	MPI_Status status;
	for(i=0;i<nump;i++)
	{
        if(tempnkv[i] == 0)
            continue;
        log = "Sending signal to START keyvalue transfer to process:"+itos(i);
        logobj.localLog(log);
        log.clear();
		rvalue=MPI_Issend(&tempnkv[i],1,MPI_INT,i,NUM_KEY,comm,&request);
		while(1)
		{
			MPI_Test(&request,&flag,&status);	
			if (!flag)
			{
				usleep(100);
			}
			else
				break;
		}
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
				log = "Sending keyvalue pair to process:"+itos(i);
                logobj.localLog(log);
                log.clear();
                rvalue=MPI_Issend(buffer,len,MPI_CHAR,i,KEYVALUE,comm,&request);
				while(1)
				{
					MPI_Test(&request,&flag,&status);	
					if (!flag)
					{
						usleep(100);
					}
					else
					break;
				}
                if (rvalue!=0)
                    logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
				str.clear();
			}
			str+=str2;
		}
		if(!str.empty())
		{
			len = str.length();
			log = "Sending keyvalue pair to process:"+itos(i);
            logobj.localLog(log);
            log.clear();
			char *buffer = strdup(str.c_str());
            rvalue=MPI_Issend(buffer,len,MPI_CHAR,i,KEYVALUE,comm,&request);
			while(1)
			{
				MPI_Test(&request,&flag,&status);	
				if (!flag)
				{
					usleep(100);
				}
				else
				break;
			}
            if (rvalue!=0)
                logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
		}
		len = 0;
        log = "Sending signal for END of maptask to process:"+itos(i);
        logobj.localLog(log);
        log.clear();
		rvalue=MPI_Issend(NULL,0,MPI_INT,i,END_MAP_TASK,comm,&request);
		while(1)
		{
			MPI_Test(&request,&flag,&status);	
			if (!flag)
			{
				usleep(100);
			}
			else
			break;
		}
        if (rvalue!=0)
            logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
	}
}


template <class K, class V>
int KeyValue<K,V>::defaulthash(K key, int nump)
{
	hash<K> hash_fn;
	size_t v = hash_fn(key)%nump + 1;
	return (int)v;
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
	//vector<int> proclen(nump,-1);
	//procfp.resize(nump);
	MPI_Status status;
	int source,flag;
	int nkv,rvalue,bufsize;
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
			usleep(100);
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
				procfp.open(procfile[source].c_str(), ios::out | ios::binary);
				strnkv = ""+itos(nkv)+"\n";
				procfp.write(strdup(strnkv.c_str()),strnkv.length());
				procfp.close();
				break;
			
			/*case SIZE_NXTMSG:
				rvalue = MPI_Recv(&proclen[source],1,MPI_INT,source,SIZE_NXTMSG,comm,&status);
                if (rvalue!=0)
                    logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
                msg = "Received SIZE of keyvalue pair from rank:"+itos(source);
                logobj.localLog(msg);
                msg.clear();
				break;*/
				
			case KEYVALUE:
				/*if(proclen[source] == -1){
					string err;
					err = "Receiving data from rank:"+itos(source)+" Could not receive size of data. Exiting!!";
					logobj.error(err);
				}*/
				msg = "Received keyvalue pair from rank:"+itos(source);
				logobj.localLog(msg);
				msg.clear();
				buffer = (char*)malloc(STR_MAX);
				if (buffer==NULL)
				{
				logobj.error("Failed to allocate "+itos(STR_MAX)+" bytes for buffer to get keyvalue string from file");
				}
				rvalue = MPI_Recv(buffer,STR_MAX,MPI_CHAR,source,KEYVALUE,comm,&status);
				if (rvalue!=0)
					logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
				rvalue = MPI_Get_count(&status,MPI_CHAR,&bufsize);
				if (rvalue!=0)
					logobj.localLog("Error : "+string(strerror(errno)) +"["+itos(errno)+"]");
				procfp.open(procfile[source].c_str(), ios::out | ios::app | ios::binary);
				procfp.write(buffer,bufsize);
				procfp.close();
				delete [] buffer;
				//proclen[source] = -1;
				break;
				
			case END_MAP_TASK:
				/*if(proclen[source] != -1){
					string err;
					err = "Rank:"+itos(source)+" Could not receive some key value pair. Exiting!!";
					logobj.error(err);
				}*/
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
				usleep(100);
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
			usleep(100);
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
			//logobj.localLog("String to be decoded :::"+buffer+" from file::"+kvfilename[i]);
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
		newfilep.open(newfile.c_str(), ios::out | ios::binary);
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
				//logobj.localLog("String to be decoded :::"+buffer+" from file::"+kvfilename[index[minpos]]);
				decodekv(&temp,buffer);
				copykv(&tempkv[minpos],temp);
				kvpos[index[minpos]] = (int)kvfilep.tellg();
				kvfilep.close();
				nkv[index[minpos]]--;
			}
			else{
				tempkv.erase(tempkv.begin() + minpos);
				remove(kvfilename[index[minpos]].c_str());
				//logobj.localLog("Index -> size:"+itos(index.size()));
				//logobj.localLog("Index -> minpos:"+itos(minpos));
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
