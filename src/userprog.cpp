#include<iostream>
#include "mapReduce.h"
#include "keyValue.h"

void func(vector<primaryKV> &kv, int &n);

int main(int argc, char **argv)
{

MapReduce<int,int> mr= MapReduce<int,int>(argc,argv);
mr.map(argc,argv,func);
KeyValue<int,int> kv = KeyValue<int,int>();
kv.add(5,10);
kv.add(123,12);
kv.add(351,15);
kv.add(83292,7);
//kv.printkv();
int t = kv.sortkv();
//kv.printkv();
kv.partitionkv(8,t);
return 0;
}

void func(vector<primaryKV> &kv, int &n)
{
    
}
