#include<iostream>
#include "mapReduce.h"
#include "keyValue.h"

void func(vector<primaryKV> &kv, int &n);

int main(int argc, char **argv)
{

MapReduce mr= MapReduce(argc,argv);
mr.map(argc,argv,func);
KeyValue<int,int> kv = KeyValue<int,int>();
kv.add(5,10);
kv.add(6,12);
kv.printkv();
return 0;
}

void func(vector<primaryKV> &kv, int &n)
{
    
}
