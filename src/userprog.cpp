#include<iostream>
#include "mapReduce.h"
#include "keyValue.h"

void func(vector<primaryKV> &kv, MapReduce<int,int> *mr);

int main(int argc, char **argv)
{

MapReduce<int,int> mr= MapReduce<int,int>(argc,argv,8);
mr.map(argc,argv,func);
//KeyValue<int,int> kv = KeyValue<int,int>();
mr.addkv(5,10);
mr.addkv(123,12);
mr.addkv(351,15);
mr.addkv(83292,7);
//kv.printkv();
mr.finalisemap();
return 0;
}

void func(vector<primaryKV> &kv, MapReduce<int,int> *mr)
{
    //mr->addkv(5,10);
    //mr->addkv(213112,2121);
    //mr->addkv(13214,123);
}
