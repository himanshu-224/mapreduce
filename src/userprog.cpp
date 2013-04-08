#include<iostream>
#include "mapReduce.h"
#include "keyValue.h"

void mfunc(vector<primaryKV> &kv, MapReduce<int,int> *mr);

int main(int argc, char **argv)
{

MapReduce<int,int> mr= MapReduce<int,int>(argc,argv,2);
mr.map(argc,argv,mfunc);
//mr.reduce(rfunc);
/*KeyValue<int,int> kv = KeyValue<int,int>();
mr.addkv(5,10);
mr.addkv(123,12);
mr.addkv(351,15);
mr.addkv(83292,7);
kv.printkv();
mr.finalisemap();*/
return 0;
}

void mfunc(vector<primaryKV> &kv, MapReduce<int,int> *mr)
{
   /* for(int i=1;i<=10;i++)
    {
        mr->addkv(i,1);
    }*/
}

void rfunc(MapReduce<int,int> *mr)
{
    while(!mr->empty())
    {
        KMultiValue<int,int> kmv = mr->getKey();
        int length=kmv.length;
        for(int i=0;i<length;i++)
        {
            mr->raddkv(kmv.key,kmv.mv[i].value);
        }
    }
    
}
