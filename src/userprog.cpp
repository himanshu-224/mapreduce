#include<iostream>
#include "mapReduce.h"
#include "keyValue.h"
#include <time.h>
#include <stdlib.h>
#include <iostream>
#include <string>
#include <sstream>
#include <algorithm>
#include <iterator>

using namespace std;
void mfunc(vector<primaryKV> &kv, MapReduce<int,int> *mr);
void rfunc(MapReduce<int,int> *mr);
int main(int argc, char **argv)
{

    int i = 0;
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    printf("PID %d on %s ready for attach\n", getpid(), hostname);
    fflush(stdout);

    MapReduce<int,int> mr= MapReduce<int,int>(argc,argv,2);
    mr.map(argc,argv,mfunc);
    mr.reduce(rfunc);

return 0;
}

void mfunc(vector<primaryKV> &kv, MapReduce<int,int> *mr)
{
    int temp,v=1024*256;
    srand(time(NULL));
    for(int i=1;i<=100000;i++)
    {
        srand(time(NULL));
        temp = rand()%v+i;
        mr->addkv(temp,1);
    }
}

void rfunc(MapReduce<int,int> *mr)
{
    while(!mr->empty())
    {
        KMultiValue<int,int> kmv = mr->getKey();
        int length=kmv.length;
        int sum=0;
        for(int i=0;i<length;i++)
        {
            mr->raddkv(kmv.key,kmv.mv[i].value);
        }
        
    }   
    
}
