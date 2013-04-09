#include<iostream>
#include "mapReduce.h"
#include "keyValue.h"
#include <time.h>
#include <stdlib.h>

void mfunc(vector<primaryKV> &kv, MapReduce<int,int> *mr);
void rfunc(MapReduce<int,int> *mr);
int main(int argc, char **argv)
{


    int i = 0;
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    printf("PID %d on %s ready for attach\n", getpid(), hostname);
    fflush(stdout);
    /*while (0 == i)
        sleep(5);

    cout<<"Attached to gdb and exiting from gdb wait\n";*/

MapReduce<int,int> mr= MapReduce<int,int>(argc,argv,2);
mr.map(argc,argv,mfunc);
mr.reduce(rfunc);
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
	int temp,v=1024*256;
	srand(time(NULL));
    for(int i=1;i<=1000;i++)
    {
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
        for(int i=0;i<length;i++)
        {
            mr->raddkv(kmv.key,kmv.mv[i].value);
        }
    }
    
}
