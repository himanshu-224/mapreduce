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
void mfunc(vector<primaryKV> &kv, MapReduce<string,int> *mr);
void rfunc(MapReduce<string,int> *mr);
int main(int argc, char **argv)
{


    int i = 0;
    char hostname[256];
    gethostname(hostname, sizeof(hostname));
    printf("PID %d on %s ready for attach\n", getpid(), hostname);
    fflush(stdout);

    MapReduce<string,int> mr= MapReduce<string,int>(argc,argv,2);
    mr.map(argc,argv,mfunc);
    mr.reduce(rfunc);

return 0;
}

void mfunc(vector<primaryKV> &kv, MapReduce<string,int> *mr)
{

for(int j=0;j<kv.size();j++)
{
	//cout<<kv[j].value<<endl;
	istringstream iss(kv[j].value);
	vector<string> tokens;
	copy(istream_iterator<string>(iss),istream_iterator<string>(), back_inserter<vector<string> >(tokens));
	
	int numkeys=tokens.size();
	for(int i=0;i<numkeys;i++)
		mr->addkv(tokens[i],1);
	
	
}
}

void rfunc(MapReduce<string,int> *mr)
{
    while(!mr->empty())
    {
        KMultiValue<string,int> kmv = mr->getKey();
        int length=kmv.length;
        int sum=0;
        for(int i=0;i<length;i++)
        {
            sum+=kmv.mv[i].value;
        }
        mr->raddkv(kmv.key,sum);
    }
    
}

