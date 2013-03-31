#include<iostream>
#include "mapReduce.h"

void func(vector<primaryKV> &kv, int &n);

int main(int argc, char **argv)
{

MapReduce mr= MapReduce(argc,argv);
mr.map(argc,argv,func);
return 0;

}

void func(vector<primaryKV> &kv, int &n)
{
    
}