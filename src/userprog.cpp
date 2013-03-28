#include<iostream>
#include "mapReduce.h"

void func(primaryKV&, int &n);
int main(int argc, char **argv)
{

MapReduce mr= MapReduce(argc,argv);
mr.map(argc,argv,func);
return 0;

}

void func(primaryKV &kv, int &n)
{
    
}