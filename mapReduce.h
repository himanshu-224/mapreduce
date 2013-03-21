#include<iostream>
#include<stdio.h>

using namespace std;
class MapReduce
{
public:
    static int num_instances;

//data provided as blocks of string to user suppiled map function
map(void(*)(int, string, KeyValue*));    