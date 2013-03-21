#include<iostream>
#include<stdio.h>
#include "dataStruc.h"

using namespace std;
class MapReduce
{
private:
	string ipListFile;
	string nodeInfoFile;
	string chunkMapFile;
	
	string dirFile;
	string mntDir;
	DataType dataType;
	int chunkSize;
	int numMaps;

//data provided as blocks of string to user suppiled map function
int map(void(*)(int, string, int*));    

void readDefaults(string configFile);
};
