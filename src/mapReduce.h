#include<iostream>
#include<stdio.h>
#include <mpi.h>
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
	vector<string> fileList;
	vector<string> dirList;
	
	int nprocs, rank;
	MPI_Comm comm;
	int debug;

	public:
//data provided as blocks of string to user suppiled map function
int map(void(*)(int, string, int*));    

MapReduce(int, char**);
MapReduce(MPI_Comm communicator,int, char**);
void readDefaults(string configFile);
void parseArguments(int argc, char **argv);
void getChunks();
void error(string);
};
