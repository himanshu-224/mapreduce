#include<iostream>
#include<stdio.h>
#include <mpi.h>
#include "dataStruc.h"
#include "logging.h" 

#ifndef MAP_REDUCE
#define MAP_REDUCE

using namespace std;
class MapReduce
{
private:
	string ipListFile;
	string nodeInfoFile;
	string chunkMapFile;
    string logFolder;
    Logging logobj;
	
	string dirFile;
	string mntDir;
	DataType dataType;
	int chunkSize;
	int numMaps;
	vector<string> fileList;
	vector<string> dirList;
	vector<ChunkInfo> chunks;
	
	int nprocs, rank;
	MPI_Comm comm;
	int debug;
	int mpi_initialized_mr;

	public:
//data provided as blocks of string to user suppiled map function
int map(void(*)(int, string, int*));    

MapReduce(int, char**);
MapReduce(MPI_Comm communicator,int, char**);
~MapReduce();
void readDefaults(string configFile);
void parseArguments(int argc, char **argv);
void getChunks();
void sendRankMapping();
void getProcChunks(int tprocs, int mypos, string myip);
void printChunks(vector<ChunkInfo> chunk);
};

#endif