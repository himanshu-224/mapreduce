#include<iostream>
#include<stdio.h>
#include <mpi.h>
#include <queue>
#include <thread>
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
    string myip;
	DataType dataType;
	int chunkSize;
	int numMaps;
	vector<string> fileList;
	vector<string> dirList;
	vector<ChunkInfo> chunks;
    
    queue<int> chunksObtained;
    std::thread t1;
	
	int nprocs, rank;
	MPI_Comm comm;
	int debug;
    int chunksCompleted;
	int mpi_initialized_mr;

	public:
//data provided as blocks of string to user suppiled map function

int map(int argc,char **argv, void(*mapfunc)(primaryKV&, int&));
int map(void(*mapfunc)(int nprocs, int rank, int&));
int map(void(*genfunc)(queue<char>&,int&), void(*mapfunc)(primaryKV&, int&));

MapReduce(int, char**);
MapReduce(MPI_Comm communicator,int, char**);
~MapReduce();
void readDefaults(string configFile);
void parseArguments(int argc, char **argv);
void getChunks();
void sendRankMapping();
void getProcChunks(int tprocs, int mypos, string myip);
void printChunks(vector<ChunkInfo> chunk);

void islocal();
void fetchdata(int index1,int index2, int filenum);
void fetchNonLocal();
vector<primaryKV> createChunk(int front);

void sendData(char* chunk,int size, int curRank);

};

#endif