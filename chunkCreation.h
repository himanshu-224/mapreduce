#include<iostream>
#include<vector>
#include<fstream>
#include<stdlib.h>
#include<string.h>
#include <dirent.h>
#include <sys/stat.h>
#include "dataStruc.h"
#include <sstream>

#ifndef CHUNK_CREATED
#define CHUNK_CREATED

class createChunks
{
    private:
        int numMaps;
        int chunkSize;
        string dirFile;
		string mntDir;
        DataType dataType;
        vector<ChunkInfo> chunks;
        vector<FileSize> fileSizes;
		
		vector<NodeSpecs> nodes;
        vector<NodeChunkInfo> nodeChunks;
		
	public:      
	
	createChunks(int num,int csize,string str,DataType type, string dir);
	
    void listSingleDir(string dirFile);
	void listDir(string dirFile, string mntDir);
    int getFileSize(string filename);  
    int getPatternPosition(string line,string sep);
    string itos(int num);
    string extractIP(string str);
	
	vector<ChunkInfo> getChunks(string);
	void textFile(string sep);
	void binaryFile();
	
	void readNodeSpecs(string xmlFile);
    void getIPList(string inputFile);
	void mapChunks();
	void sortChunksAndNodes();
	void assignLocalChunks();
	void assignRemainingChunks();
	void printStats();
	void findRating();
	
	void saveChunks(string outFilePath);
	void generateChunkMap(string nodeInfoFile,string ipListFile, string outputFile);
	
	

};

#endif