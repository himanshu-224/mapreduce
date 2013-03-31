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
        int chunkSize;
        string dirFile;
		string mntDir;
        DataType dataType;
        string separator;
        int fsplit;
        vector<ChunkInfo> chunks;
        vector<FileSize> fileSizes;
		vector<string> fileList;
		vector<string> dirList;
		int debug;
		
		vector<NodeSpecs> nodes;
        vector<NodeChunkInfo> nodeChunks;
		
	public:      
	
	createChunks(int csize,string str,DataType type,string separator,string split, string dir, vector<string> fileList, vector<string> dirList, int debug);
	
    void listSingleDir(string dirFile);
	void listDir(string dirFile, string mntDir);
    int getFileSize(string filename);  
    int getPatternPosition(string line,string sep);
    string itos(int num);
    string extractIP(string str);
	
	vector<ChunkInfo> getChunks(string);
    void noSplit();
	void textFile(string sep);
	void binaryFile();
	
	void readNodeSpecs(string xmlFile);
    void getIPList(string inputFile);
	void mapChunks();
	void sortChunksAndNodes();
	void assignLocalChunks();
	void assignRemainingChunks();
    void assignLocalChunksNoSplit();
    void assignRemainingChunksNoSplit();
	void printStats();
	void findRating();
	
	void saveChunks(string outFilePath);
	void generateChunkMap(string nodeInfoFile,string ipListFile, string outputFile);
	
	

};

#endif