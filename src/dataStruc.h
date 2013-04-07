

#ifndef DATA_STRUCTURES_H
#define DATA_STRUCTURES_H

#include<iostream>
#include<vector>

#define NUM_KEY 2
#define SIZE_NXTMSG 3
#define KEYVALUE 4
#define END_MAP_TASK 5
#define END_MAP 6

using namespace std;
enum DataType{binary,text};

struct NodeSpecs
{
    string IP;
    string hostName;
    int numProcs;
    int ram;
    float procSpeed;
};  

struct NodeChunkInfo
{
    string ip;
    float rating;
    float loadFactor;
    int numProcs;
    int numAssigned;
    int sizeAssigned;
	int localChunks;
    int upperLimit;
    int sizeLimit;
};   

struct primaryKV{
    string key;
    string value;
};

struct FileInfo
{
    string path;
    string localpath;
    int startByte;
    int endByte;
    string IP;
};

struct ChunkInfo
{
    string assignedTo;
	int number;
    int size;
    string majorIP;
    int local;
    vector<FileInfo> chunk;
    
};
struct FileSize
{
    string path;
    int size;
};

struct IPCount
{
    string IP;
    int size;
};

#endif