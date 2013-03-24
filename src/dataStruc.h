

#ifndef DATA_STRUCTURES_H
#define DATA_STRUCTURES_H

#include<iostream>
#include<vector>
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
	int localChunks;
    int upperLimit;
};   

struct FileInfo
{
    string path;
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