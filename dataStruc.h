
#include<iostream>
#include<vector>
using namespace std;
enum DataType{binary,text};

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
    int size;
    string majorIP;
    vector<FileInfo> chunk;
    
};
struct FileSize
{
    string path;
    int size;
};
