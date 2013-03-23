
#include<iostream>
#include<vector>
#include<fstream>
#include<stdlib.h>
#include<string.h>
#include <dirent.h>
#include <sys/stat.h>
#include<algorithm>
#include <sstream>
#include<math.h>
#include "pugixml/pugixml.hpp"
#include "dataStruc.h"
#include "chunkCreation.h"

using namespace std;
       
createChunks::createChunks(int csize,string str,DataType type, string dir, vector<string> flist, vector<string> dlist,int dbg)
    {
        chunkSize=csize;
        dirFile=str;
        dataType=type;
        mntDir=dir;
		fileList=flist;
		dirList=dlist;
		debug=dbg;
    }
    
vector<ChunkInfo> createChunks::getChunks(string sep="\n")
{
    if (dataType==binary)
        binaryFile();
    else if(dataType==text)
        textFile(sep);
        
    
    for(int i=0;i<chunks.size();i++)
    {
        int lim=chunks[i].chunk.size(), csize=0;
        vector<IPCount> ips;
        for(int j=0;j<lim;j++)
        {
            csize+=chunks[i].chunk[j].endByte-chunks[i].chunk[j].startByte+1;
            int flag=0;
            for(int k=0;k<ips.size();k++)
            {    
                if (chunks[i].chunk[j].IP.compare(ips[k].IP)==0)
                {
                    ips[k].size+=chunks[i].chunk[j].endByte-chunks[i].chunk[j].startByte+1;
                    flag=1;
                    break;
                }
            }
            if (flag==0)
            {
                IPCount ipc;
                ipc.IP= chunks[i].chunk[j].IP;
                ipc.size=chunks[i].chunk[j].endByte-chunks[i].chunk[j].startByte+1;
                ips.push_back(ipc);
            }
            
            //cout<<chunks[i].chunk[j].path<<endl;
            
        }
        int maximum=0;
        for(int j=0;j<ips.size();j++)
        {   
            if (maximum<ips[j].size)
            {
                maximum=ips[j].size;
                chunks[i].majorIP=ips[j].IP;
            }
        }
        chunks[i].size=csize;
        cout<<"Size and No. of files in chunk "<<i+1<<" = "<<csize<<" , "<<lim<<endl;
    }
    return chunks;                 
    
}

void createChunks::textFile(string sep)
{
    listDir(dirFile,mntDir);
    int numFiles=fileSizes.size();
    int remaining=chunkSize, chunkNo=0, remFileSize=fileSizes[0].size, stByte=1;
    vector<FileInfo> chunkFiles;
    ChunkInfo data;
    chunks.push_back(data);
    chunks[0].assignedTo="";
    chunks[0].chunk= chunkFiles;
    for(int i=0;i<numFiles;)
    {
        if (remFileSize<remaining)
        {
            FileInfo fi;
            fi.path=fileSizes[i].path;
            fi.IP= extractIP(fi.path);
            fi.startByte=stByte;
            fi.endByte=fileSizes[i].size;
            if (fi.endByte!=stByte+remFileSize-1)
                cout<<"Error in Chunk creation in calculation of byte offsets\n";
            chunks[chunkNo].chunk.push_back(fi);
            
            remaining=remaining-remFileSize;   
            stByte=1;
            i++;
            if (i<numFiles)
                remFileSize=fileSizes[i].size;
        }
        else
        {
            int pos=-1,curpos,prevpos;
            string line;
            ifstream nfile(fileSizes[i].path.c_str());
            nfile.seekg(stByte+remaining-2); /*-2 so that if sep character is the last character of the block it is not missed*/
            while (nfile.good())
            {
                prevpos=nfile.tellg();
                getline (nfile,line);
                curpos=nfile.tellg();
                if (strcmp(sep.c_str(),"\n")==0)
                {
                    pos=nfile.tellg();
                    break;
                }
                else
                {
                    int tmp=getPatternPosition(line,sep);/*returns the position of the end of pattern and index starts at 1*/
                    if (tmp!=-1)
                    {
                        pos=prevpos+tmp; 
                        break;
                    }
                }
                if ((curpos-(stByte+remaining-1))>chunkSize/4)
                {
                    int spacepos= line.find(' ',0)+1;
                    if (spacepos>0)
                        pos=prevpos+spacepos;
                    else
                        pos=curpos;                        
                    break;
                }                    
                
            }
            if (pos==-1)
                pos=fileSizes[i].size;
            nfile.close(); /*don't know whether file should be closed or not*/

            FileInfo fi;
            fi.path=fileSizes[i].path;
            fi.IP= extractIP(fi.path);
            fi.startByte=stByte;
            fi.endByte=pos;
            chunks[chunkNo].chunk.push_back(fi);
            
            remFileSize-=(fi.endByte- fi.startByte+1);
            stByte=fi.endByte+1;
            chunkNo+=1;
            if (not(remFileSize==0 && i==numFiles-1))
            {
                vector<FileInfo> chunkFiles;
                ChunkInfo data;
                chunks.push_back(data);                
                chunks[chunkNo].assignedTo="";
                chunks[chunkNo].chunk= chunkFiles;                
            }
            remaining=chunkSize;
        }
        if (remFileSize==0)
        {
            i++;
            if (i<numFiles)
                remFileSize=fileSizes[i].size;
             stByte=1;
        }

    }
    cout<<"No. of Chunks = "<<chunks.size()<<endl;        
}

void createChunks::binaryFile()
{
    listDir(dirFile,mntDir);
    int numFiles=fileSizes.size();
    int remaining=chunkSize, chunkNo=0, remFileSize=fileSizes[0].size, stByte=1;
    vector<FileInfo> chunkFiles;
    ChunkInfo data;
    chunks.push_back(data);
    chunks[0].assignedTo="";
    chunks[0].chunk= chunkFiles;
    for(int i=0;i<numFiles;)
    {
        if (remFileSize<remaining)
        {
            FileInfo fi;
            fi.path=fileSizes[i].path;
            fi.IP= extractIP(fi.path);
            fi.startByte=stByte;
            fi.endByte=fileSizes[i].size;
            if (fi.endByte!=stByte+remFileSize-1)
                cout<<"Error in Chunk creation in calculation of byte offsets\n";
            chunks[chunkNo].chunk.push_back(fi);
            
            remaining=remaining-remFileSize;   
            stByte=1;
            i++;
            if (i<numFiles)
                remFileSize=fileSizes[i].size;
        }
        else
        {
            FileInfo fi;
            fi.path=fileSizes[i].path;
            fi.IP= extractIP(fi.path);
            fi.startByte=stByte;
            fi.endByte=stByte+remaining-1;
            chunks[chunkNo].chunk.push_back(fi);
            
            remFileSize-=remaining;
            stByte=stByte+remaining;
            chunkNo+=1;
            if (not(remFileSize==0 && i==numFiles-1))
            {
                vector<FileInfo> chunkFiles;
                ChunkInfo data;
                chunks.push_back(data);                
                chunks[chunkNo].assignedTo="";
                chunks[chunkNo].chunk= chunkFiles;                
            }
            remaining=chunkSize;
        }
        if (remFileSize==0)
        {
            i++;
            if (i<numFiles)
                remFileSize=fileSizes[i].size;
             stByte=1;
        }
    }
    cout<<"No. of Chunks = "<<chunks.size()<<endl;
}

/*string createChunks::extractIP(string str)
{
	int pos= str.find(":");
	for(int i=pos-1;i>=0;i--)
	{
		if (str[i]=='/')
		{
		return str.substr(i+1,pos-1-i);
		}
	}
    return "";
}*/

string createChunks::extractIP(string str)
{
	int pos= str.find("#");
	for(int i=pos-1;i>=0;i--)
	{
		if (str[i]=='/')
		{
		return str.substr(i+1,pos-1-i);
		}
	}
    return "";
}

string createChunks::itos(int num)
{
    return static_cast<ostringstream*>( &(ostringstream() << num) )->str();
}

int createChunks::getFileSize(string filename)
{
        struct stat stat_buf;
        int rc = stat(filename.c_str(), &stat_buf);
        return rc == 0 ? stat_buf.st_size : 0;
} 

void createChunks::listDir(string dirFile,string mntDir)
{
		int i;
		for (i=0;i<fileList.size();i++)
		{
			FileSize f;
			f.path= mntDir+fileList[i];
			f.size=getFileSize(f.path);
			fileSizes.push_back(f);
		}
		for(i=0;i<dirList.size();i++)
		{
			string dirName = mntDir+dirList[i];
            listSingleDir(dirName);
		}
        ifstream fin (dirFile.c_str());
        char str[256];
        while (fin>>str)
        {      
			string dirName(str);
			dirName= mntDir+dirName;
            listSingleDir(dirName);
        }
        fin.close();
}
void createChunks::listSingleDir(string dirname)
{
        DIR* d_fh;
        struct dirent* entry;
        char full_path[4096];
        if ((d_fh = opendir(dirname.c_str())) == NULL)
        {
            cout<<"Couldn't open directory"<<dirname<<endl;
            exit(-1);
        }
        while((entry=readdir(d_fh)) != NULL) 
        {
            if(strncmp(entry->d_name, "..", 2) != 0 && strncmp(entry->d_name, ".", 1) != 0) 
            {
                string fullPath=dirname+string("/")+entry->d_name;
                //cout<<fullPath<<endl;
                
                if (entry->d_type == DT_DIR) 
                {
                    listSingleDir(fullPath);
                }
                else 
                {
                    FileSize f;
                    f.path=fullPath;
                    f.size=getFileSize(fullPath);
                    fileSizes.push_back(f);
                }
            }
        }
        closedir(d_fh);
}

int createChunks::getPatternPosition(string line,string sep)
{
    int pos=line.find(sep);
    if (pos>=0)   
        return pos+ sep.length();
    return -1;
}

/*Chunk Map part starts from here*/
/*helper functions*/
bool sortChunkFunc(ChunkInfo c1, ChunkInfo c2)
{
        return (c1.size > c2.size) ;
}

bool sortNodeFunc(NodeChunkInfo c1, NodeChunkInfo c2)
{
        return (c1.rating > c2.rating) ;
}



void createChunks::readNodeSpecs(string xmlFile)
{
    pugi::xml_document doc;
    if (!doc.load_file(xmlFile.c_str()))
		exit(-1);    
		
    pugi::xml_node nodespecs = doc.child("NodeSpecList");
    
    for (pugi::xml_node anode = nodespecs.first_child(); anode; anode= anode.next_sibling())
    {
        NodeSpecs data;
        
        data.IP = anode.child_value("IP");
        data.numProcs=atoi( anode.child_value("Processors"));
        data.ram=atoi(  anode.child_value("RAM") );
        data.procSpeed=atof( anode.child_value("ProcessorSpeed"));
		nodes.push_back(data);
        
    }        
}
void createChunks::getIPList(string inputFile)
{
    ifstream fin (inputFile.c_str());
    char str[256];
    while (fin>>str)
    {      
        NodeChunkInfo nci;
        nci.ip=str;
        nodeChunks.push_back(nci);
    }
    fin.close();
}

void createChunks::mapChunks()
{
    findRating();
    sortChunksAndNodes();        
    assignLocalChunks();
    assignRemainingChunks();
}

void createChunks::sortChunksAndNodes() //sort chunks by chunkSize and nodesChunks by best machine in descending order
{
    sort(chunks.begin(),chunks.end(),sortChunkFunc);
    sort(nodeChunks.begin(),nodeChunks.end(),sortNodeFunc);
}

void createChunks::assignLocalChunks()
{
    int numNodes=nodeChunks.size();
    int numChunks=chunks.size();
    int numChunksLeft=numChunks;
    for(int i=0;i<numChunks;i++)
    {
        for(int j=0;j<numNodes;j++)
        {
            if (chunks[i].majorIP.compare(nodeChunks[j].ip) ==0)
            {
                if (nodeChunks[j].numAssigned < nodeChunks[j].upperLimit)
                {
                    chunks[i].assignedTo=nodeChunks[j].ip;
                    nodeChunks[j].numAssigned++;
                    numChunksLeft-=1;
                    break;
                }
            }
            
        }
    }
            for(int j=0;j<numNodes;j++)
                    nodeChunks[j].localChunks= nodeChunks[j].numAssigned;
            
}

void createChunks::assignRemainingChunks()     
{
    int numNodes=nodeChunks.size();
    int numChunks=chunks.size();
    
    int k=-1;
    for(int i=0;i<numChunks;) //ensures that a single node does not get all the big chunks
    {
        if (chunks[i].assignedTo.compare("") ==0)
        {
            int flag=0;
            for(int j=k+1;j<numNodes;j++)
            {
                if (nodeChunks[j].numAssigned < nodeChunks[j].upperLimit)
                {
                    chunks[i].assignedTo=nodeChunks[j].ip;
                    nodeChunks[j].numAssigned++;
                    k=j;
                    flag=1;
                    i++;
                    break;
                }
            }
            if (flag==0)
            {
                k=-1;
            }
        }
        else
            i++;
    }        
}

void createChunks::printStats()
{
    int numNodes=nodeChunks.size();
    cout<<endl;
    for(int i=0;i<numNodes;i++)
    {
			if (debug==1)
			{
				cout<<"For Node : "<<nodeChunks[i].ip<<endl;
				cout<<"Upper Limit for chunks assigned : "<<nodeChunks[i].upperLimit<<endl;
				cout<<"Number of chunks assigned : "<<nodeChunks[i].numAssigned<<endl;
				cout<<"Number of local chunks assigned : "<<nodeChunks[i].localChunks<<endl;
				cout<<"Speed Rating : "<<nodeChunks[i].rating<<endl;
				cout<<"Overall Rating : "<<nodeChunks[i].loadFactor<<endl;
				cout<<endl;
			}
    }
}

void createChunks::findRating()
{
    int numNodes=nodeChunks.size();
    int totalNodes=nodes.size();
    float totalLF=0;
    for(int i=0;i<numNodes;i++)
    {
        int flag=0;
        for(int j=0;j<totalNodes;j++)
        {
            if (nodeChunks[i].ip.compare(nodes[j].IP)==0)
            {
                float procSpeedFactor=nodes[j].procSpeed/2;
                float ramFactor = nodes[j].ram/4096;
                nodeChunks[i].rating=procSpeedFactor + ramFactor *0.1;
                nodeChunks[i].numProcs= nodes[j].numProcs;
                
                float twoProcFactor =1.7;
                float fourProcfactor = 3.1;
                float lfrating = 1+(1-nodeChunks[i].rating)*0.50;
                if (nodes[j].numProcs==2)
                    nodeChunks[i].loadFactor= twoProcFactor * lfrating;
                else if (nodes[j].numProcs==4)
                    nodeChunks[i].loadFactor= fourProcfactor * lfrating;
                else if (nodes[j].numProcs==1)
                    nodeChunks[i].loadFactor= 1 * lfrating;
                flag=1;
                break;
            }
        }
        if (flag==0)
        {
            cout<<"Error...Could not find the specifications of the specified machine : "<<nodeChunks[i].ip<<endl;
            exit(-1);
        }
        totalLF+=nodeChunks[i].loadFactor;
    }
    int numChunks=chunks.size();
    float cutoff=0.9;
    int sum=0;
    while(sum<numChunks) // rounding off the no of chunks ensuring that minimum no of nodes are overloaded
    {        
        sum=0;
        for(int i=0;i<numNodes;i++)
        {
            float chunkAssigned =(nodeChunks[i].loadFactor/totalLF) * numChunks;
            if ((chunkAssigned - (int)chunkAssigned) >cutoff)
                nodeChunks[i].upperLimit=ceil(chunkAssigned);
            else
                nodeChunks[i].upperLimit=floor(chunkAssigned);
            sum+=nodeChunks[i].upperLimit;
            //cout<<cutoff<<"\t"<<nodeChunks[i].upperLimit<<endl;
        }
        cutoff=cutoff-0.05;
    }
}

void createChunks::saveChunks(string outFilePath)
{
    pugi::xml_document doc;
    pugi::xml_node node = doc.append_child("CHUNKMAP");
    
    int sz=chunks.size();
    for(int i=0;i<sz;i++)
    {
        int size=chunks[i].chunk.size(); 
        pugi::xml_node chunk = node.append_child("CHUNK");
		
		pugi::xml_node number = chunk.append_child("Number");
		number.append_child(pugi::node_pcdata).set_value(itos(i+1).c_str());
		
		pugi::xml_node assignedTo = chunk.append_child("AssignedTo");
		assignedTo.append_child(pugi::node_pcdata).set_value(chunks[i].assignedTo.c_str());
        
		pugi::xml_node psize = chunk.append_child("Size");
		psize.append_child(pugi::node_pcdata).set_value(itos(chunks[i].size).c_str());
        
		pugi::xml_node majorIP = chunk.append_child("MajorIP");
		majorIP.append_child(pugi::node_pcdata).set_value(chunks[i].majorIP.c_str());
		
        pugi::xml_node files = chunk.append_child("Files");

        for(int j=0;j<size;j++)
        {
			pugi::xml_node file = files.append_child("File");
            
			pugi::xml_node path = file.append_child("Path");
			path.append_child(pugi::node_pcdata).set_value(chunks[i].chunk[j].path.c_str());
            
			pugi::xml_node ip = file.append_child("IP");
			ip.append_child(pugi::node_pcdata).set_value(chunks[i].chunk[j].IP.c_str());
			
            pugi::xml_node startByte = file.append_child("StartByte");
			startByte.append_child(pugi::node_pcdata).set_value(itos(chunks[i].chunk[j].startByte).c_str());
			
            pugi::xml_node endByte = file.append_child("EndByte");
			endByte.append_child(pugi::node_pcdata).set_value(itos(chunks[i].chunk[j].endByte).c_str());			
            
        }      
    }    
    doc.save_file(outFilePath.c_str());
}

void createChunks::generateChunkMap(string nodeInfoFile,string ipListFile, string outputFile )
{
	getChunks();
	readNodeSpecs(nodeInfoFile);
	getIPList(ipListFile);
	mapChunks();
	saveChunks(outputFile);	
}

/*int main(int argc, char** argv)
{

  if (argc != 3) {
    cout<<"Incorrect usage!\n";
    exit(-1);
  }
  int chunkSize=1024;
  string inputDirFile=argv[1];
  string dtype=argv[2];
  string mntDir="/mnt/mpidata/";
  DataType dataType;
  if (strcmp(argv[2],"text")==0)
    dataType=text;
  else if  (strcmp(argv[2],"binary")==0)
     dataType=binary;
  else
     exit(-1);
    
  
	createChunks obj = createChunks(chunkSize,inputDirFile,dataType, mntDir);
	obj.generateChunkMap("nodesInfo.xml","ipList.txt","chunkMap.xml");
	obj.printStats(); 
  
  return 0;
}
*/