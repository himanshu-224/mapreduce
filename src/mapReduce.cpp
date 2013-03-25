
#include<iostream>
#include<mpi.h>
#include<stdlib.h>
#include<string.h>
#include<sstream>
#include<vector>
#include "mapReduce.h"
#include "chunkCreation.h"
#include "pugixml/pugixml.hpp"

using namespace std;


vector<string> &split(const string &s, char delim, vector<string> &elems) {
    stringstream ss(s);
    string item;
    while(getline(ss, item, delim)) {
        elems.push_back(item);
    }
    return elems;
}

vector<string> split(string s, char delim) {
    vector<string> elems;
    return split(s.c_str(), delim, elems);
}

void MapReduce::readDefaults(string configFile)
{
	pugi::xml_document doc;
	if (!doc.load_file(configFile.c_str()))
	{
		logobj.localLog("Could not locate configuration file..Exiting");
		exit(-1); 
	}
	pugi::xml_node conf = doc.child("Configuration");
	
	pugi::xml_node paths = conf.child("Paths");
	ipListFile = paths.child_value("IPListFile");
	nodeInfoFile = paths.child_value("NodeInfoFile");
	chunkMapFile= paths.child_value("ChunkMapFile");
	mntDir= paths.child_value("MountDirectory");
	
	pugi::xml_node params = conf.child("Parameters");
	chunkSize = atoi(params.child_value("ChunkSize"));
	
}

MapReduce::MapReduce(int argc, char** argv)
{
	debug=1;
	mpi_initialized_mr=1;
	
	/*this part is just for testing
		nprocs=8;
		parseArguments(argc,argv);
		readDefaults("configuration/config.xml");
		getChunks();	
		sendRankMapping();
	this part is just for testing*/
    
    logobj=Logging();
    
	int flag;
	MPI_Initialized(&flag);	
	
	if (!flag) 
	{
		int argc = 0;
		char **argv = NULL;
		MPI_Init(&argc,&argv);
	}
	else
	{
		logobj.error("MPI Environment is already initialized..Exiting\n");
	}
	
	comm = MPI_COMM_WORLD;
	MPI_Comm_rank(comm,&rank);
	MPI_Comm_size(comm,&nprocs);	

    logobj.rank=rank;
    readDefaults("configuration/config.xml");
    
    parseArguments(argc,argv);
    logobj.debug=debug;
    
	if (rank==0)
	{		
		getChunks();	
	}
	MPI_Barrier(comm);
	sendRankMapping();
}

MapReduce::MapReduce(MPI_Comm communicator,int argc, char** argv)
{
	debug=1;
	mpi_initialized_mr=0;
    
    logobj=Logging();
	
	comm = communicator;
	MPI_Comm_rank(comm,&rank);
	MPI_Comm_size(comm,&nprocs);

    logobj.rank=rank;
    readDefaults("configuration/config.xml");
    
    parseArguments(argc,argv);
    logobj.debug=debug;
    
	if (rank==0)
	{	
		getChunks();	
	}
	MPI_Barrier(comm);	
	sendRankMapping();
}

MapReduce::~MapReduce()
{
  MPI_Barrier(comm);
  if (mpi_initialized_mr==1) MPI_Finalize();
}

void MapReduce::parseArguments(int argc, char **argv)
{
	int i;
	for(i=1;i<argc;i++)
	{
		string str(argv[i]);
		int pos=str.find("=");
		string key = str.substr(0,pos);
		string value = str.substr(pos+1,str.length()-pos-1);
		if (key.compare("dirfile")==0)
		{
			dirFile=value;
		}
		else if (key.compare("type")==0)
		{
			if (value.compare("binary")==0)
				dataType=binary;
			else if (value.compare("text")==0)
				dataType=text;
			else{
				logobj.error("The datatype "+value+" is not supported...Exiting\n");
            }
				
		}
		else if (key.compare("filelist")==0)
		{
			fileList= split(value,',');
		}
		else if (key.compare("dirlist")==0)
		{
			dirList= split(value,',');
		}		
        else if (key.compare("debug")==0)
        {
            debug= atoi(value.c_str());
        }   		
		else
        {
			logobj.error("Invalid input parameter provided...Exiting\n");
        }
	}
}

void MapReduce::getChunks()
{
	createChunks chunkObj = createChunks(chunkSize,dirFile,dataType, mntDir, fileList,dirList, debug);
	chunkObj.generateChunkMap(nodeInfoFile,ipListFile,chunkMapFile);
	chunkObj.printStats(); 

}
void MapReduce::sendRankMapping()
{
	int i;
    char singleip[16];
	char **rankMap;
	rankMap=(char**)malloc(nprocs*sizeof(char*));
	for(i=0;i<nprocs;i++)
		rankMap[i]=(char*)malloc(16*sizeof(char));
	
	/*just for testing
	rank=0;
	just for testing*/
	
    char *lrankMap;
    lrankMap=(char*)malloc(nprocs*16*sizeof(char));
    
	if (rank==0)
	{
		i=0;
		ifstream fin (ipListFile.c_str());
		while (fin>>singleip)
		{
			strcpy(rankMap[i],singleip);
			i++;
		}
		fin.close();

        for(i=0;i<nprocs*16;i++)
        {
            lrankMap[i]=rankMap[i/16][i%16];
            
        }   
	}
	
    
	MPI_Bcast(lrankMap,nprocs*16,MPI_CHAR,0,comm);
	vector<int> nodeProcs;
	char str[16];
    
    if (rank!=0)
        for(i=0;i<nprocs*16;i++)
        {
            rankMap[i/16][i%16]=lrankMap[i];        
        }
        
	strcpy(str,rankMap[rank]);
    
	for(i=0;i<nprocs;i++)
	{
		if (strcmp(str,rankMap[i])==0)
		{
			nodeProcs.push_back(i);
		}
	}
	string myip(str);
	int mypos;
	for(i=0;i<nodeProcs.size();i++)
	{
		if (nodeProcs[i]==rank)
		{
			mypos=i;
			break;
		}
	}
	getProcChunks(nodeProcs.size(),mypos,myip);
}
void MapReduce::getProcChunks(int tprocs, int mypos, string myip)
{
    pugi::xml_document doc;
    if (!doc.load_file(chunkMapFile.c_str()))
    {
        logobj.error("Cannot open the xml file chunkMap.xml "+chunkMapFile);
		exit(-1);    
    }
		
    pugi::xml_node chunkmap = doc.child("CHUNKMAP");
    
	int curpos=0;
    for (pugi::xml_node achunk = chunkmap.first_child(); achunk; achunk= achunk.next_sibling())
    {
        ChunkInfo chk;
        string thisip=achunk.child_value("AssignedTo");
		if (thisip.compare(myip)==0)
		{
			if ((curpos%tprocs)==mypos)
			{
				ChunkInfo chk;
				chk.assignedTo=thisip;
				chk.number=atoi(achunk.child_value("Number"));
				chk.size=atoi(achunk.child_value("Size"));
				chk.majorIP=achunk.child_value("MajorIP");
				
				vector<FileInfo> allfiles;
				pugi::xml_node filenode=achunk.child("Files");
				for (pugi::xml_node afile = filenode.first_child(); afile; afile= afile.next_sibling())
				{
					FileInfo finfo;
					finfo.path = afile.child_value("Path");
					finfo.startByte = atoi(afile.child_value("StartByte"));
					finfo.endByte = atoi(afile.child_value("EndByte"));
					finfo.IP=afile.child_value("IP");
					allfiles.push_back(finfo);
				}
				chk.chunk=allfiles;
				chunks.push_back(chk);
			}
			curpos++;
		}	
    }        
	printChunks(chunks);
}

void MapReduce::printChunks(vector<ChunkInfo> chunks)
{
	logobj.localLog("\nCHUNKS ASSIGNED");
    for(int i=0;i<chunks.size();i++)
    {
		logobj.localLog("CHUNK");
        int lim=chunks[i].chunk.size();
		logobj.localLog(chunks[i].number);
		logobj.localLog(chunks[i].assignedTo);
		logobj.localLog(chunks[i].majorIP);
		logobj.localLog(chunks[i].size);
		logobj.localLog("\nFiles");
        for(int j=0;j<lim;j++)
        {
            logobj.localLog(chunks[i].chunk[j].path);
			logobj.localLog(chunks[i].chunk[j].startByte);
			logobj.localLog(chunks[i].chunk[j].endByte);
			logobj.localLog(chunks[i].chunk[j].IP);
		}
		logobj.localLog("");
	}	
}