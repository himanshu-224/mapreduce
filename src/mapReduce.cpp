
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

void MapReduce::error(string err)
{
	if (debug==1)
	{
		cout<<err<<endl;
	}
	exit(-1);
}

void MapReduce::readDefaults(string configFile)
{
	pugi::xml_document doc;
	if (!doc.load_file(configFile.c_str()))
	{
		cout<<"Could not locate configuration file..Exiting\n";
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
	parseArguments(argc,argv);
	readDefaults("configuration/config.xml");
	getChunks();
	
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
		error("MPI Environment is already initialized..Exiting\n");
	}
	comm = MPI_COMM_WORLD;
	MPI_Comm_rank(comm,&rank);
	MPI_Comm_size(comm,&nprocs);	

}

MapReduce::MapReduce(MPI_Comm communicator,int argc, char** argv)
{
	debug=1;
	parseArguments(argc,argv);
	readDefaults("configuration/config.xml");
	getChunks();
	
	comm = communicator;
	MPI_Comm_rank(comm,&rank);
	MPI_Comm_size(comm,&nprocs);

	readDefaults("configuration/config.xml");
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
			else
				error("The datatype "+value+" is not supported...Exiting\n");
				
		}
		else if (key.compare("filelist")==0)
		{
			fileList= split(value,',');
		}
		else if (key.compare("dirlist")==0)
		{
			dirList= split(value,',');
		}		
		else
			error("Invalid input parameter provided...Exiting\n");
	}
}

void MapReduce::getChunks()
{
	createChunks chunkObj = createChunks(chunkSize,dirFile,dataType, mntDir, fileList,dirList, debug);
	chunkObj.generateChunkMap(nodeInfoFile,ipListFile,chunkMapFile);
	chunkObj.printStats(); 

}