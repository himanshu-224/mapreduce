
#include<iostream>
//#include<mpi.h>
#include<stdlib.h>
#include "mapReduce.h"
#include "pugixml/pugixml.hpp"

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
