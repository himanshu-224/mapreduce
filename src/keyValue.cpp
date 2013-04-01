#include <mpi.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <time.h>

#include "keyValue.h"
#include "mapReduce.h"
#include "logging.h"

template <class K,class V>
KeyValue<K,V>::KeyValue(MapReduce *mr_caller, MPI_Comm communicator, Logging *log_caller)
{
	mr = mr_caller;
	comm = communicator;
	logobj = log_caller;
	MPI_Comm_rank(comm,&me);
	int curtime = time(NULL);

	int n = 100;
	filename = (char*)malloc(n);
	if(filename == NULL)
	{
		char str[255];
		char name[255];
		int namelen;
		MPI_Get_processor_name(name,&namelen);
		name[namelen]='\0';
		
		sprintf(str,"ERROR on proc %d (%s): Failed to allocate %lu bytes for array filename\n",me,name,n);
		logobj->error(str);
	}
	sprintf(filename,"/export/mapReduce/keyValue/kv.%d",curtime);
	fileflag = 0;
	fp=NULL;
}

template <class K,class V>
KeyValue<K,V>::~KeyValue()
{
	if(fileflag)
	{
		remove(filename);
	}
	delete [] filename;
}