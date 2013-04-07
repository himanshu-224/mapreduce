
#include<iostream>
#include<sstream>
#include<vector>
#include<algorithm>
#include<queue>
#include<thread>

#include<mpi.h>
#include<stdio.h>
#include<stdlib.h>
#include<string.h>
#include<unistd.h>
#include<error.h>

#include<sys/stat.h>
#include<sys/statfs.h>
#include<sys/mount.h>

#include "dataStruc.h"
#include "logging.h" 
#include "keyValue.h"
#include "chunkCreation.h"
#include "pugixml/pugixml.hpp"

#define NFS_SUPER_MAGIC 0x6969

#ifndef MAP_REDUCE
#define MAP_REDUCE

using namespace std;

template<class K,class V>
class MapReduce
{
private:
	string ipListFile;
	string nodeInfoFile;
	string chunkMapFile;
    string logFolder;
    
    Logging logobj;
	KeyValue<K,V> *kv;
    
    string separator;
    string fsplit;
	string dirFile;
    string homedir;
	string mntDir;
    string logDir;
    string myip;
    string rootip;
	DataType dataType;
	int chunkSize;
    int isCluster;
	int numMaps;
    int numReducers;
	vector<string> fileList;
	vector<string> dirList;
	vector<ChunkInfo> chunks;
    
    queue<int> chunksObtained;
    //std::thread t1;
	
	int nprocs, rank;
	MPI_Comm comm;
	int debug;
    int chunksCompleted;
	int mpi_initialized_mr;

	public:
//data provided as blocks of string to user suppiled map function

int map(int argc,char **argv, void(*mapfunc)(vector<primaryKV>&, MapReduce<K,V> *),int(*hashfunc)(K key, int nump));
int map(int argc,char **argv, void(*mapfunc)(vector<string>,  MapReduce<K,V> *),int(*hashfunc)(K key, int nump));
int map(void(*mapfunc)(int nprocs, int rank,  MapReduce<K,V> *),int(*hashfunc)(K key, int nump));
int map(void(*genfunc)(queue<char>&,int&), void(*mapfunc)(primaryKV&,  MapReduce<K,V> *),int(*hashfunc)(K key, int nump));

MapReduce(int, char**,int);
MapReduce(MPI_Comm communicator,int, char**);
~MapReduce();
void readDefaults(string configFile);
void sendDefaults();
void receiveDefaults();
void parseArguments(int argc, char **argv);
void getChunks();
void sendRankMapping();
void getProcChunks(int tprocs, int mypos, string myip);
void printChunks(vector<ChunkInfo> chunk);

void islocal();
void mountDir();
void fetchdata(int index1,int index2, int filenum);
void fetchNonLocal();
void reduceReceive();
void reduceSort();
vector<primaryKV> createChunk(int front);

void sendData(char* chunk,int size, int curRank);

void addkv(K, V);
void finalisemap(int(*hashfunc)(K key, int nump));

};

//End of header file part

/*Start of Implementation Part*/

string itos(int num);
string extractIP(string str);

//template <class K,class V> void threadFunc1(MapReduce<int,int> *obj);

void RecvData(queue<char> &buffer, int &completed, int recvRank, MPI_Comm comm, Logging &logobj);

vector<string> filesystemsList(string dirFile,vector<string> dirList,vector<string> fileList);

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

void uniqueInsert(vector<string>& iplist, string ip)
{
    int flag=0;
    for(int i=0;i<iplist.size();i++)
    {
        if (iplist[i].compare(ip)==0){
            flag=1;
            break;
        }
    }
    if (flag==0)
        iplist.push_back(ip);
}

vector<string> filesystemsList(string dirFile,vector<string> dirList,vector<string> fileList)
{
    int i;
    vector<string> iplist;
    for (i=0;i<fileList.size();i++)
    {
        uniqueInsert(iplist,extractIP(dirList[i]));
    }
    for(i=0;i<dirList.size();i++)
    {
        uniqueInsert(iplist,extractIP(dirList[i]));
    }
    if (dirFile.compare("")==0)     
        return iplist;
    
    ifstream fin (dirFile.c_str());
    char str[256];
    while (fin>>str)
    {      
        string dirName(str);
        uniqueInsert(iplist,extractIP(dirName));
    }
    fin.close();
    return iplist;
}    

template <class K,class V>
void MapReduce<K,V>::readDefaults(string configFile)
{
    pugi::xml_document doc;
    if (!doc.load_file(configFile.c_str()))
    {
        cout<<"Could not locate configuration file..Exiting";
        exit(-1); 
    }
    pugi::xml_node conf = doc.child("Configuration");
    
    pugi::xml_node paths = conf.child("Paths");
    homedir= paths.child_value("HomeDirectory");
    ipListFile = homedir+paths.child_value("IPListFile");
    nodeInfoFile = homedir+paths.child_value("NodeInfoFile");
    chunkMapFile= homedir+paths.child_value("ChunkMapFile");
    mntDir= homedir+paths.child_value("MountDirectory");
    logDir=homedir+paths.child_value("LogDirectory");
    
    pugi::xml_node params = conf.child("Parameters");
    chunkSize = atoi(params.child_value("ChunkSize"));  
    isCluster= atoi(params.child_value("Cluster"));
}

template <class K,class V>
void MapReduce<K,V>::sendDefaults()
{
    int n=6;
    int arr[n];
    string globalchunkMapFile;
    
    if (isCluster)
    {
    string singleip;
    ifstream fin (ipListFile.c_str());
    fin>>singleip;
    fin.close();    
    int pos=chunkMapFile.find("export");
    globalchunkMapFile=mntDir+singleip+chunkMapFile.substr(pos+6);
    }
    else
        globalchunkMapFile=chunkMapFile;
    
    arr[0]=homedir.length();
    arr[1]=globalchunkMapFile.length(); 
    arr[2]=mntDir.length();
    arr[3]=itos(chunkSize).length();
    arr[4]=itos(isCluster).length();
    arr[5]=logDir.length();
    
    string str = homedir+globalchunkMapFile+mntDir+itos(chunkSize)+itos(isCluster)+logDir;
    char *buffer = strdup(str.c_str());
    
    for(int i=1;i<nprocs;i++){
        MPI_Send(arr,n,MPI_INT,i,0,comm);    
        MPI_Send(buffer,str.length(),MPI_CHAR,i,0,comm);
    }
}

template <class K,class V>
void MapReduce<K,V>::receiveDefaults()
{
    MPI_Status status;
    int n=6,length=0,curpos=0;
    int arr[n];
    MPI_Recv(arr,n,MPI_INT,0,0,comm,&status);
    
    for(int i=0;i<n;i++)
        length+=arr[i];
    
    char* buffer= new char[length];
    
    MPI_Recv(buffer,length,MPI_CHAR,0,0,comm,&status);
    string str(buffer);
    
    homedir=str.substr(curpos,arr[0]);
    curpos+=arr[0];
    chunkMapFile=str.substr(curpos,arr[1]);
    curpos+=arr[1];
    mntDir=str.substr(curpos,arr[2]);
    curpos+=arr[2];
    chunkSize=atoi(str.substr(curpos,arr[3]).c_str());
    curpos+=arr[3];
    isCluster=atoi(str.substr(curpos,arr[4]).c_str());
    curpos+=arr[4];
    logDir=str.substr(curpos,arr[5]);
}

template <class K,class V>
void threadFunc1(MapReduce<K,V> *obj)
{
    obj->fetchNonLocal();
}

template<class K, class V>
void ReducerReceive(MapReduce<K,V> *mr) 
{
    mr->reduceReceive();
}

template<class K, class V>
void ReducerSort(MapReduce<K,V> *mr)
{
    mr->reduceSort();
}


template <class K,class V>
void MapReduce<K,V>::reduceReceive()
{
    
}

template <class K,class V>
void MapReduce<K,V>::reduceSort()
{
    
}



template <class K,class V>
MapReduce<K,V>::MapReduce(int argc, char** argv, int numRed)
{
    debug=1;
    mpi_initialized_mr=1;
    chunksCompleted=0;
    numReducers = numRed;
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
        cout<<"MPI Environment is already initialized..Exiting\n";
        exit(-1);
    }
    
    comm = MPI_COMM_WORLD;
    MPI_Comm_rank(comm,&rank);
    MPI_Comm_size(comm,&nprocs);    

    if (rank==0)
    {
        if (numReducers>nprocs)
        {
            cout<<"No. of reducers exceeds no of processors\n";
            exit(-1);
        }        
        readDefaults("configuration/config.xml");
        sendDefaults();
    }
    else
    {
        receiveDefaults();
    }
    mkdir(logDir.c_str(),S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH); //created directory for logs
    logobj=Logging(logDir,rank,debug);
    
    parseArguments(argc,argv);
    kv=new KeyValue<K,V>(comm,logobj);
    
    if (rank==0)  /*Mount all directories except its own in READONLY mode*/
    {
        mkdir(mntDir.c_str(),S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        logobj.localLog("Created directory : "+mntDir);
        vector<string> iplist = filesystemsList(dirFile,dirList,fileList);
        
        string singleip;
        ifstream fin (ipListFile.c_str());
        fin>>singleip;
        fin.close(); 
        iplist.erase(remove(iplist.begin(), iplist.end(), singleip), iplist.end());
        
        for (vector<string>::iterator it=iplist.begin(); it!=iplist.end();++it)
        {
            if (isCluster)
            {
                struct statfs foo;
                string src = *it+":"+homedir+"export";
                string dest= mntDir+*it;
                int notMounted=statfs (dest.c_str(), &foo);
                if ((not notMounted)&& (foo.f_type == NFS_SUPER_MAGIC))
                {  
                    logobj.localLog("Directory "+ src + " already mounted  at "+dest);
                }
                else
                {
                    if (notMounted) //not mounted, .i.e directory does not exist
                    {
                    mkdir(dest.c_str(),S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);  
                    logobj.localLog("Created directory : "+dest);
                    }      
                    logobj.localLog("Mounting directory "+ src + " at "+dest);

                    string flags="nolock,vers=3,proto=udp,addr="+*it;
                    int rvalue = mount(src.c_str(),dest.c_str(),"nfs", MS_RDONLY ,flags.c_str());
                    if (rvalue==-1 && errno==EBUSY)
                         logobj.localLog("Directory already mounted");
                    else if (rvalue==-1)
                         logobj.error("Could not mount directory "+src+" at "+dest+"\nError : "+strerror(errno) +"["+itos(errno)+"]"+"\n...Exiting");    
                }
            }   

        }
    }
   
}

/*MapReduce::MapReduce(MPI_Comm communicator,int argc, char** argv)
{
    debug=1;
    mpi_initialized_mr=0;
    chunksCompleted=0;
    
    comm = communicator;
    MPI_Comm_rank(comm,&rank);
    MPI_Comm_size(comm,&nprocs);

    readDefaults("configuration/config.xml");

}*/

template <class K,class V>
MapReduce<K,V>::~MapReduce()
{
  MPI_Barrier(comm);
  if (mpi_initialized_mr==1) MPI_Finalize();
}

template <class K,class V>
void MapReduce<K,V>::parseArguments(int argc, char **argv)
{
    int i;
    dirFile="";
    dataType=binary;
    separator="\n";
    fsplit="yes";
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
        else if (key.compare("sep")==0)
        {
            if (value.compare("space")==0)
                separator=' ';
            else
                separator=value;
        }
        else if (key.compare("split")==0)
        {
            if (value.compare("yes")==0 || value.compare("no")==0)
                fsplit=value;
            else
                logobj.error("Invalid answer provided for split. Valid arguments are \nsplit=yes\nsplit=no\nExiting\n");
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

template <class K,class V>
void MapReduce<K,V>::getChunks()
{
    createChunks chunkObj = createChunks(chunkSize,dirFile,dataType,separator,fsplit, mntDir, fileList,dirList, debug);
    chunkObj.generateChunkMap(nodeInfoFile,ipListFile,chunkMapFile);
    chunkObj.printStats(); 

}

template <class K,class V>
void MapReduce<K,V>::sendRankMapping()
{
    int i;
    char singleip[16];
    char **rankMap;
    rankMap=(char**)malloc(nprocs*sizeof(char*));
    for(i=0;i<nprocs;i++)
        rankMap[i]=(char*)malloc(16*sizeof(char));
    
    char *lrankMap;
    lrankMap=(char*)malloc(nprocs*16*sizeof(char));
    
    if (rank==0)
    {
        logobj.localLog("Reading ip list from "+ipListFile);
        i=0;
        ifstream fin (ipListFile.c_str());
        while (fin>>singleip)
        {
            strcpy(rankMap[i],singleip);
            i++;
            if (i==nprocs)
                break;
        }
        fin.close();
        
        if (i<nprocs){
            for(int j=i;j<nprocs;j++)
            {
                strcpy(rankMap[j],rankMap[(j-i)%i]);
            }
        }
        logobj.localLog("Obtained Rank Map from "+ipListFile);
        for(i=0;i<nprocs*16;i++)
        {
            lrankMap[i]=rankMap[i/16][i%16];
            
        }   
    }
    
    MPI_Bcast(lrankMap,nprocs*16,MPI_CHAR,0,comm);
    vector<int> nodeProcs;
    char str[16];
    
    if (rank!=0){   
        for(i=0;i<nprocs*16;i++)
        {
            rankMap[i/16][i%16]=lrankMap[i];        
        }
        logobj.localLog("Obtained Rank Map from rank 0");           
        char tmpstr[16];
        strcpy(tmpstr,rankMap[0]);
        rootip=string(tmpstr);
    }
        
    strcpy(str,rankMap[rank]);
    
    for(i=0;i<nprocs;i++)
    {
        if (strcmp(str,rankMap[i])==0)
        {
            nodeProcs.push_back(i);
        }
    }
    myip=string(str);
    int mypos;
    for(i=0;i<nodeProcs.size();i++)
    {
        if (nodeProcs[i]==rank)
        {
            mypos=i;
            break;
        }
    }
    logobj.localLog("Determined the ranks of other processes running on the same node");           
    
    if(rank!=0 && isCluster) //Mounting root process's export directory. Should be mounted as Read/Write
    {
        mkdir(mntDir.c_str(),S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        logobj.localLog("Created directory : "+mntDir);

        struct statfs foo;
        string src = rootip+":"+homedir+"export";
        string dest= mntDir+rootip; 
        int notMounted=statfs (dest.c_str(), &foo);
        if ((not notMounted)&& (foo.f_type == NFS_SUPER_MAGIC))
        {  
            logobj.localLog("Directory "+ src + " already mounted  at "+dest);
        }
        else
        {
            if (notMounted) //not mounted, .i.e directory does not exist
            {
                mkdir(dest.c_str(),S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);  
                logobj.localLog("Created directory : "+dest);
            }      
            logobj.localLog("Mounting directory "+ src + " at "+dest);

            string flags="nolock,vers=3,proto=udp,addr="+rootip;
            int rvalue = mount(src.c_str(),dest.c_str(),"nfs", 0 ,flags.c_str());
            if (rvalue==-1 && errno==EBUSY)
                 logobj.localLog("Directory already mounted");
            else if (rvalue==-1)
                 logobj.error("Could not mount directory "+src+" at "+dest+"\nError : "+strerror(errno) +"["+itos(errno)+"]"+"\n...Exiting");    
        }
    }
    getProcChunks(nodeProcs.size(),mypos,myip);
}

/*Get the information of all the chunks to be handled by this processor*/
template <class K,class V>
void MapReduce<K,V>::getProcChunks(int tprocs, int mypos, string myip)
{
    pugi::xml_document doc;
    if (!doc.load_file(chunkMapFile.c_str()))
    {
        logobj.error("Cannot open the xml file "+chunkMapFile);
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
    logobj.localLog("Obtained list of chunks assigned to this process");           
    //printChunks(chunks);
    islocal();
      
    if(isCluster) //Mounting its own export directory. Probably should be mounted Read/Write. For rank 0 must be mounted Read/Write
    {

        struct statfs foo;
        string src = myip+":"+homedir+"export";
        string dest= mntDir+myip; 
        int notMounted=statfs (dest.c_str(), &foo);
        if ((not notMounted)&& (foo.f_type == NFS_SUPER_MAGIC))
        {  
            logobj.localLog("Directory "+ src + " already mounted  at "+dest);
        }
        else
        {
            if (notMounted) //not mounted, .i.e directory does not exist
            {
                mkdir(dest.c_str(),S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);  
                logobj.localLog("Created directory : "+dest);
            }      
            logobj.localLog("Mounting directory "+ src + " at "+dest);
            string flags;

            if (rank==0)
                flags="nolock,vers=3,proto=udp,addr="+myip;  //should be lock
            else
                flags="nolock,vers=3,proto=udp,addr="+myip;

            int rvalue = mount(src.c_str(),dest.c_str(),"nfs", 0 ,flags.c_str());
            if (rvalue==-1 && errno==EBUSY)
                 logobj.localLog("Directory already mounted");
            else if (rvalue==-1)
                 logobj.error("Could not mount directory "+src+" at "+dest+"\nError : "+strerror(errno) +"["+itos(errno)+"]"+"\n...Exiting");    
        }
    }
}

template <class K,class V>
void MapReduce<K,V>::printChunks(vector<ChunkInfo> chunks)
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

/*If even one file in the non-chunk is nonlocal, it is treated as non-local. For local chunks no data needs to be fetched
 but for non-local chunks the non-local part needs to be fetched into memory*/

template <class K,class V>
void MapReduce<K,V>::islocal()
{
    logobj.localLog("Determining which chunks are present locally");
     for(int i=0;i<chunks.size();i++)
     {
         int lim=chunks[i].chunk.size();
         chunks[i].local=1;
         for(int j=0;j<lim;j++)
         {
             if (myip.compare(chunks[i].chunk[j].IP)!=0)
             {
                 chunks[i].local=0;
             }
             chunks[i].chunk[j].localpath="";
                 
         }
         if (chunks[i].local==1)
             chunksObtained.push(i);
     }
     //logobj.localLog("Local chunks obtained "+itos(chunksObtained.size()));
         
}

template <class K,class V>
vector<primaryKV> MapReduce<K,V>::createChunk(int front)
{   
    vector<primaryKV> chunk;
    int crsize=0;
    for(int i=0;i<chunks[front].chunk.size();i++)
    {
        primaryKV pkv;
        FileInfo fi=chunks[front].chunk[i];
        ifstream fin;
        int length=fi.endByte-fi.startByte+1;
        if (fi.localpath.compare("")==0)
        {   
            pkv.key=fi.path;
            fin.open(fi.path.c_str(), ios::in | ios::binary);
            fin.seekg (fi.startByte-1, fin.beg);
        }
        else
        {
            pkv.key=fi.localpath;
            fin.open(fi.localpath.c_str(), ios::in | ios::binary);
            fin.seekg(0,fin.beg);
        }
        char *buffer= new char[length];
        fin.read(buffer,length);
        pkv.value=string(buffer,length);
        chunk.push_back(pkv);
        crsize+=pkv.value.length();
        
        if (pkv.value.length()!=fin.gcount())
        {
            logobj.localLog("\tFor chunk "+itos(chunks[front].number)+ ", for part "+itos(i+1)+",bytes read NOTEQUALS gcount");
        }
        
        //logobj.localLog("\tSize of chunk "+itos(chunks[front].number)+ " required, part "+itos(i+1)+" = "+itos(length));
        logobj.localLog("\tSize of chunk "+itos(chunks[front].number)+ " created, part "+itos(i+1)+" = "+itos(pkv.value.length()));        
        //logobj.localLog("\tSize of chunk "+itos(chunks[front].number)+ " read, part "+itos(i+1)+" = "+itos(fin.gcount()));
        fin.close();
    }    
    logobj.localLog("Size of chunk "+itos(chunks[front].number)+ " required  = "+itos(chunks[front].size));
    logobj.localLog("Size of chunk "+itos(chunks[front].number)+ " created  = "+itos(crsize));
    return chunk;
}

template <class K,class V>
void MapReduce<K,V>::fetchdata(int index1, int index2, int filenum)
{
    ifstream fin;
    ofstream fout;
    FileInfo fi=chunks[index1].chunk[index2];
    string outfile="/tmp/rank_"+itos(rank)+"chunk_"+itos(chunks[index1].number)+"file_"+itos(filenum);
    fi.localpath=outfile;
    
    fin.open(fi.path.c_str(), ios::in);
    fin.seekg(fi.startByte-1, fin.beg);
    int length = fi.endByte-fi.startByte+1;
    
    int cutoff=32*1024*1024;
    //int cutoff=32;
    fout.open(outfile.c_str(),ios::out | ios::binary);
    if (length<=cutoff)
        cutoff=length;
    
    while(length>0)    
    {
        char *buffer= new char[cutoff];
        fin.read(buffer,cutoff);
        logobj.localLog("Chunk "+itos(chunks[index1].number)+" : No. of bytes read = "+itos(fin.gcount()));
        fout.write(buffer,cutoff);
        
        length-=cutoff;
        if (length<=cutoff)
            cutoff=length;        
    }
    
    fin.close();
    fout.close();
}

template <class K,class V>
void MapReduce<K,V>::fetchNonLocal()
{ 
    if (rank!=0 && isCluster) //rank 0 has already mounted all the required directories
    {
        mountDir();
    }
    
    logobj.localLog("Fetching non-local chunks...");
     for(int i=0;i<chunks.size();i++)
     {
         if (chunks[i].local==0)
         {
            int lim=chunks[i].chunk.size();
            int filenum=0;
            for(int j=0;j<lim;j++)
            {
                if (myip.compare(chunks[i].chunk[j].IP)!=0)
                {
                    fetchdata(i,j,filenum);
                    filenum++;
                }
            }    
            chunksObtained.push(i);
         }
         
     }
     logobj.localLog("Chunks obtained "+itos(chunksObtained.size()));
}

template <class K,class V>
void MapReduce<K,V>::mountDir()
{
    logobj.localLog("Mounting directories ..");
    vector<string> iplist;
    for(int i=0;i<chunks.size();i++)
     {
         if (chunks[i].local==0)
         {
            int lim=chunks[i].chunk.size();
            int filenum=0;
            for(int j=0;j<lim;j++)
            {
                if (myip.compare(chunks[i].chunk[j].IP)!=0)
                {
                    if (chunks[i].chunk[j].IP.compare(rootip)!=0)
                        uniqueInsert(iplist,chunks[i].chunk[j].IP); 
                }
            }    
            chunksObtained.push(i);
         }
         
     } 
     /*Mount all required directories except rank 0's directory and own directory as they 
      have already been mounted. Sufficient to mount READ ONLY*/
     
     for (vector<string>::iterator it=iplist.begin(); it!=iplist.end();++it)
     {
        struct statfs foo;
        string src = *it+":"+homedir+"export";
        string dest= mntDir+*it; 
        int notMounted=statfs (dest.c_str(), &foo);
        if ((not notMounted)&& (foo.f_type == NFS_SUPER_MAGIC))
        {  
            logobj.localLog("Directory "+ src + " already mounted  at "+dest);
        }
        else
        {
            if (notMounted) //not mounted, .i.e directory does not exist
            {
                mkdir(dest.c_str(),S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);  
                logobj.localLog("Created directory : "+dest);
            }      
            logobj.localLog("Mounting directory "+ src + " at "+dest);

            string flags="nolock,vers=3,proto=udp,addr="+*it;
            int rvalue = mount(src.c_str(),dest.c_str(),"nfs", MS_RDONLY ,flags.c_str());
            if (rvalue==-1 && errno==EBUSY)
                 logobj.localLog("Directory already mounted");
            else if (rvalue==-1)
                 logobj.error("Could not mount directory "+src+" at "+dest+"\nError : "+strerror(errno) +"["+itos(errno)+"]"+"\n...Exiting");    
        }

     }     
     
}

template<class K>
int defaulthash(K key, int nump)
{
    hash<K> hash_fn;
    size_t v = hash_fn(key)%nump;
    //v = rand()%nump + 1;
    return (int)v;
}

template <class K,class V>
int MapReduce<K,V>::map(int argc,char **argv, void(*mapfunc)(vector<primaryKV>&, MapReduce<K,V> *), int(*hashfunc)(K key, int nump) = defaulthash<K>)
{
    parseArguments(argc,argv);
    if (rank==0)
    {   
        getChunks();    
    }
    thread t2;
    thread t3;
    if (rank<numReducers)
    {
        t2 = thread(ReducerReceive<K,V>,this);
        t3=thread(ReducerSort<K,V>,this);
    }
    MPI_Barrier(comm);  
    
    sendRankMapping();
    thread t1=thread(threadFunc1<K,V>,this);
    
    int totalChunks=chunks.size();
    logobj.localLog("Total Chunks "+itos(totalChunks));
    while(chunksCompleted!=totalChunks)
    {
        if (!chunksObtained.empty())
        {
            int front =chunksObtained.front();
            vector<primaryKV> chunk=createChunk(front);
            chunksObtained.pop();
            chunksCompleted++;
            /*Insert Map Code Here*/
            mapfunc(chunk,this);
            //finalisemap(hashfunc);
            /*Insert map Code here*/
        }
        else
        {
            usleep(1000); //sleep for a millisecond;
        }
    }  
    t1.join();
    
   if (rank<numReducers){
       t2.join();
       t3.join();
   }
    return 1;
}

/*For the case when file splitting is not allowed. The user defined map function 
 * is given the paths of the files belonging to its chunk. If a file is nonlocal
 * it is first copied locally and the local path is provided*/
template <class K,class V>
int MapReduce<K,V>::map(int argc,char **argv, void(*mapfunc)(vector<string>,  MapReduce<K,V> *),int(*hashfunc)(K key, int nump) = defaulthash<K> )
{
    parseArguments(argc,argv);
    if (rank==0)
    {   
        getChunks();    
    }  
    thread t2;
    thread t3;
    if (rank<numReducers)
    {
        t2 = thread(ReducerReceive<K,V>,this);
        t3=thread(ReducerSort<K,V>,this);
    }
    MPI_Barrier(comm);  
    
    sendRankMapping();
    thread t1=thread(threadFunc1<K,V>,this);
    
    int totalChunks=chunks.size();
    logobj.localLog("Total Chunks "+itos(totalChunks));
    while(chunksCompleted!=totalChunks)
    {
        if (!chunksObtained.empty())
        {
            int front =chunksObtained.front();
            chunksObtained.pop();
            chunksCompleted++;

            vector<FileInfo> fileList = chunks[front].chunk;
            int numFiles=fileList.size();
            vector<string> pathList;
            /*Insert Map Code Here*/
            for(int i=0;i<numFiles;i++)
            {
                if (fileList[i].localpath.compare("")==0)
                    pathList.push_back(fileList[i].path);
                else
                    pathList.push_back(fileList[i].localpath);
            }
            mapfunc(pathList,this);
            finalisemap(hashfunc);
            /*Insert map Code here*/
        }
        else
        {
            usleep(1000); //sleep for a millisecond;
        }
    }  
    t1.join();
    if (rank<numReducers)
    {
        t2.join();
        t3.join();
    }
    return 1;
}

/* For the case when no data is provided to the user defined map function. The user is responsible for 
 * generating appropriate portion of data for its process  using it's process's rank and nprocs*/
template <class K,class V>
int MapReduce<K,V>::map(void(*mapfunc)(int nprocs, int rank,  MapReduce<K,V> *),int(*hashfunc)(K key, int nump) = defaulthash<K> )
{  
    int kv;
    thread t2;
    thread t3;
    if (rank<numReducers)
    {
        t2 = thread(ReducerReceive<K,V>,this);
        t3=thread(ReducerSort<K,V>,this);
    }
    MPI_Barrier(comm);
    
    mapfunc(nprocs, rank, this);
    finalisemap(hashfunc);
    
    if (rank<numReducers){
        t2.join();
        t3.join();
    }
    
    return 1;
}

/*For the case when data is not read from the disk but is generated centrally, i.e. by rank 0. The generated 
 data is then sent to each of the processes*/
template <class K,class V>
int MapReduce<K,V>::map(void(*genfunc)(queue<char>&,int&), void(*mapfunc)(primaryKV&,  MapReduce<K,V> *),int(*hashfunc)(K key, int nump) = defaulthash<K> )
{  
    queue<char> buffer;
    int completed=0,flag=1;

    thread t2;
    thread t3;
    if (rank<numReducers)
    {
        t2 = thread(ReducerReceive<K,V>,this);
        t3=thread(ReducerSort<K,V>,this);
    }
    MPI_Barrier(comm);
    if (rank==0)
    {
        thread t1=thread(genfunc,buffer,completed);
    
        int curRank=1;/*rank 0 should not be assigned any maps*/
        while(flag)
        {
            char* chunk=new char[chunkSize];
            if (buffer.size()>=chunkSize){
                for(int j=0;j<chunkSize;j++)
                {
                    chunk[j]=buffer.front();
                    buffer.pop();                  
                }   
                sendData(chunk,chunkSize,curRank);
                curRank=(curRank+1)%nprocs;
                if (curRank==0)
                    curRank++;                  
            }
            else if (completed==1){
                int j=0;
                while(!buffer.empty())
                {
                    chunk[j++]=buffer.front();
                    buffer.pop();                   
                }
                sendData(chunk,j,curRank);
                flag=0;
            }
        }
        for(int i=1;i<nprocs;i++) /*Notify completion of data transfer*/
        {
            MPI_Send(NULL,0,MPI_INT,i,3,comm);
        }
        t1.join();
    }
    else
    {
       thread t1 = thread(RecvData,buffer, completed, 0, comm, logobj);
        int i=1;
        while(flag)
        {
            string chunk="";
            if (buffer.size()>=chunkSize){
                for(int j=0;j<chunkSize;j++)
                {
                    chunk+=buffer.front();
                    buffer.pop();
                }
                
            }
            else if (completed==1){
                while(!buffer.empty())
                {
                    chunk+=buffer.front();
                    buffer.pop();
                }
                flag=0;
            }   
            if (chunk.compare("")!=0)
            {
                primaryKV chk;
                chk.key=itos((nprocs-1)*i+rank);
                chk.value=chunk;
                mapfunc(chk,this);
                finalisemap(hashfunc);
                i++;
            }
      }
       t1.join();
    }
    
    if (rank<numReducers)
    {
        t2.join();
        t3.join();
    }
    return 1;
}

/*MPI_Send vs MPI_Isend?? */
template <class K,class V>
void MapReduce<K,V>::sendData(char* chunk,int size, int curRank)
{
    MPI_Send(&size,1,MPI_INT,curRank,2,comm);
    MPI_Send(chunk,size,MPI_CHAR,curRank,1,comm);
}

void RecvData(queue<char> &buffer, int &completed, int recvRank, MPI_Comm comm, Logging &logobj)
{
    int size=-1;
    MPI_Status status;
    char *chunk;
    while(1)
    {
        MPI_Probe(0, MPI_ANY_TAG,comm,&status);
        if (status.MPI_TAG==3) {
            MPI_Recv(NULL,0,MPI_INT,0,3,comm,&status);
            completed=1;
            break;
        }
    
        else if (status.MPI_TAG==2) {
            MPI_Recv(&size,1,MPI_INT,recvRank,1,comm,&status);
        }
        else if (status.MPI_TAG==1) 
        {
            if (size==-1) {
                logobj.error("Receiving data from rank 0 : could not receive size of data...Exiting");
            }
            chunk = new char[size];
            MPI_Recv(chunk,size,MPI_CHAR,recvRank,1,comm,&status);
            size=-1;
            for(int i=0;i<size;i++)
                buffer.push(chunk[i]);
        }
    }
}


// Wrapper functions to operate on keyvalue pair
template <class K, class V>
void MapReduce<K,V>::addkv(K key, V value)
{
	kv->add(key,value);
}

template <class K, class V>
void MapReduce<K,V>::finalisemap(int(*hashfunc)(K key, int nump) = defaulthash<K>)
{
	//logobj.localLog("Entered finalisemap\n");
	int t = kv->sortkv();
	logobj.localLog("sorted keyValue pair");
	kv->partitionkv(numReducers,t,hashfunc);
	logobj.localLog("exit finalisemap");
}

#endif
