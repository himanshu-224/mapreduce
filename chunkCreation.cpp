
#include<iostream>
#include<vector>
#include<fstream>
#include<stdlib.h>
#include<string.h>
#include <dirent.h>
#include <sys/stat.h>
#include "Markup.h"
#include "dataStruc.h"
#include <sstream>

using namespace std;

struct IPCount
{
    string IP;
    int size;
};
class createChunks
{
    private:
        int numMaps;
        int chunkSize;
        string inputDir;
        DataType dataType;
        vector<ChunkInfo> chunks;
        vector<FileSize> fileSizes;
  
    public:      
    
    void listDir(string dirname);
    int getFileSize(string filename);  
    int getPatternPosition(string line,string sep);
    string itos(int num);
    string extractIP(string str);
    
	createChunks(int num,int csize,string str,DataType type)
	{
	    numMaps=num;
	    chunkSize=csize;
	    inputDir=str;
	    dataType=type;
	}
	
    vector<ChunkInfo> getChunks(string sep="\n")
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
    
    void textFile(string sep)
    {
        listDir(inputDir);
        int numFiles=fileSizes.size();
        int remaining=chunkSize, chunkNo=0, remFileSize=fileSizes[0].size, stByte=1;
        vector<FileInfo> chunkFiles;
        ChunkInfo data;
        chunks.push_back(data);
        chunks[0].assignedTo=chunkNo;
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
                    chunks[chunkNo].assignedTo=chunkNo;
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
    
    void binaryFile()
    {
        listDir(inputDir);
        int numFiles=fileSizes.size();
        int remaining=chunkSize, chunkNo=0, remFileSize=fileSizes[0].size, stByte=1;
        vector<FileInfo> chunkFiles;
        ChunkInfo data;
        chunks.push_back(data);
        chunks[0].assignedTo=chunkNo;
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
                    chunks[chunkNo].assignedTo=chunkNo;
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
    /*void saveChunks1(string outFilePath)
    {
        ofstream fout (outFilePath.c_str());
        if (fout.is_open())
        {
            int sz=chunks.size();
            for(int i=0;i<sz;i++)
            {
                int size=chunks[i].chunk.size();
                fout<<"CHUNK-"<<(i+1)<<"\n";
                fout<<chunks[i].size<<"\n";                
                for(int j=0;j<size;j++)
                {
                    fout<<chunks[i].chunk[j].path<<"\n";
                    fout<<chunks[i].chunk[j].startByte<<"\n";
                    fout<<chunks[i].chunk[j].endByte<<"\n";
                    fout<<"\n";
                }
            }
            fout.close();
        }
        else
        {
            cout<<"Could not open output file..Exiting\n";
            exit(-1);
        }
    }*/

    void saveChunks(string outFilePath)
    {
        CMarkup xml;
        xml.AddElem( "CHUNKMAP");
        xml.IntoElem();
        
        int sz=chunks.size();
        for(int i=0;i<sz;i++)
        {
            int size=chunks[i].chunk.size(); 
            xml.AddElem( "CHUNK");       
            xml.IntoElem();
            xml.AddElem("Number",itos(i+1));
            xml.AddElem( "Size",itos(chunks[i].size));
            xml.AddElem( "MajorIP",chunks[i].majorIP);  
            xml.AddElem("Files"); 
            xml.IntoElem();            

            for(int j=0;j<size;j++)
            {
                xml.AddElem("File");
                xml.IntoElem();
                xml.AddElem( "Path",chunks[i].chunk[j].path);  
                xml.AddElem( "IP",chunks[i].chunk[j].IP);
                xml.AddElem( "StartByte",itos(chunks[i].chunk[j].startByte));   
                xml.AddElem( "EndByte",itos(chunks[i].chunk[j].endByte)); 
                xml.OutOfElem();  
            }      
            xml.OutOfElem();
            xml.OutOfElem();

        }    
        xml.OutOfElem();
        xml.Save( outFilePath);   
    }
      
};

string createChunks::extractIP(string str)
{
    return "192.155.132.1";
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

void createChunks::listDir(string dirname)
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
                    listDir(fullPath);
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

int main(int argc, char** argv)
{

  if (argc != 3) {
    cout<<"Incorrect usage!\n";
    exit(-1);
  }
  int numMaps=2,chunkSize=1024;
  string inputDir=argv[1];
  string dtype=argv[2];
  DataType dataType;
  if (strcmp(argv[2],"text")==0)
    dataType=text;
  else if  (strcmp(argv[2],"binary")==0)
     dataType=binary;
  else
     exit(-1);
    
  vector<ChunkInfo> chunks;
  
  createChunks obj = createChunks(numMaps,chunkSize,inputDir,dataType);
  chunks = obj.getChunks();
  obj.saveChunks("chunkMap.xml");
  
  return 0;
}
