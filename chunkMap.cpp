#include<iostream>
#include<vector>
#include<fstream>
#include<string.h>
#include "Markup.h"
#include "dataStruc.h"
#include <sstream>
#include<math.h>
#include<algorithm>

using namespace std;
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
    int upperLimit;
};    

bool sortChunkFunc(ChunkInfo c1, ChunkInfo c2)
{
        return (c1.size > c2.size) ;
}

bool sortNodeFunc(NodeChunkInfo c1, NodeChunkInfo c2)
{
        return (c1.rating > c2.rating) ;
}
    
class chunkMap
{
    private:
        int numMaps;
        int chunkSize;
        string inputDir;
        DataType dataType;
        vector<ChunkInfo> chunks;
        vector<NodeSpecs> nodes;
        vector<NodeChunkInfo> nodeChunks;
       
        
    public:
    
    
    void readChunks(string xmlFile)
    {
        CMarkup xml;
        bool bSuccess = xml.Load(xmlFile);
        xml.FindElem("CHUNKMAP");
        xml.IntoElem();
        int i=0,j=0;
        
        while (xml.FindElem("CHUNK") )
        {
            ChunkInfo data;
            xml.IntoElem();
            /*xml.FindElem("Number");
            data.number= atoi( MCD_2PCSZ(xml.GetData()) );*/
            data.assignedTo="";
            xml.FindElem("Size" );
            data.size = atoi( MCD_2PCSZ(xml.GetData()) );
            xml.FindElem("MajorIP");
            data.majorIP = xml.GetData();
            xml.FindElem("Files");
            xml.IntoElem();
            while(xml.FindElem("File"))
            {
                FileInfo fi;
                xml.IntoElem();
                xml.FindElem("Path");
                fi.path=xml.GetData();
                xml.FindElem("IP");
                fi.IP=xml.GetData();
                xml.FindElem("StartByte");
                fi.startByte= atoi( MCD_2PCSZ(xml.GetData()));
                xml.FindElem("EndByte" );
                fi.endByte = atoi( MCD_2PCSZ(xml.GetData()));
                xml.OutOfElem();
                data.chunk.push_back(fi);
            }    
            xml.OutOfElem();
            xml.OutOfElem();
            chunks.push_back(data);
            i++;
        }        
    }

    void readNodeSpecs(string xmlFile)
    {
        CMarkup xml;
        bool bSuccess = xml.Load(xmlFile);
        xml.FindElem("NodeSpecList");
        xml.IntoElem();
        int i=0,j=0;
        
        while (xml.FindElem("Node") )
        {
            NodeSpecs data;
            xml.IntoElem();
            xml.FindElem("IP" );
            data.IP = xml.GetData();
            xml.FindElem("Processors");
            data.numProcs=atoi( MCD_2PCSZ(xml.GetData()) );
            xml.FindElem("RAM");
            data.ram=atoi( MCD_2PCSZ(xml.GetData()) );
            xml.FindElem("ProcessorSpeed");
            data.procSpeed=atof( MCD_2PCSZ(xml.GetData()) );
            xml.OutOfElem();
            nodes.push_back(data);
            i++;
        }        
    }
    void getIPList(string inputFile)
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
    
    void mapChunks()
    {
        sortChunksAndNodes();
        findRating();
        assignLocalChunks();
        //assignRemainingChunks();
    }
    
    void sortChunksAndNodes() //sort chunks by chunkSize and nodesChunks by best machine in descending order
    {
        sort(chunks.begin(),chunks.end(),sortChunkFunc);
        sort(nodeChunks.begin(),nodeChunks.end(),sortNodeFunc);
    }
    void assignLocalChunks()
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
                
    }
    void assignRemainingChunks()     
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
            xml.AddElem("AssignedTo",chunks[i].assignedTo);
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
    
    void findRating()
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
    
    string itos(int num)
    {
        return static_cast<ostringstream*>( &(ostringstream() << num) )->str();
    }   
    
};
int main()
{
    chunkMap obj = chunkMap();
    obj.readChunks("/home/himanshu/Desktop/BTPCode/chunkMap.xml");
    obj.readNodeSpecs("/home/himanshu/Desktop/BTPCode/nodesInfo.xml");
    obj.getIPList("/home/himanshu/Desktop/BTPCode/ipList.txt");
    obj.mapChunks();
    obj.saveChunks("/home/himanshu/Desktop/BTPCode/testChunks.xml");
    return 0;
}

