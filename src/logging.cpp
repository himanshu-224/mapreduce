
#include"logging.h"
#include<stdlib.h>

using namespace std;

string itos(int num)
{
    return static_cast<ostringstream*>( &(ostringstream() << num) )->str();
}

/*string itos(int num)
{
    int len=0,num1=num;
    while(num1!=0)
    {
        len++;
        num1=num1/10;
    }
    char *str=(char*)malloc((len+1)*sizeof(char));
    len=0;
    while(num!=0)
    {
        str[len]=(char)(num%10+48);
        num=num/10;
        len++;
    }
    str[len]='\0';
    char *str1=(char*)malloc((len+1)*sizeof(char));
    int j=0;
    for(int i=len-1;i>=0;i--)
        str1[j++]=str[i];
    str1[j]='\0';
    string numstring(str1);
    return numstring;
    
}*/
Logging::Logging()
{
    rank=0;
    debug=1;
    logfilepath=string("logs/")+string("logfile_")+itos(rank)+".txt";
}
Logging::~Logging()
{
    
    //logfile.close();
}

void Logging::error(string err)
{
    if (debug==1)
    {
        cout<<"Rank "<<rank<<" : "<<err<<endl;
    }
    localLog(err);
    exit(-1);
}

void Logging::localLog(string msg)
{
    ofstream logfile;
    logfilepath=string("logs/")+string("logfile_")+itos(rank)+".txt";
    logfile.open(logfilepath.c_str(),ios::out | ios::app | ios::binary);    
    logfile<<msg<<endl;
    logfile.close();
}

void Logging::localLog(int logmsg)
{
    localLog(itos(logmsg));   
}
