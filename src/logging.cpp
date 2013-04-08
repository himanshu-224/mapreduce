
#include"logging.h"
#include<stdlib.h>

using namespace std;

string itos(int num)
{
    return static_cast<ostringstream*>( &(ostringstream() << num) )->str();
}

Logging::Logging()
{
}
Logging::Logging(string baseDir, int rnk, int dbg)
{
    rank=rnk;
    debug=dbg;
    logfilepath=baseDir+string("logfile_")+itos(rank)+".txt";
    ofstream logfile;
    logfile.open(logfilepath.c_str(),ios::out);    
    logfile.close();    
}
Logging::~Logging()
{
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

void Logging::warning(string err)
{
    if (debug==1)
    {
        cout<<"Rank "<<rank<<" : "<<err<<endl;
    }
    localLog(err);
}

void Logging::localLog(string msg)
{
    ofstream logfile;
    logfile.open(logfilepath.c_str(),ios::out | ios::app | ios::binary);    
    logfile<<msg<<endl;
    logfile.close();
}

void Logging::localLog(int logmsg)
{
    localLog(itos(logmsg));   
}
