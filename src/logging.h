
#include<iostream>
#include<fstream>
#include<sstream>

#ifndef LOG_AND_ERROR
#define LOG_AND_ERROR

using namespace std;

class Logging
{
public:
    //ofstream logfile;
    string logfilepath;
    
public:    
    int rank;
    int debug;
    Logging();
    Logging(string baseDir, int rnk, int dbg);
    ~Logging();
    void localLog(string logmsg);
    void localLog(int logmsg);
    void error(string err);
    void warning(string err);
};    

#endif