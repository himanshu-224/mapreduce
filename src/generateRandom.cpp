#include <stdlib.h>
#include <iostream>
#include <string>
#include <sstream>
#include <algorithm>
#include <fstream>
#include <iterator>
#include<vector>
#include<time.h>

using namespace std;

int main (int argc, char **argv) {
ofstream fout;
fout.open("/home/himanshu/mpidata/10.1.1.1/mapReduce/folder2/numberfile",ios::out);
//srand(time(NULL));
int i;
int n=10000;
if (argc>1)
    n=atoi(argv[1]);
    
for(i=0;i<n;i++)
	fout<<rand()%10000<<" ";

fout.close();

  return 0;
}
