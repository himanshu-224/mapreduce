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

int main () {
ofstream fout;
fout.open("/root/export/mapReduce/folder2/numberfile",ios::out);
srand(time(NULL));
int i;
for(i=0;i<10000;i++)
    fout<<rand()%10000<<" ";

fout.close();

  return 0;
}