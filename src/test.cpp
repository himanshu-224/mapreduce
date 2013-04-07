#include <iostream>
#include <iterator>
#include <vector>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <algorithm>

using namespace std;

int main () {
  vector<int> t(10,0);
  int i,j = 1024*64;
  for (i =0 ;i<10;i++)
	  t[i] = rand()%j + 1;
  int minpos;
  for(i = 0; i<10;i++)
	  cout<<t[i]<<" ";
  cout<<endl;
  for(i=0;i<10;i++){
	minpos = distance(t.begin(),min_element(t.begin(),t.end()));
	cout<<minpos<<endl;
	t.erase(t.begin()+minpos);
	for(j = 0; j<t.size();j++)
		cout<<t[j]<<" ";
	cout<<endl;
  }

  return 0;
}