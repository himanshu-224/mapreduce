
#include<iostream>
#include<string.h>
#include<sstream>
#include<vector>

using namespace std;

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

int main()
{

vector<string> strs;
strs= split(",hello,how,are,you",',');
int i;
for(i=0;i<strs.size();i++)
cout<<strs[i]<<endl;
return 0;
}