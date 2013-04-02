#include <iostream>
#include<string.h>
#include<stdio.h>
#include <sys/statfs.h>
#include <errno.h>
#include<sys/mount.h>
#define NFS_SUPER_MAGIC 0x6969

using namespace std;

int main()
{
    
struct statfs foo;
//cout<<mount("/dev/sda3", "/media/hd","auto", MS_RDONLY, "")<<endl;
//printf("Reason: %s [%d]\n",strerror(errno), errno);

if (statfs ("/home/himanshu/mpidata", &foo)){
    cout<<"filesystem not mounted"<<endl;
}
else
{
    cout<<"filesystem mounted"<<endl;
}
if (foo.f_type == NFS_SUPER_MAGIC) {
    cout<<"File present on NFS"<<endl;
}

}