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
cout<<mount("10.1.255.254:/root/export", "/root/mpidata/10.1.255.254","nfs", 0,"lock,vers=3,proto=udp,addr=10.1.255.254")<<endl;
cout<<EBUSY<<endl;
printf("Reason: %s [%d]\n",strerror(errno), errno);

if (statfs ("/root/mpidata/10.1.255.254", &foo)){
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
