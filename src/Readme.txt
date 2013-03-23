
Create this additional directory "C:\cygwin\export\mapReduce"
Within this directory copy the files "ipList.txt" and "nodesInfo.xml"
The above two files were previously present in the main BTPCode folder but will be removed in this commit. Get the files from the previous commits.

Setup the directories required to simulate NFS and the cluster as follows :

Create the directory : "\mnt\mpidata\10.1.1.1#export\mapReduce\folder1" in the cygwin home folder. 
Generally the full Windows path will turn out to be "C:\cygwin\mnt\mpidata\10.1.1.1#export\mapReduce\folder1"

similarly create the folders : 
"C:\cygwin\mnt\mpidata\10.1.255.252#export\mapReduce\folder1"
"C:\cygwin\mnt\mpidata\10.1.255.253#export\mapReduce\folder1"
"C:\cygwin\mnt\mpidata\10.1.255.254#export\mapReduce\folder1"

Within each of these folders place some text data. Keep it to about 10-15 Kb in each folder for now.

Note the '#' instead of ':' in the path as windows does not allow ':' in filename.


To compile: 
make -f  make userprog

To run:

./mapreduce dirfile=dirFile.txt type=binary