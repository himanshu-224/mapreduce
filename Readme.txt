To compile :

g++ -o test chunkCreation.cpp pugixml.cpp dataStruc.h

to Run : 

./test dirList.txt binary


Setup the directories required to simulate NFS and the cluster as follows :

Create the directory : "\mnt\mpidata\10.1.1.1#export\mapReduce\folder1" in the cygwin home folder. 
Generally the full Windows path will turn out to be "C:\cygwin\mnt\mpidata\10.1.1.1#export\mapReduce\folder1"

similarly create the folders : 
"C:\cygwin\mnt\mpidata\10.1.255.252#export\mapReduce\folder1"
"C:\cygwin\mnt\mpidata\10.1.255.253#export\mapReduce\folder1"
"C:\cygwin\mnt\mpidata\10.1.255.254#export\mapReduce\folder1"

Within each of these folders place some text data. Keep it to about 10-15 Kb in each folder for now.

Note the '#' instead of ':' in the path as windows does not allow ':' in filename.

Special note for Atul : CHECK IT AND ASK ME TODAY ONLY IF THIS CODE DOES NOT RUN. DON'T JUST WAIT FOR THE WHOLE VACATION TO BE OVER AND FINALLY SAY THE CODE DID NOT RUN.