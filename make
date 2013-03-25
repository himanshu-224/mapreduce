
userprog: src/userprog.cpp src/objs/mapReduce.o src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h src/objs/logging.o
	mpic++ -c src/userprog.cpp -o src/objs/userprog.o
	mpic++ -o mapreduce src/objs/userprog.o src/objs/mapReduce.o src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h src/objs/logging.o

runprog: 
    mpirun -np 8 ./mapreduce dirfile=dirList.txt type=binary
	
mapreduce : src/objs/mapReduce.o src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h
	mpic++ -o mapreduce src/objs/mapReduce.o src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h
	
chunk: src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h
	g++ -o mapreduce src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h	

src/objs/pugixml.o : src/pugixml/pugixml.cpp src/pugixml/pugixml.hpp src/pugixml/pugiconfig.hpp
	g++ -c src/pugixml/pugixml.cpp	-o src/objs/pugixml.o	
	
src/objs/chunkCreation.o : src/chunkCreation.cpp src/chunkCreation.h
	g++ -c src/chunkCreation.cpp -o src/objs/chunkCreation.o

src/objs/mapReduce.o : src/mapReduce.cpp src/mapReduce.h
	mpic++ -c src/mapReduce.cpp -o src/objs/mapReduce.o	

src/objs/logging.o: src/logging.cpp  src/logging.h
	g++ -c src/logging.cpp -o src/objs/logging.o
    
	

		


