

MPICC=/usr/bin/mpic++
MPIRUN=/usr/bin/mpirun

userprog: src/userprog.cpp src/mapReduce.h src/keyValue.h src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h src/objs/logging.o 
	$(MPICC) -g -std=c++0x -pthread -c src/userprog.cpp -o src/objs/userprog.o
	$(MPICC) -g -std=c++0x -pthread -o mapreduce src/objs/userprog.o src/mapReduce.h src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h src/objs/logging.o src/keyValue.h

runprog: 
    $(MPIRUN) -g -np 8 ./mapreduce dirfile=dirList.txt type=binary
	
mapreduce : src/objs/mapReduce.o src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h
	$(MPICC) -g -o mapreduce src/objs/mapReduce.o src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h
	
chunk: src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h
	g++ -g -o mapreduce src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h	

src/objs/pugixml.o : src/pugixml/pugixml.cpp src/pugixml/pugixml.hpp src/pugixml/pugiconfig.hpp
	g++ -g -c src/pugixml/pugixml.cpp	-o src/objs/pugixml.o	
	
src/objs/chunkCreation.o : src/chunkCreation.cpp src/chunkCreation.h src/dataStruc.h
	g++ -g -c src/chunkCreation.cpp -o src/objs/chunkCreation.o

src/objs/logging.o: src/logging.cpp  src/logging.h src/dataStruc.h
	g++ -g -c src/logging.cpp -o src/objs/logging.o

		


