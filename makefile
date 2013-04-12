
MPICC=mpic++
MPIRUN=mpirun
    
pname=wordcount

$(pname): src/objs/$(pname).o src/mapReduce.h src/keyValue.h src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h src/objs/logging.o
	$(MPICC) -g -std=c++0x -pthread -o mapreduce1 src/objs/$(pname).o src/mapReduce.h src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h src/objs/logging.o src/keyValue.h
	
compilerun:src/objs/$(pname).o src/mapReduce.h src/keyValue.h src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h src/objs/logging.o 
	$(MPICC) -g -std=c++0x -pthread -o mapreduce1 src/objs/$(pname).o src/mapReduce.h src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h src/objs/logging.o src/keyValue.h
	sudo $(MPIRUN) -np 4 ./mapreduce1 dirfile=dirList.txt type=binary  
	
run:	
	sudo $(MPIRUN) -np 4 ./mapreduce1 dirfile=dirList.txt type=binary 
	
src/objs/pugixml.o : src/pugixml/pugixml.cpp src/pugixml/pugixml.hpp src/pugixml/pugiconfig.hpp
	g++ -g -c src/pugixml/pugixml.cpp	-o src/objs/pugixml.o
	
src/objs/$(pname).o :	
	    $(MPICC) -g -std=c++0x -pthread -c src/$(pname).cpp -o src/objs/$(pname).o    	
	
src/objs/chunkCreation.o : src/chunkCreation.cpp src/chunkCreation.h src/dataStruc.h
	g++ -g -c src/chunkCreation.cpp -o src/objs/chunkCreation.o

src/objs/logging.o: src/logging.cpp  src/logging.h src/dataStruc.h
	g++ -g -c src/logging.cpp -o src/objs/logging.o

