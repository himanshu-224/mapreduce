
chunk: src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h
	g++ -o mapreduce src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h

mapreduce : src/objs/mapReduce.o src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h
	g++ -o mapreduce src/objs/mapReduce.o src/objs/chunkCreation.o src/objs/pugixml.o src/dataStruc.h
	
src/objs/mapReduce.o : src/mapReduce.cpp src/mapReduce.h
	g++ -c src/mapReduce.cpp -o src/objs/mapReduce.o

src/objs/chunkCreation.o : src/chunkCreation.cpp src/chunkCreation.h
	g++ -c src/chunkCreation.cpp -o src/objs/chunkCreation.o
	
src/objs/pugixml.o : src/pugixml/pugixml.cpp src/pugixml/pugixml.hpp src/pugixml/pugiconfig.hpp
	g++ -c src/pugixml/pugixml.cpp	-o src/objs/pugixml.o
	

	

		


