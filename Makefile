cc = g++
cppflag = -std=c++11 -Wextra -Wall -pthread
LFLAGS = -std=c++11 -Wall -g

all: MapReduceFramework.a Search

MapReduceFramework.a: MapReduceFramework.o
	ar -rcs MapReduceFramework.a MapReduceFramework.o

Search: Search.o MapReduceFramework.o
	$(cc) $(cppflag) Search.o MapReduceFramework.o -o Search

MapReduceFramework.o: MapReduceFramework.h MapReduceFramework.cpp
	$(cc) -c $(cppflag) MapReduceFramework.cpp

Search.o: MapReduceFramework.h Search.cpp
	$(cc) -c $(cppflag) Search.cpp


clean:
	rm -f *.o *.a Search *.tar MAIN

tar: Makefile README Search.cpp MapReduceFramework.cpp
	tar cvf ex3.tar $^