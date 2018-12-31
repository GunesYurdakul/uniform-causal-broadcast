all: da_proc

da_proc: da_proc.cpp
	g++ -std=c++11 -pthread -Wall -o da_proc da_proc.cpp

clean:
	rm da_proc
