kvstore2pcsystem:main.o Coordinator.o Participant.o MessageProcessor.o
	g++ main.o Coordinator.o Participant.o MessageProcessor.o -o kvstore2pcsystem
main.o:main.cpp
	g++ -c main.cpp
Coordinator.o:Coordinator.cpp Coordinator.h
	g++ -c Coordinator.cpp
Participant.o:Participant.cpp Participant.h
	g++ -c Participant.cpp
MessageProcessor.o:MessageProcessor.cpp MessageProcessor.h
	g++ -c MessageProcessor.cpp
clean:
	-rm main.o
	-rm Coordinator.o
	-rm Participant.o
	-rm MessageProcessor.o
	-rm kvstore2pcsystem
