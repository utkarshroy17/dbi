#include "Pipe.h"

#include <iostream>
#include <stdlib.h> 

Pipe :: Pipe (int bufferSize) {

	// cout << "Pipe initialized " << bufferSize << endl;

	// set up the mutex assoicated with the pipe
	pthread_mutex_init (&pipeMutex, NULL);

	// set up the condition variables associated with the pipe
	pthread_cond_init (&producerVar, NULL);
	pthread_cond_init (&consumerVar, NULL);

	// set up the pipe's buffer
	buffered = new (std::nothrow) Record[bufferSize];
	if (buffered == NULL)
	{
		cout << "ERROR : Not enough memory. EXIT !!!\n";
		exit(1);
	}

	totSpace = bufferSize;
	firstSlot = 0;
	lastSlot = 0;

	// cout << "firstSlot " << firstSlot << " " << lastSlot << endl;

	// note that the pipe has not yet been turned off
	done = 0;
}

Pipe :: ~Pipe () {
	cout << "Pipe desctructor \n";

	// free everything up!
	delete [] buffered;

	pthread_mutex_destroy (&pipeMutex);
	pthread_cond_destroy (&producerVar);
	pthread_cond_destroy (&consumerVar);
	
}


void Pipe :: Insert (Record *insertMe) {
	cout << "Pipe Insert \n";

	// first, get a mutex on the pipeline
	pthread_mutex_lock (&pipeMutex);

	// next, see if there is space in the pipe for more data; if
	// there is, then do the insertion
	if (lastSlot - firstSlot < totSpace) {
		buffered [lastSlot % totSpace].Consume (insertMe);
	// if there is not, then we need to wait until the consumer
	// frees up some space in the pipeline
	} else {
		//cout << "full" << endl;
		pthread_cond_wait (&producerVar, &pipeMutex);
		//cout << "empty" << endl;
		buffered [lastSlot % totSpace].Consume (insertMe);		
	}
	
	// note that we have added a new record
	lastSlot++;

	// signal the consumer who might now want to suck up the new
	// record that has been added to the pipeline
	pthread_cond_signal (&consumerVar);


	// done!
	pthread_mutex_unlock (&pipeMutex);
	cout << "2.Pipe Insert " << firstSlot << " " << lastSlot << endl;
}

void Pipe::Get(){
	cout << "Pipe.Get " << firstSlot << " " << lastSlot << endl;
}

int Pipe :: Remove (Record *removeMe) {		//https://stackoverflow.com/questions/22288667/c-member-variable-losing-value-after-constructor
	cout << "Calling Remove Pipe.cc " << firstSlot << " " << lastSlot << endl;

	// first, get a mutex on the pipeline
	pthread_mutex_lock (&pipeMutex);

	// next, see if there is anything in the pipeline; if
	// there is, then do the removal
	if (lastSlot != firstSlot) {
		
		cout << "2 Remove Pipe.cc \n";
		removeMe->Consume (&buffered [firstSlot % totSpace]);

	// if there is not, then we need to wait until the producer
	// puts some data into the pipeline
	} else {

		cout << "3 Remove Pipe.cc \n";
		// the pipeline is empty so we first see if this
		// is because it was turned off
		if (done) {

			pthread_mutex_unlock (&pipeMutex);
			return 0;
		}

		// wait until there is something there
		pthread_cond_wait (&consumerVar, &pipeMutex);

		// since the producer may have decided to turn off
		// the pipe, we need to check if it is still open
		if (done && lastSlot == firstSlot) {
			pthread_mutex_unlock (&pipeMutex);
			return 0;
		}

		removeMe->Consume (&buffered [firstSlot % totSpace]);
	}
	
	// note that we have deleted a record
	firstSlot++;

	// signal the producer who might now want to take the slot
	// that has been freed up by the deletion
	pthread_cond_signal (&producerVar);
	
	// done!
	pthread_mutex_unlock (&pipeMutex);
	return 1;

	cout << "End of Remove, Pipe.cc \n";
}


void Pipe :: ShutDown () {

	cout << "Pipe shutdown \n";

	// first, get a mutex on the pipeline
        pthread_mutex_lock (&pipeMutex);

	// note that we are now done with the pipeline
	done = 1;

	// signal the consumer who may be waiting
	pthread_cond_signal (&consumerVar);

	// unlock the mutex
	pthread_mutex_unlock (&pipeMutex);
	
}
