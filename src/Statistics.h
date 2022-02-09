#ifndef STATISTICS_H
#define STATISTICS_H

#include "pthread.h"
#include "Logger.h"

class Statistics {
    public:
        pthread_mutex_t lock;
        PtrLogger logger;

        Statistics();
        ~Statistics();
        //Metodos para el split
        void splitAddFile();
        void splitAddBytes(int bytes);
        void splitAddLine();
        void splitAddTuple();
        void printSplitStatistics();

        //Metodos para Map
        void mapAddInputTuple();
        void mapAddOutputTuple(int numTuples);
        void mapAddBytes(int bytes);
        void printMapStatistics();

        //Metodos para el Suffle
        void suffleAddTuple();
        void suffleAddKey();
        void printSuffleStatistics();

        //Metodos para el Reduce
        void reduceAddKey();
        void reduceAddOcurrence(int ocurrence);
        void reduceAddBytes(int bytes);
        void printReduceStatistics();
};
typedef class Statistics TStatistics, *PtrStatistics;

#endif // STATISTICS_H