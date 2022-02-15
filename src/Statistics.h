#ifndef STATISTICS_H
#define STATISTICS_H

#include "pthread.h"
#include "Logger.h"

class Statistics {
    public:
        pthread_mutex_t lock;

        Statistics();
        ~Statistics();
        void splitAddFile();
        void splitAddBytes(int bytes);
        void splitAddLine();
        void splitAddTuple();
        void printSplitStatistics();

        void mapAddInputTuple();
        void mapAddOutputTuple(int numTuples);
        void mapAddBytes(int bytes);
        void printMapStatistics();

        void suffleAddTuple();
        void suffleAddKey();
        void printSuffleStatistics();

        void reduceAddKey();
        void reduceAddOcurrence(int ocurrence);
        void reduceAddBytes(int bytes);
        void printReduceStatistics();
};
typedef class Statistics TStatistics, *PtrStatistics;

#endif // STATISTICS_H