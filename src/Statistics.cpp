/* ---------------------------------------------------------------
Práctica 3
Código fuente: Statistics.cpp
Grau Informàtica
49259651E Sergio Beltrán Guerrero 
48139505E Martí Serratosa Sedó
--------------------------------------------------------------- */

#include "Statistics.h"
#include "Logger.h"
#include "pthread.h"
#include "stdlib.h"
#include "stdio.h"
#include <cstring>

using namespace std;

struct SplitStatistics {
    int numFiles;
    int numBytes;
    int numLines;
    int numTuples;
};

struct MapStatistics {
    int numInputTuples;
    int numOutputTuples;
    int numBytes;
};

struct SuffleStatistics {
    int numTuples;
    int numKeys;
};

struct ReduceStatistics {
    int numKeys;
    int numOcurrences;
    int numBytes;
};

struct SplitStatistics splitStatistics;
struct MapStatistics mapStatistics;
struct SuffleStatistics suffleStatistics;
struct ReduceStatistics reduceStatistics;

Statistics::Statistics() { 
    splitStatistics.numFiles = 0;
    splitStatistics.numBytes = 0;
    splitStatistics.numLines = 0;
    splitStatistics.numTuples = 0;

    mapStatistics.numInputTuples = 0;
    mapStatistics.numOutputTuples = 0;
    mapStatistics.numBytes = 0;

    suffleStatistics.numTuples = 0;
    suffleStatistics.numKeys = 0;

    reduceStatistics.numKeys = 0;
    reduceStatistics.numOcurrences = 0;
    reduceStatistics.numBytes = 0;

    if (pthread_mutex_init(&lock, NULL) != 0)
    {
        printf("\n mutex init failed\n");
        exit(-1);
    }
};

void
Statistics::splitAddFile() {
    pthread_mutex_lock(&lock);
    splitStatistics.numFiles++;
    pthread_mutex_unlock(&lock);
}

void
Statistics::splitAddBytes(int bytes) {
    pthread_mutex_lock(&lock);
    splitStatistics.numBytes += bytes;
    pthread_mutex_unlock(&lock);
}

void
Statistics::splitAddLine() {
    pthread_mutex_lock(&lock);
    splitStatistics.numLines++;
    pthread_mutex_unlock(&lock);
}

void
Statistics::splitAddTuple() {
    pthread_mutex_lock(&lock);
    splitStatistics.numTuples++;
    pthread_mutex_unlock(&lock);
}

void
Statistics::printSplitStatistics() {
    pthread_mutex_lock(&lock);
    char title[256];
    sprintf(title, "Split Statistics: ");
    char files[256];
    sprintf(files, "Number of files: %d  |  ", splitStatistics.numFiles);
    char bytes[256];
    sprintf(bytes, "Number of bytes: %d  |  ", splitStatistics.numBytes);
    char lines[256];
    sprintf(lines, "Number of lines: %d  |  ", splitStatistics.numLines);
    char tuples[256];
    sprintf(tuples, "Number of tuples: %d", splitStatistics.numTuples);
    char *result = strcat(title, files);
    result = strcat(result, bytes);
    result = strcat(result, lines);
    result = strcat(result, tuples);
    logger->log(result);
    pthread_mutex_unlock(&lock);
}

void
Statistics::mapAddInputTuple() {
    pthread_mutex_lock(&lock);
    mapStatistics.numInputTuples++;
    pthread_mutex_unlock(&lock);
}

void
Statistics::mapAddOutputTuple(int numTuples) {
    pthread_mutex_lock(&lock);
    mapStatistics.numOutputTuples += numTuples;
    pthread_mutex_unlock(&lock);
}

void
Statistics::mapAddBytes(int bytes) {
    pthread_mutex_lock(&lock);
    mapStatistics.numBytes += bytes;
    pthread_mutex_unlock(&lock);
}

void
Statistics::printMapStatistics() {
    pthread_mutex_lock(&lock);
    char title[256];
    sprintf(title, "Map Statistics: ");
    char input[256];
    sprintf(input, "Number of input tuples: %d  |  ", mapStatistics.numInputTuples);
    char output[256];
    sprintf(output, "Number of output tuples: %d  |  ", mapStatistics.numOutputTuples);
    char bytes[256];
    sprintf(bytes, "Number of bytes: %d", mapStatistics.numBytes);
    char *result = strcat(title, input);
    result = strcat(result, output);
    result = strcat(result, bytes);
    logger->log(result);
    pthread_mutex_unlock(&lock);
}

void
Statistics::suffleAddTuple() {
    pthread_mutex_lock(&lock);
    suffleStatistics.numTuples++;
    pthread_mutex_unlock(&lock);
}

void
Statistics::suffleAddKey() {
    pthread_mutex_lock(&lock);
    suffleStatistics.numKeys ++;
    pthread_mutex_unlock(&lock);
}

void
Statistics::printSuffleStatistics() {
    pthread_mutex_lock(&lock);
    char title[256];
    sprintf(title,"Suffle Statistics:  ");
    char tuples[256];
    sprintf(tuples, "Number of tuples: %d  |  ", suffleStatistics.numTuples);
    char keys[256];
    sprintf(keys, "Number of keys: %d", suffleStatistics.numKeys);
    char *result = strcat(title, tuples);
    result = strcat(result, keys);
    logger->log(result);
    pthread_mutex_unlock(&lock);
}

void
Statistics::reduceAddKey() {
    pthread_mutex_lock(&lock);
    reduceStatistics.numKeys ++;
    pthread_mutex_unlock(&lock);
}

void
Statistics::reduceAddOcurrence(int ocurrence) {
    pthread_mutex_lock(&lock);
    reduceStatistics.numOcurrences += ocurrence;
    pthread_mutex_unlock(&lock);
}

void
Statistics::reduceAddBytes(int bytes) {
    pthread_mutex_lock(&lock);
    reduceStatistics.numBytes += bytes;
    pthread_mutex_unlock(&lock);
}

void
Statistics::printReduceStatistics() {
    pthread_mutex_lock(&lock);
    char title[256];
    sprintf(title, "Reduce Statistics:  ");
    char keys[256];
    sprintf(keys, "Number of keys: %d  |  ", reduceStatistics.numKeys);
    char ocurrences[256];
    sprintf(ocurrences, "Number of ocurrences: %d  |  ", reduceStatistics.numOcurrences);
    char bytes[256];
    sprintf(bytes, "Number of bytes: %d", reduceStatistics.numBytes);
    char *result = strcat(title, keys);
    result = strcat(result, ocurrences);
    result = strcat(result, bytes);
    logger->log(result);
    pthread_mutex_unlock(&lock);
}
