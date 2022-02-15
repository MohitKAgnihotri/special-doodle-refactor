#include "Statistics.h"
#include "Logger.h"
#include "pthread.h"
#include "stdlib.h"
#include "stdio.h"
#include <cstring>
#include <mutex>

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

typedef struct my_statistics
{
    struct SplitStatistics splitStatistics;
    struct MapStatistics mapStatistics;
    struct SuffleStatistics suffleStatistics;
    struct ReduceStatistics reduceStatistics;
}my_statistics_t;

my_statistics_t my_stat;

Statistics::Statistics() {
    memset(&my_stat, 0, sizeof(my_statistics_t));
    if (pthread_mutex_init(&lock, NULL) != 0)
    {
        printf("\n mutex init failed\n");
        exit(-1);
    }
};

static void increment_statistics (int &statistics, int value) {
    pthread_mutex_lock(&Statistics::lock);
    statistics += value;Statistics
    pthread_mutex_unlock(&Statistics::lock);
}

void Statistics::printSplitStatistics() {
    pthread_mutex_lock(&lock);
    char title[256];
    sprintf(title, "Split Statistics: ");
    char files[256];
    sprintf(files, "Number of files: %d  |  ", my_stat.splitStatistics.numFiles);
    char bytes[256];
    sprintf(bytes, "Number of bytes: %d  |  ", my_stat.splitStatistics.numBytes);
    char lines[256];
    sprintf(lines, "Number of lines: %d  |  ", my_stat.splitStatistics.numLines);
    char tuples[256];
    sprintf(tuples, "Number of tuples: %d", my_stat.splitStatistics.numTuples);
    char *result = strcat(title, files);
    result = strcat(result, bytes);
    result = strcat(result, lines);
    result = strcat(result, tuples);
    write_message_to_log_file(result);
    pthread_mutex_unlock(&lock);
}

void Statistics::printMapStatistics() {
    pthread_mutex_lock(&lock);
    char title[256];
    sprintf(title, "Map Statistics: ");
    char input[256];
    sprintf(input, "Number of input tuples: %d  |  ", my_stat.mapStatistics.numInputTuples);
    char output[256];
    sprintf(output, "Number of output tuples: %d  |  ", my_stat.mapStatistics.numOutputTuples);
    char bytes[256];
    sprintf(bytes, "Number of bytes: %d", my_stat.mapStatistics.numBytes);
    char *result = strcat(title, input);
    result = strcat(result, output);
    result = strcat(result, bytes);
    write_message_to_log_file(result);
    pthread_mutex_unlock(&lock);
}

void Statistics::printSuffleStatistics() {
    pthread_mutex_lock(&lock);
    char title[256];
    sprintf(title,"Suffle Statistics:  ");
    char tuples[256];
    sprintf(tuples, "Number of tuples: %d  |  ", my_stat.suffleStatistics.numTuples);
    char keys[256];
    sprintf(keys, "Number of keys: %d", my_stat.suffleStatistics.numKeys);
    char *result = strcat(title, tuples);
    result = strcat(result, keys);
    write_message_to_log_file(result);
    pthread_mutex_unlock(&lock);
}

void Statistics::printReduceStatistics() {
    pthread_mutex_lock(&lock);
    char title[256];
    sprintf(title, "Reduce Statistics:  ");
    char keys[256];
    sprintf(keys, "Number of keys: %d  |  ", my_stat.reduceStatistics.numKeys);
    char ocurrences[256];
    sprintf(ocurrences, "Number of ocurrences: %d  |  ", my_stat.reduceStatistics.numOcurrences);
    char bytes[256];
    sprintf(bytes, "Number of bytes: %d", my_stat.reduceStatistics.numBytes);
    char *result = strcat(title, keys);
    result = strcat(result, ocurrences);
    result = strcat(result, bytes);
    write_message_to_log_file(result);
    pthread_mutex_unlock(&lock);
}
