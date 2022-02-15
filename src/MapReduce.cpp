
#include "MapReduce.h"
#include "Types.h"
#include "Logger.h"


#include <dirent.h>
#include <string.h>
#include <pthread.h>
#include <unistd.h>
#include <sys/stat.h>
#include <fstream>
#include <pthread.h>

using namespace std;
struct mapReduceFile {
    string input_filename;
    MapReduce *myMapReduce;
    PtrMap map;
    int numThread;
};

int numberOfThreads = 0;
vector <string> files;
pthread_barrier_t barrier;
pthread_cond_t cond;
pthread_mutex_t condMutex;
int nsincroSplit = 0;
bool firstSplit = false;
int nsincroMap = 0;
bool firstMap = false;
int nsincroSuffle = 0;
bool firstSuffle = false;

static void increment_safely_statistics(int *var) {
    pthread_mutex_lock(&condMutex);
    (*var)++;
    pthread_cond_signal(&cond);
    pthread_mutex_unlock(&condMutex);
}

MapReduce::MapReduce(char *input, char *output, TMapFunction mapf, TReduceFunction reducef, int nreducers) {
    MapFunction = mapf;
    ReduceFunction = reducef;
    InputPath = input;
    OutputPath = output;

    statistics = new TStatistics();

    for (int x = 0; x < nreducers; x++) {
        char filename[256] = {0};

        sprintf(filename, "%s/result.r%d", OutputPath, x + 1);
        PtrReduce reduce = new TReduce(ReduceFunction, filename);
        reduce->statistics = statistics;
        AddReduce(reduce);
    }

    if (pthread_mutex_init(&lock, NULL) != 0) {
        printf("\n pthread_mutex_init Init Failed\n");
        exit(1);
    }
}

MapReduce::~MapReduce() {
    pthread_mutex_destroy(&lock);
}

TError
MapReduce::Run(struct mapReduceFile *mapReduceStruct) {
    pthread_mutex_lock(&lock);
    if (!firstSplit) {
        statistics->printSplitStatistics();
        firstSplit = true;
    }
    pthread_mutex_unlock(&lock);

    if (Split(mapReduceStruct) != COk)
        error("MapReduce::Run-Error Split");

    increment_safely_statistics(&nsincroSplit);

    pthread_mutex_lock(&lock);
    if (!firstMap) {
        statistics->printMapStatistics();
        firstMap = true;
    }
    pthread_mutex_unlock(&lock);

    if (Map(mapReduceStruct) != COk)
        error("MapReduce::Run-Error Map");

    increment_safely_statistics(&nsincroMap);

    pthread_mutex_lock(&lock);
    if (!firstSuffle) {
        statistics->printSuffleStatistics();
        firstSuffle = true;
    }
    pthread_mutex_unlock(&lock);

    if (Suffle(mapReduceStruct) != COk)
        error("MapReduce::Run-Error Merge");

    increment_safely_statistics(&nsincroSuffle);

    pthread_barrier_wait(&barrier);

    return (COk);
}


TError
MapReduce::Split(struct mapReduceFile *mapReduceStruct) {
    mapReduceStruct->map = new TMap(MapFunction);
    int l = mapReduceStruct->input_filename.length();
    char *inputFile = new char[l + 1];
    strcpy(inputFile, mapReduceStruct->input_filename.c_str());

    char message[256];
    sprintf(message, "Processing input file %s", mapReduceStruct->input_filename.c_str());
    write_message_to_log_file(message);

    pthread_mutex_lock(&lock);
    Mappers.push_back(mapReduceStruct->map);
    pthread_mutex_unlock(&lock);

    mapReduceStruct->map->ReadFileTuples(inputFile, statistics);
    return (COk);
}

TError
MapReduce::Map(struct mapReduceFile *mapReduceStruct) {
#ifdef DEBUG
        printf("DEBUG::Running Map %d\n", (int) mapReduceStruct->numThread + 1);
#endif
    mapReduceStruct->map->Run(statistics);
    return (COk);
}

TError
MapReduce::Suffle(struct mapReduceFile *mapReduceStruct) {
    MyQueue<TMapOutputTuple> output = mapReduceStruct->map->getOutput();

    while (!output.empty()) {
        TMapOutputTuple tuple = output.front();
        TMapOutputKey key = tuple.first;

        int r = std::hash < TMapOutputKey > {}(key) % Reducers.size();

#ifdef DEBUG
            printf("DEBUG::MapReduce::Suffle merge key %s to reduce %d.\n", key.c_str(), r);
#endif

        Reducers[r]->AddInput(tuple.first, tuple.second);
        statistics->suffleAddKey();
        statistics->suffleAddTuple();

        output.pop();
    }
    return (COk);
}

void *runReduce(TReduce reduce) {
    reduce.Run();
}

TError
MapReduce::Reduce() {
    vector <pthread_t> tids;
    for (vector<TReduce>::size_type m = 0; m != Reducers.size(); m++) {
        pthread_t tid;
        tids.push_back(tid);
        if (pthread_create(&tid, NULL, (void *(*)(void *)) runReduce, (void *) Reducers[m]) != 0) {
            CancelThreads(tids);
        }
    }
    for (vector<TReduce>::size_type m = 0; m != Reducers.size(); m++) {
        pthread_join(tids[m], NULL);
    }

    return (COk);
}

void *runSplitMapSuffle(struct mapReduceFile *mapReduceStruct) {
    if (mapReduceStruct->myMapReduce->Run(mapReduceStruct) != COk)
        error("MapReduce::Run-Error");
}


TError
MapReduce::RunThreads(vector <string> filenames) {
    numberOfThreads = filenames.size();
    files = filenames;
    struct mapReduceFile mapReduceStruct[numberOfThreads];

    if (pthread_barrier_init(&barrier, NULL, numberOfThreads + 1) != 0) {
        printf("\n barrier init failed\n");
        exit(1);
    }

    for (int i = 0; i < numberOfThreads; i++) {
        pthread_t tid;
        tids.push_back(tid);
        mapReduceStruct[i].input_filename = files[i].c_str();
        mapReduceStruct[i].myMapReduce = this;
        mapReduceStruct[i].numThread = i;

        if (pthread_create(&tid, NULL, (void *(*)(void *)) runSplitMapSuffle, (void *) &mapReduceStruct[i]) != 0)
            CancelThreads(tids);
    }

    if (pthread_mutex_init(&condMutex, NULL) != 0) {
        printf("\n mutex init failed\n");
        CancelThreads(tids);
    }
    if (pthread_cond_init(&cond, NULL) != 0) {
        printf("\n cond init failed\n");
        CancelThreads(tids);
    }

    while (nsincroSplit < numberOfThreads) {
        pthread_mutex_lock(&condMutex);
        pthread_cond_wait(&cond, &condMutex);
        pthread_mutex_unlock(&condMutex);
    }
    statistics->printSplitStatistics();

    while (nsincroMap < numberOfThreads) {
        pthread_mutex_lock(&condMutex);
        pthread_cond_wait(&cond, &condMutex);
        pthread_mutex_unlock(&condMutex);
    }
    statistics->printMapStatistics();

    while (nsincroSuffle < numberOfThreads) {
        pthread_mutex_lock(&condMutex);
        pthread_cond_wait(&cond, &condMutex);
        pthread_mutex_unlock(&condMutex);
    }
    statistics->printSuffleStatistics();

    pthread_barrier_wait(&barrier);

    statistics->printReduceStatistics();
    if (Reduce() != COk)
        error("MapReduce::Run-Error Reduce");
    statistics->printReduceStatistics();

    pthread_barrier_destroy(&barrier);
    return (COk);
}

void MapReduce::CancelThreads(vector <pthread_t> &tids) {
    char log[256];
    sprintf(log, "Cancelling threads");
    write_message_to_log_file(log);
    for (int i = 0; i < tids.size(); i++) {
        pthread_cancel(tids[i]);
    }
    exit(1);
}