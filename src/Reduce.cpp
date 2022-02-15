#include "Reduce.h"
#include "Types.h"
#include <fstream>


Reduce::Reduce(TReduceFunction reduceFunction, string OutputPath) {
    ReduceFunction = reduceFunction;
#ifdef DEBUG
    printf("DEBUG::Creating output file %s\n", output_path.c_str());
#endif

    OutputFile.open(OutputPath, std::ofstream::out | std::ofstream::trunc);
    if (!OutputFile.is_open())
        error("Reduce::Reduce Failed to open file " + OutputPath + " for writing");

    if (pthread_mutex_init(&lock, NULL) != 0) {
        printf("\n pthread_mutex_init failed\n");
        exit(1);
    }
}

Reduce::~Reduce() {
    OutputFile.close();
    pthread_mutex_destroy(&lock);
}

void Reduce::AddInputKeys(TMapOuputIterator begin, TMapOuputIterator end) {
    TMapOuputIterator it;

    for (it = begin; it != end; it++) {
        AddInput(it->first, it->second);
    }
}

void Reduce::AddInput(TReduceInputKey key, TReduceInputValue value) {
    pthread_mutex_lock(&lock);
#ifdef DEBUG
    printf("DEBUG::Reduce add input %s -> %d\n", key.c_str(), value);
#endif
    Input.insert(TReduceInputTuple(key, value));
    pthread_mutex_unlock(&lock);
}

TError
Reduce::Run() {
    TError err;
    TReduceInputIterator it2;

    for (TReduceInputIterator it1 = Input.begin(); it1 != Input.end(); it1 = it2)
    {
        TReduceInputKey key = (*it1).first;
        pair <TReduceInputIterator, TReduceInputIterator> keyRange = Input.equal_range(key);

        err = ReduceFunction(this, key, keyRange.first, keyRange.second);
        if (err != COk)
            return (err);

        pthread_mutex_lock(&lock);
        Input.erase(keyRange.first, keyRange.second);
        pthread_mutex_unlock(&lock);

        it2 = keyRange.second;
    }

    return (COk);
}

void Reduce::EmitResult(TReduceOutputKey key, TReduceOutputValue value) {
    OutputFile << key << " " << value << endl;
    statistics->reduceAddKey();
    statistics->reduceAddOcurrence(value);
    statistics->reduceAddBytes(key.size());
}