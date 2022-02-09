/* ---------------------------------------------------------------
Práctica 3
Código fuente: Map.cpp
Grau Informàtica
49259651E Sergio Beltrán Guerrero 
48139505E Martí Serratosa Sedó
--------------------------------------------------------------- */

#include "Map.h"
#include "Types.h"
#include "Statistics.h"
#include <fstream> // std::ifstream

// Lee fichero de entrada (split) línea a línea y lo guarda en una cola del Map en forma de
// tuplas (key,value).

int bytesRead = 0;

TError
Map::ReadFileTuples(char *fileName, Statistics *statistics) {
    ifstream file(fileName);
    string str;
    streampos Offset = 0;
    if (!file.is_open())
        return (CErrorOpenInputFile);
    
    statistics->splitAddFile();

    while (std::getline(file, str))
    {
        statistics->splitAddLine();
        statistics->splitAddBytes(str.size());
        statistics->splitAddTuple();
        if (debug)
            printf("DEBUG::Map input %d -> %s\n", (int)Offset, str.c_str());

        AddInput(new TMapInputTuple((TMapInputKey)Offset, str));

        Offset = file.tellg();
    }

    file.close();

    return (COk);
}

void
Map::AddInput(PtrMapInputTuple tuple) {
    // No necesitamos hacer un mutex_lock porque en la clase 
    // MyQueue ya controla que no hay condiciones de carrera.
    Input.push(tuple);
}

// Ejecuta la tarea de Map: recorre la cola de tuplas de entrada y para cada una de ellas
// invoca a la función de Map especificada por el programador.
TError
Map::Run(Statistics *statistics) {
    TError err;

    while (!Input.empty()) {
        statistics->mapAddInputTuple();
        statistics->mapAddBytes(Input.front()->getValue().size());

        if (debug)
            printf("DEBUG::Map process input tuple %ld -> %s\n", (Input.front())->getKey(),
                   (Input.front())->getValue().c_str());
        err = MapFunction(this, *(Input.front()));
        if (err != COk)
            return (err);

        Input.pop();
    }

    statistics->mapAddOutputTuple(Output.size());

    return (COk);
}

// Función para escribir un resultado parcial del Map en forma de tupla (key,value)
void Map::EmitResult(TMapOutputKey key, TMapOutputValue value) {
    if (debug)
        printf("DEBUG::Map emit result %s -> %d\n", key.c_str(), value);
    pair<TMapOutputKey, TMapOutputValue> TMapOutputTuple(key, value);
    Output.push(TMapOutputTuple);
    bytesRead += key.size();
}