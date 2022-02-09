/* ---------------------------------------------------------------
Práctica 3
Código fuente: Logger.cpp
Grau Informàtica
49259651E Sergio Beltrán Guerrero 
48139505E Martí Serratosa Sedó
--------------------------------------------------------------- */

#include "Logger.h"
#include <time.h>
#include <sys/stat.h>
#include <cstring>
#include <iostream>


#define LOGNAME_FORMAT "./log/%Y%m%d_%H%M%S"
#define LOGSTRING_FORMAT "%Y-%m-%d %H:%M:%S"
#define LOGNAME_SIZE 20

using namespace std;

Logger::Logger() {
    // Creamos el nombre del fichero log con la fecha y hora actual
    static char name[LOGNAME_SIZE];
    time_t now = time(0);
    strftime(name, sizeof(name), LOGNAME_FORMAT, localtime(&now));
    strcat(name, ".log");
    
    // Creamos el fichero de log
    mkdir("./log", S_IRWXU);
    loggerFile = fopen(name, "w");

    // Inicializaremos los mutex
    if (pthread_mutex_init(&lock, NULL) != 0)
    {
        printf("\n mutex init failed\n");
        exit(-1);
    }
}

Logger::~Logger() {
    fclose(loggerFile);
    pthread_mutex_destroy(&lock);
}

void
Logger::log(char message []) {
    pthread_mutex_lock(&lock);
    static char logMessage[256];
    time_t now = time(0);
    strftime(logMessage, sizeof(logMessage), LOGSTRING_FORMAT, localtime(&now));

    strcat(logMessage, ":  ");
    strcat(logMessage, message);
    strcat(logMessage, "\n");

    fputs(logMessage, loggerFile);
    cout << logMessage;
    pthread_mutex_unlock(&lock);
}