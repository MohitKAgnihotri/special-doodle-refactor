#include "Logger.h"
#include <time.h>
#include <sys/stat.h>
#include <cstring>
#include <iostream>


#define LOGNAME_FORMAT "./log/%Y%m%d_%H%M%S"
#define LOGSTRING_FORMAT "%Y-%m-%d %H:%M:%S"
#define LOGNAME_SIZE 20

bool is_logger_init = false;
static FILE *loggerFile;
static pthread_mutex_t file_access_lock;




void write_message_to_log_file(char message [])
{
    if (!is_logger_init)
    {
        static char name[LOGNAME_SIZE];
        time_t now = time(0);
        strftime(name, sizeof(name), LOGNAME_FORMAT, localtime(&now));
        strcat(name, ".log");

        mkdir("./log", S_IRWXU);
        loggerFile = fopen(name, "w");

        if (pthread_mutex_init(&file_access_lock, NULL) != 0)
        {
            printf("\n mutex init failed\n");
            exit(-1);
        }
        is_logger_init = true;
    }
    pthread_mutex_lock(&file_access_lock);
    static char logMessage[256];
    time_t now = time(0);
    strftime(logMessage, sizeof(logMessage), LOGSTRING_FORMAT, localtime(&now));

    strcat(logMessage, ":  ");
    strcat(logMessage, message);
    strcat(logMessage, "\n");

    fputs(logMessage, loggerFile);
    std::cout << logMessage;
    pthread_mutex_unlock(&file_access_lock);
}

void finish_writing_message_to_log_file( void )
{
    if(is_logger_init)
    {
        fclose(loggerFile);
        pthread_mutex_destroy(&file_access_lock);
        is_logger_init = false;
    }
}