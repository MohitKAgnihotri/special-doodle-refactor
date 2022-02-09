#ifndef LOGGER_H_
#define LOGGER_H_

#include <string>

class Logger {
    FILE *loggerFile;
    public:
    	pthread_mutex_t lock;

        Logger();
        ~Logger();
        void log(char message[]);
};
typedef class Logger TLogger, *PtrLogger;

#endif // LOGGER_H_