#ifndef MAPREDUCE_H_
#define MAPREDUCE_H_

#include "Map.h"
#include "Reduce.h"
#include "MyQueue.h"
#include "Logger.h"
#include "Statistics.h"

#include <functional>
#include <string>

class MapReduce 
{
	char *InputPath;
	char *OutputPath;
	TMapFunction MapFunction;
	TReduceFunction ReduceFunction;
	PtrLogger logger;
	PtrStatistics statistics;

	vector<pthread_t> tids;		
	vector<PtrMap> Mappers;
	vector<PtrReduce> Reducers;

	public:
		pthread_mutex_t lock;

		MapReduce(char * input, char *output, TMapFunction map, TReduceFunction reduce, int nreducers=2);
		~MapReduce();
		TError RunThreads(vector<string> filenames);
		TError runSuffle(int mapNum);
		TError Run(struct mapReduceFile * mapReduceStruct);

	private:
		TError Split(struct mapReduceFile * mapReduceStruct);
		TError Map(struct mapReduceFile * mapReduceStruct);
		TError Suffle(struct mapReduceFile * mapReduceStruct);
		TError Reduce();

		void CancelThreads(vector<pthread_t> &tids);
		
		inline void AddMap(PtrMap map) { Mappers.push_back(map); };
		inline void AddReduce(PtrReduce reducer) { Reducers.push_back(reducer); };
};
typedef class MapReduce TMapReduce, *PtrMapReduce;


#endif /* MAPREDUCE_H_ */
