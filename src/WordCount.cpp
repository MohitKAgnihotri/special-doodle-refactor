/* ---------------------------------------------------------------
Práctica 3
Código fuente: WordCount.cpp
Grau Informàtica
49259651E Sergio Beltrán Guerrero 
48139505E Martí Serratosa Sedó
--------------------------------------------------------------- */

#include "Types.h"
#include "MapReduce.h"

#include <sstream> 
#include <stdlib.h>
#include <dirent.h>
#include <sys/stat.h>
#include <string.h>
#include <iostream>
#include <algorithm>    // std::count

#define MAX_SIZE 8388608 // 8MB

using namespace std;
struct MapReduceInput{
	string filename;
};

vector<string> filenames;
vector<string> newFiles;

TError MapWordCount(PtrMap, TMapInputTuple tuple);
TError ReduceWordCount(PtrReduce, TReduceInputKey key, TReduceInputIterator begin, TReduceInputIterator end);
void splitSmallerFile(char * input_path); 
void splitFile(char *filename);
void removeNewFiles();

int main(int argc, char* argv[])
{
	char *input_dir, *output_dir;
	
	// Procesar argumentos.
    if (argc < 3 || argc > 4)
        error("Error in arguments: WordCount <input dir> <ouput dir> <reducers number>.\n");
    input_dir = argv[1];
    output_dir = argv[2];

    int nReducers = (argc == 4) ? atoi(argv[3]) : 2;
	// Analizaremos los ficheros y para que todos sean de tamaño < 8MB
	splitSmallerFile(input_dir);

	PtrMapReduce mapr = new TMapReduce(input_dir, output_dir, MapWordCount, ReduceWordCount, nReducers);
	if (mapr==NULL)
		error("Error new MapReduce.\n");
	
	mapr->RunThreads(filenames);

	removeNewFiles();
	exit(0);
}

// Split files larger than 8MB
void splitSmallerFile(char * input_path) {
	DIR *dir;
	struct dirent *entry;
	unsigned char isFile = 0x8;
	struct stat std;
	char filename[512];

	if ((dir = opendir(input_path)) != NULL) {
		while ((entry = readdir(dir)) != NULL) {
			if (strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0 && entry->d_type == isFile) {
				sprintf(filename, "%s/%s", input_path, entry->d_name);
				stat(filename, &std);
				if (std.st_size > MAX_SIZE) {
					splitFile(filename);
				} else {
					filenames.push_back(filename);
				}
			}
		}
	} else {
		error("Error opening input directory");
	}


}

void splitFile(char *filename) {
	std::ifstream inFile(filename); 
	int nLines = std::count(std::istreambuf_iterator<char>(inFile), std::istreambuf_iterator<char>(), '\n');
	int size = inFile.tellg();
	int nFiles = size / MAX_SIZE;
	if (size % MAX_SIZE != 0) nFiles++;
	ofstream newFile[nFiles];
	string line;
	int i = 0;
	int bytesInFile = 0;

	inFile.clear();
	inFile.seekg(0, ios::beg);
	newFile[i].open(filename + to_string(i));
	filenames.push_back(filename + to_string(i));
	newFiles.push_back(filename + to_string(i));
	while (getline(inFile, line)) {
		if (bytesInFile + line.size() + 1 > MAX_SIZE) {
			newFile[i].close();
			i++;
			bytesInFile = 0;
			newFile[i].open(filename + to_string(i));
			filenames.push_back(filename + to_string(i));
			newFiles.push_back(filename + to_string(i));
		}
		newFile[i] << line << endl;
		bytesInFile += line.size() + 1;
	}
}

void removeNewFiles() {
	for (int i = 0; i < newFiles.size(); i++) {
		remove(newFiles[i].c_str());
	}
}

// Word Count Map.
TError MapWordCount(PtrMap map, TMapInputTuple tuple)
{
	string value = tuple.getValue();

	if (debug) printf ("DEBUG::MapWordCount procesing tuple %ld->%s\n",tuple.getKey(), tuple.getValue().c_str());
		
	// Convertir todos los posibles separadores de palabras a espacios.
	for (int i=0; i<value.length(); i++)
	{
    	if (value[i] == ':' || value[i] == '.' || value[i] == ';' || value[i] == ',' || 
			value[i] == '"' || value[i] == '\'' || value[i] == '(' || value[i] == ')' || 
			value[i] == '[' || value[i] == ']' || value[i] == '?' || value[i] == '!' || 
			value[i] == '%' || value[i] == '<' || value[i] == '>' || value[i] == '-' || 
			value[i] == '_' || value[i] == '#' || value[i] == '*' || value[i] == '/')
        	value[i] = ' ';
	}

	stringstream ss;
	string temp;
	ss.str(value);
	// Emit map result (word,'1').
	while (ss >> temp) 
	    map->EmitResult(temp,1);

	return(COk);
}


// Word Count Reduce.
TError ReduceWordCount(PtrReduce reduce, TReduceInputKey key, TReduceInputIterator begin, TReduceInputIterator end)
{
	TReduceInputIterator it;
	int totalCount=0;

	if (debug) printf ("DEBUG::ReduceWordCount key %s ->",key.c_str());

	// Procesar todas los valores para esta clave.
	for (it=begin; it!=end; it++) {
		if (debug) printf (" %d",it->second);
		totalCount += it->second;
	}

	if (debug) printf (".\n");

	reduce->EmitResult(key, totalCount);

	return(COk);
}


