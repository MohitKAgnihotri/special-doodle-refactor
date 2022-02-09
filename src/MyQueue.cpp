/* ---------------------------------------------------------------
Práctica 3
Código fuente: MyQueue.cpp
Grau Informàtica
49259651E Sergio Beltrán Guerrero 
48139505E Martí Serratosa Sedó
--------------------------------------------------------------- */

//#include "MyQueue.h"

//#include <queue>

#include <pthread.h>

template <class T>
MyQueue<T>::MyQueue()
{
	//	Queue = new std::queue<T>();
	pthread_rwlock_init(&rwlock, NULL);
}

//template class MyQueue<int>;