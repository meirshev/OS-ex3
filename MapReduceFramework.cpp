//
// Created by rozen1990 on 4/30/16.
//
#include <sys/time.h>
#include "MapReduceFramework.h"
#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <pthread.h>
#include <iostream>
#include <mutex>
#include <map>
#include <algorithm>
#include <signal.h>
#include <fstream>
//  #include "Search.cpp"




#define LOGFILE	".MapReduceFramework.log"
#define CHUNK_SIZE 10
int plusCounter = 0;
int minusConter = 0;
using namespace std;

typedef std::pair<k2Base*, v2Base*> TRANSITION_ITEM;



struct cmpK2 {
    bool operator()(const k2Base* a, const k2Base* b) const {
        return *a < *b;
    }
};

/**
 * args given to the execmap threads
 */
struct execMapStruct
{

    //~execMapStruct();
    map<pthread_t,pthread_mutex_t>& threadsMutexes;
    int first;
    int whoIsRunning;
    vector <IN_ITEM>& TaskQueue;
    vector <bool>& isDone;
    MapReduceBase& mapReduce;
    map<pthread_t,vector<TRANSITION_ITEM>>& mapOutput;
    map<pthread_t, pthread_mutex_t>& mapMutexes;
    execMapStruct(map<pthread_t,pthread_mutex_t>& a1, int a2, int a3, vector <IN_ITEM>& a4, vector <bool>& a5, MapReduceBase& a6, map<pthread_t,vector<TRANSITION_ITEM>>& a7, map<pthread_t, pthread_mutex_t>& a8) :
            threadsMutexes(a1), first(a2), whoIsRunning(a3), TaskQueue(a4), isDone(a5), mapReduce(a6), mapOutput(a7), mapMutexes(a8){};
};


/**
 * args given to the reduce threads
 */
struct execReduceStruct
{
    //~execReduceStruct();
    execReduceStruct(vector<k2Base*>& a1, map<k2Base*, V2_LIST, cmpK2>& a2, int a3, MapReduceBase& a4, vector <bool>& a5, int a6) :
    k2Keys(a1), shuffleOutput(a2), firstElement(a3), mapReduce(a4), isDoneReduce(a5), whoIsRunning(a6){};


    vector<k2Base*>& k2Keys;
    map<k2Base*,  V2_LIST, cmpK2>& shuffleOutput;
    int firstElement;
    MapReduceBase& mapReduce;
    vector <bool>& isDoneReduce;
    int whoIsRunning;

};

/**
 * args given to the shuffle thread
 */
struct shuffleStruct
{
    int numOfThreads;
    pthread_t* threads;
};




// kjsnacxkj jdskcnsc       sldkcmldsc     dslckmlskmc       lsdkmclkmdcs
ofstream logFile;

pthread_cond_t cond = PTHREAD_COND_INITIALIZER;

int amountOfWork = 0; // in units of chunks
bool execMapDone = false;
//map<k2Base*, vector<v2Base*>> shuffleOutput;
map<k2Base*, V2_LIST, cmpK2> shuffleOutput;
map<pthread_t,vector<TRANSITION_ITEM>> mapOutput;
map<pthread_t,vector<OUT_ITEM>> reduceOutput;
map<pthread_t,pthread_mutex_t> threadsMutex;
pthread_mutex_t em_map_mutex;
vector<pthread_t> eMapThreads;

int execMapCounter = 0;
int execMapNeeded;

int reduceCount = 0;
int reduceNeeded;

bool mapInitialized = false;


typedef std::list<int>::iterator listIterator;
typedef std::map<k2Base*,V2_LIST, cmpK2>::iterator it_type;
typedef std::map<pthread_t, vector<OUT_ITEM>>::iterator it_type2;
typedef std::map<pthread_t,vector<TRANSITION_ITEM>>::iterator it_type3;
typedef std::map<pthread_t,pthread_mutex_t>::iterator it_type4;

pthread_mutex_t reduceMutex;


pthread_t mainID;

/**
 * this function writes to the log the current time.
 */
void writeTime()
{
    time_t t = time(0);   // get time now
    struct tm * now = localtime( & t );
    logFile <<"[" << now->tm_mday << '.'
    << (now->tm_mon + 1) << '.'
    <<  (now->tm_year + 1900) << " " << now->tm_hour << ":" << now->tm_min << ":" << now->tm_sec << "]\n";

}


/**
 * this function is explined in the pdf
 */
void Emit3 (k3Base* k3, v3Base* v3)
{
    reduceOutput[pthread_self()].push_back(OUT_ITEM(k3, v3));
}

/**
 * the execreduce threads run this function
 */
void* execReduce(void* args)
{



    pthread_mutex_lock(&reduceMutex);
    pthread_mutex_unlock(&reduceMutex);


    execReduceStruct* data = (execReduceStruct*)args;
    int i = data->firstElement;
    int j;
    if(data->k2Keys.size() - data->firstElement < 10)
    {
        j = data->k2Keys.size();
    }
    else
    {
        j = data->firstElement + 10;
    }

    unsigned int k;

    for (; i < j; i++)
    {

        data->mapReduce.Reduce(data->k2Keys[i], data->shuffleOutput[data->k2Keys[i]]);

    }

    data->isDoneReduce[data->whoIsRunning] = true;
    //cout << "execReduce" << reduceCount << "     " << reduceNeeded <<  endl;
    reduceCount++;
    pthread_exit(NULL);
}

/**
 * the shufflethread runs this function.
 */
void* shuffle(void* args)
{
    shuffleStruct* data = (shuffleStruct*)args;




    struct timespec timeout;
    struct timeval now;

    unsigned int vecSize;

    pthread_mutex_t nothing;
    pthread_mutex_init(&nothing, NULL);

    int counter = 0;

    if(mapInitialized == false)
    {
        pthread_mutex_lock(&em_map_mutex);
        pthread_mutex_unlock(&em_map_mutex);
    }



    while(true)
    {

        timeout.tv_sec = now.tv_sec + 0.01;
        timeout.tv_nsec = now.tv_usec;

        pthread_cond_timedwait(&cond, &nothing, &timeout);


        for(int k = 0; k < data->numOfThreads; k++)
        {
            pthread_mutex_lock(&threadsMutex[eMapThreads[k]]);

            if(mapOutput[eMapThreads[k]].size() > 0)
            {

                vecSize = mapOutput[eMapThreads[k]].size();

                unsigned int i;
                for(i = 0; i < mapOutput[eMapThreads[k]].size(); i++)
                {

                    shuffleOutput[(mapOutput[eMapThreads[k]][i].first)].push_back(mapOutput[eMapThreads[k]][i].second);
                }
                for(i = 0; i < vecSize; i++)
                {
                    mapOutput[eMapThreads[k]].pop_back();
                }


            }
            pthread_mutex_unlock(&threadsMutex[eMapThreads[k]]);

        }


        if(execMapDone)
        {

            for(int t = 0; t < data->numOfThreads; t++)
            {
                pthread_mutex_lock(&threadsMutex[eMapThreads[t]]);
                if(mapOutput[eMapThreads[t]].size() == 0)
                {
                    counter++;
                }
                pthread_mutex_unlock(&threadsMutex[eMapThreads[t]]);
            }

            if(counter == data->numOfThreads)
            {

                break;
            }
            else
            {

                counter = 0;
            }
        }


    }


}
bool m = true;

/**
 * this function is explained in the pdf.
 */
void Emit2 (k2Base* k2, v2Base* v2)
{

    mapOutput[pthread_self()].push_back(TRANSITION_ITEM(k2, v2));
    //cout << "vec size   " << mapOutput[pthread_self()].size() << endl;
}

/**
 * the execmapthreads run this function.
 */
void* ExecMap(void* args)
{


    if(mapInitialized == false)
    {
        pthread_mutex_lock(&em_map_mutex);
        pthread_mutex_unlock(&em_map_mutex);
    }

    execMapStruct *data = (execMapStruct*) args;

    pthread_mutex_lock(&(data->threadsMutexes[pthread_self()]));
    int j;

    int i = data->first;


    if (data->TaskQueue.size() - data->first < 10)
    {
        j = data->TaskQueue.size();
    }
    else
    {
        j = data->first + 10;
    }

    for (; i < j; i++)
    {
        data->mapReduce.Map(data->TaskQueue[i].first, data->TaskQueue[i].second);
    }

    plusCounter++;
    data->isDone[data->whoIsRunning] = true;
    pthread_cond_signal(&cond);

    //cout << "execmap     " << execMapCounter << "    " << execMapNeeded << endl;
    execMapCounter++;

    pthread_mutex_unlock(&(data->threadsMutexes[pthread_self()]));

    pthread_exit(NULL);
}


bool comparator(OUT_ITEM& OBJ_ONE ,OUT_ITEM& OBJ_TWO)
{
    return  (*OBJ_ONE.first < *OBJ_TWO.first);
}


/**
 * the function explained in the pdf.
 */
OUT_ITEMS_LIST runMapReduceFramework(MapReduceBase& mapReduce, IN_ITEMS_LIST& itemsList, int multiThreadLevel)
{

    logFile.open (LOGFILE);

    logFile << "runMapReduceFramework started with ";
    logFile << multiThreadLevel;
    logFile << " threads\n";

    map<pthread_t, pthread_mutex_t> safeMap;
    pthread_mutex_init(&em_map_mutex,NULL);
    execMapNeeded = itemsList.size();
    if ((execMapNeeded % CHUNK_SIZE) != 0)
    {
        execMapNeeded = ((execMapNeeded / CHUNK_SIZE) + 1);
    }
    else
    {
        execMapNeeded = (execMapNeeded / CHUNK_SIZE);
    }

    mainID = pthread_self();
    vector <IN_ITEM> TaskQueue;
    vector <bool> isDone(multiThreadLevel, true);


    for (list<IN_ITEM>::const_iterator it = itemsList.begin(); it != itemsList.end(); ++it)
    {
        TaskQueue.push_back(*it);
    }

    for(it_type4 iterator4 = threadsMutex.begin(); iterator4 != threadsMutex.end(); iterator4++)
    {
        pthread_mutex_init(&threadsMutex[iterator4->first],NULL);
    }

    pthread_t shuffleThread;
    pthread_t execMapThreads[multiThreadLevel] = {0};
    shuffleStruct shuffleArgs = {multiThreadLevel, execMapThreads};
    pthread_create(&shuffleThread, NULL,shuffle, (void*)&shuffleArgs);

    for(int b = 0; b < multiThreadLevel; b++)
    {
        pthread_t temp;
        eMapThreads.push_back(temp);
    }

    unsigned int index_mutex = 0;
    int counter = 0;

    struct timeval t1, t2;
    //double mapShuffleTime;


    vector<execMapStruct*> toDelete;

    pthread_mutex_lock(&em_map_mutex);

    pthread_create(&shuffleThread, NULL,shuffle, (void*)&shuffleArgs);
    gettimeofday(&t1, NULL);
    while (index_mutex < itemsList.size()) //if there are threads that free
    {

        if (isDone[counter])
        {
            if(mapInitialized)
            {
                pthread_join(eMapThreads[counter], NULL);
                logFile << "Thread ExecMap terminated ";
                writeTime();

            }


            execMapStruct* temp = new execMapStruct(threadsMutex, index_mutex, counter, TaskQueue, isDone, mapReduce, mapOutput, safeMap);
            toDelete.push_back(temp);
            if (pthread_create(&eMapThreads[counter], NULL,ExecMap, (void*)temp) < 0)
            {
                cout << "error";
                exit(0);
            }

            logFile << "Thread ExecMap created ";
            writeTime();

            isDone[counter] = false;

            if(mapInitialized == false)
            {
                threadsMutex[eMapThreads[counter]];

                mapOutput[eMapThreads[counter]];
            }

            index_mutex = index_mutex + 10;

        }

        counter++;

        if(counter == multiThreadLevel && mapInitialized == false)
        {
            mapInitialized = true;
            pthread_mutex_unlock(&em_map_mutex);
        }

        if(counter == multiThreadLevel)
        {
            counter = 0;
        }
    }

    while(execMapCounter < execMapNeeded)
    {

    }
    execMapDone = true;

    pthread_join(shuffleThread, NULL);// wait until shuffle finishes before continuing to reduce



    gettimeofday(&t2, NULL);
    long mapShuffleTime = ((t2.tv_sec*1e6 + t2.tv_usec) - (t1.tv_sec*1e6 + t1.tv_usec))/1000;


    logFile << "Map and Shuffle tool ";
    logFile << mapShuffleTime;
    logFile << " ns\n";

    unsigned int reduceIndex = 0;
    unsigned int reduceCounter = 0;
    OUT_ITEMS_LIST  finalOutput;
    pthread_t execReduceThreads[multiThreadLevel] = {0};
    vector<k2Base*> k2Keys;
    vector <bool> isDoneReduce(multiThreadLevel, true);
    bool reduceMapInitialized = false;


    pthread_mutex_init(&reduceMutex,NULL);



    int f = 0;
    for(it_type iterator = shuffleOutput.begin(); iterator != shuffleOutput.end(); iterator++)
    {

        k2Keys.push_back(iterator->first);
    }

    reduceNeeded = k2Keys.size();
    if ((reduceNeeded % CHUNK_SIZE) != 0)
    {
        reduceNeeded = ((reduceNeeded / CHUNK_SIZE) + 1);
    }
    else
    {
        reduceNeeded = (reduceNeeded / CHUNK_SIZE);
    }

    struct timeval r1;
    struct timeval r2;


    int howMany = (k2Keys.size()) / 10;
    gettimeofday(&r1, NULL);

    pthread_mutex_lock(&reduceMutex);
    vector<execReduceStruct*> toDelete2;

    while(reduceIndex < k2Keys.size())
    {

        if(isDoneReduce[reduceCounter])
        {

            if(reduceMapInitialized)
            {

                pthread_join(execReduceThreads[counter], NULL);
                logFile << "Thread ExecReduce terminated ";
                writeTime();
            }

            execReduceStruct* reduceArgs = new execReduceStruct(k2Keys, shuffleOutput, reduceIndex, mapReduce, isDoneReduce, reduceCounter);
            toDelete2.push_back(reduceArgs);
            if (pthread_create(&execReduceThreads[reduceCounter], NULL,execReduce, (void*)reduceArgs) < 0)
            {

                cout << "error";
                exit(0);
            }

            logFile << "Thread ExecReduce created ";
            writeTime();

            isDoneReduce[reduceCounter] = false;
            if(reduceMapInitialized == false)
            {

                reduceOutput[execReduceThreads[reduceCounter]]; // there is here a cnstant initialization - while it is not needed.

            }


            reduceIndex = reduceIndex + 10;

        }

        reduceCounter++;
        if(!reduceMapInitialized && reduceCounter >= howMany && howMany < 100)
        {

            reduceMapInitialized = true;
            pthread_mutex_unlock(&reduceMutex);
        }

        if(!reduceMapInitialized && reduceCounter == multiThreadLevel && howMany > 99)
        {

            reduceMapInitialized = true;
            pthread_mutex_unlock(&reduceMutex);
        }

        if(reduceCounter == multiThreadLevel)
        {

            reduceCounter = 0;
        }
    }



    while(reduceCount < reduceNeeded)
    {
        //cout << reduceCount << "     " << reduceNeeded << endl;
    }





    vector<OUT_ITEM> unsortedOutput;


    unsigned int b;
    for(it_type2 iterator = reduceOutput.begin(); iterator != reduceOutput.end(); iterator++) {

        //cout << reduceOutput[iterator->first].size() << "vec sizes"<< endl;
        for(b = 0; b < reduceOutput[iterator->first].size(); b++)
        {
            unsortedOutput.push_back(reduceOutput[iterator->first].at(b));
        }
        // Repeat if you also want to iterate through the second map.
    }

    //sort(unsortedOutput.begin(), unsortedOutput.end(), comparator);

    for(b = 0; b < unsortedOutput.size(); b++)
    {
        finalOutput.push_back(unsortedOutput.at(b));
    }

    finalOutput.sort(comparator);

    int size1 = toDelete.size();
    int size2 = toDelete2.size();


    for(int q = 0; q < size1;q++ )
    {
        delete toDelete[q];
    }

    for(int q = 0; q < size2;q++ )
    {
        delete toDelete2[q];
    }

    isDoneReduce.clear();
    shuffleOutput.clear();
    mapOutput.clear();
    reduceOutput.clear();
    threadsMutex.clear();
    eMapThreads.clear();
    execMapCounter = 0;
    reduceCount = 0;

    mapInitialized = false;
    execMapDone = false;

    gettimeofday(&r2, NULL);
    long reduceTime = ((r2.tv_sec*1e6 + r2.tv_usec) - (r1.tv_sec*1e6 + r1.tv_usec))/1000;

    logFile << "Reduce took " ;
    logFile << reduceTime;
    logFile << "ns\n";
    logFile << "runMapReduceFramework finished\n";

    logFile.close();
    return finalOutput;
}