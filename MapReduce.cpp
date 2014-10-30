//============================================================================
// Name        : lab_2.cpp
// Author      : Brenton Chasse, Alex Nichols
// Version     :
// Copyright   : 2014
// Description : MapReduce for CS377
//============================================================================



/*
 *Reducer's buffer needs to be such that we can determine if it is full atomically
 *
 *
 */


#include <iostream>
#include <sstream>
#include <fstream>
#include <unordered_map>
#include <queue>

using namespace std;

ReducerThread* reducerList;

struct Data {
    string word;
    string fileName;
    int lineNum;
};

struct InvertedIndex {
    unordered_map<string,string> map = unordered_map<string, string>();
    void add(Data data) {
        stringstream os;
        os << map[data.word] << "(" << data.fileName << ": " << data.lineNum << ") ";
        map[data.word] = string(os.str().c_str());
        
        // delete data;//every thing should now die b/c scope and stuffs
    }
};

struct Buffer {
    queue<Data> buffer;
    
    bool isFull(pthread_mutex_t* fullLock){
        bool returnVal = false;
        pthread_mutex_lock(fullLock);
        if(buffer.size() == 10)
            returnVal = true;
        pthread_mutex_unlock(fullLock);
        return returnVal;
    }
    
    bool isEmpty(pthread_mutex_t* emptyLock){
        bool returnVal = false;
        pthread_mutex_lock(emptyLock);
        if(buffer.empty())
            returnVal = true;
        pthread_mutex_lock(emptyLock);
        return returnVal;
    }
    
    void add(Data dataItem){
        buffer.push(dataItem);
        return;
    }
    
    Data remove(){
        Data returnData = buffer.front();
        buffer.pop();
        return returnData;
    }
};

static int hashFunction(string word, int mod) {
    hash<string> hash_funct;
    size_t str_hash = hash_funct(word);
    return ((int) ((str_hash % mod) + mod) % mod);
};

typedef struct {
    Buffer buffer;
    InvertedIndex invertedIndex;
    pthread_mutex_t bufferLock;
    pthread_mutex_t invertedIndexLock;
    pthread_mutex_t fullLock;
    pthread_mutex_t emptyLock;
    pthread_cond_t full;
    pthread_cond_t empty;
    pthread_t thisThread;
} ReducerThread;

class Reducers {
private:
public:
    pthread_t** reducerThreadList;
    
    
    Reducers(int numReducers){
        for(int i = 0; i < numReducers; i++) {
            reducerList[i] = new ReducerThread;
            pthread_mutex_init(reducerList[i].bufferLock, NULL);
            pthread_mutex_init(reducerList[i].fullLock, NULL);
            reducerThreadList[i] = new pthread_t;
            pthread_create(reducerList[i].thisThread, NULL, reduce, i);
        }
    }
    
    static void* reduce(void* reducerNumber){
        ReducerThread thisReducer = reducerList[*(int*)reducerNumber];
        Data myData;
        printf("Started Consumer %i\n", reducerNumber);
        //TODO: while there are still astive mappers or the buffer isn't empty
        while(true){
            pthread_mutex_lock(&thisReducer.bufferLock);
            
            while(thisReducer.buffer.isEmpty(&thisReducer.emptyLock)){
                pthread_cond_wait(&thisReducer.empty, &thisReducer.bufferLock);
            }
            myData = thisReducer.buffer.remove();
            
            pthread_cond_signal(&thisReducer.full);
            pthread_mutex_unlock(&thisReducer.bufferLock);
            
            pthread_mutex_lock(&thisReducer.invertedIndexLock);
            thisReducer.invertedIndex.add(myData);
            pthread_mutex_unlock(&thisReducer.invertedIndexLock);
        }
    }
};

class Mappers {
private:
    pthread_t** mapperThreadList;
    static string* fileList;
    //we need a list of all of the reducers (reducerList)
public:
    
    Mappers(int numMappers) {
        for(int i = 0; i < numMappers; i++) {
            stringstream os;
            os << "foo" << i << ".txt";
            fileList[i] = static_cast<string>(os.str());
            mapperThreadList[i] = new pthread_t;
            pthread_create(mapperThreadList[i], NULL, map, &i);
        }
    }
    
    static void* map(void* mapperNumber){
        int lineNumber = 1;
        string word;
        ifstream input((char*) fileList[*(int*)mapperNumber].c_str());
        if(input.is_open()){
            while(getline(input, word)){
                Data wordData;
                wordData.word = word;
                wordData.lineNum = lineNumber;
                wordData.fileName = fileList[*(int*)mapperNumber];
                static int wordHash = hashFunction(word, *(int*)mapperNumber);
                Reducers myReducer = reducerList[wordHash];
                
                pthread_mutex_lock(&myReducer.bufferLock);
                
                while(myReducer.buffer.isFull(&myReducer.fullLock)){
                    pthread_cond_wait(&myReducer.full, &myReducer.bufferLock);
                }
                
                myReducer.buffer.add(wordData);
                
                pthread_cond_signal(&myReducer.empty);
                pthread_mutex_unlock(&myReducer.bufferLock);
                
                lineNumber++;
            }
        }
        
    }
};

//TODO: wait for all threads to join eachother ( Brendan said something about this in discussion)
//TODO: print out the contents of the inverted index
//TODO: add sOoOoOoOo many comments to this lovely lady

//TODO: This whole main thingy needs to be rethought
int main(int argc, char *argv[]) {
    //Ensure correct number of arguments
    /*
     if (argc != 3) {
     cout << "Error - Please enter two arguments: <number of mappers> <number of reducers>" << endl;
     return -1;
     }
     
     //Set number of threads to be created
     int numMappers  = strtol(argv[1], NULL, 10);
     int numReducers = strtol(argv[2], NULL, 10);
     */
    
    int numMappers  = 10;
    int numReducers = 40;
    
    mapperThreadList = new pthread_t[numMappers];
    
    //must instantiate reducers before consumers
    
    pthread_mutex_t finished;
    pthread_mutex_init(&finished, NULL);
    
    Reducers reducerList = Reducers(numReducers, &finished);
    Mappers  mappers  = Mappers(numMappers, &reducers);
    
    //let's wait here until Reducers is finished printing out inverted Indices
    //then we can acquire the lock and return
    pthread_mutex_lock(&finished);
    
    return 0;
}