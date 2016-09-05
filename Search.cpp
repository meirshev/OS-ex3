//
// Created by rozen1990 on 4/27/16.
//
#include "MapReduceFramework.h"
#include <iostream>
#include <string>
#include "string.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <dirent.h>
#include <unistd.h>
#include <vector>
#include <algorithm>

using namespace std;



/**
 * K1 inherits from k1base - contains paths to the given directories.
 */
class K1 : public k1Base
{
public:
    string _path;
    string _subString;
    K1(string path, string subString)
    {
        this->_path = path;
        this->_subString = subString;
    }
    bool operator < (const k1Base &other) const
    {
        return true;
    }
//    ~K1();
};

/**
 * contains paths to the given directories.
 */
class K2 : public k2Base
{
public:
    string _path;
    K2(string path)
    {
        this->_path = path;
    }
    bool operator < (const k2Base &other) const
    {

    }
};

/**
 * in the abstract manner it represents null
 */
class V1 : public v1Base { };

/**
 * abstract manner - represents the files inside the folder.
 */
class V2 : public v2Base
{
public:
    string _fileName;
    V2(string file_name)
    {
        this->_fileName = file_name;
    }
};

/**
 * abstract manner - represents the files inside the folder.
 */
class K3 : public k3Base
{
public:
    string _fileName;
    K3(string fileName)
    {
        this->_fileName = fileName;
    }
    virtual bool operator<(const k3Base &other) const override
    {
        //cout << "ok" << endl;
        K3* temp = (K3*)&other;
        int tempo = strcmp(this->_fileName.c_str(), temp->_fileName.c_str());
        //cout << this->_fileName.c_str() << temp->_fileName.c_str() << tempo << "asa"<< endl;
        if (tempo <= 0)
        {
            return true;
        }
        else
        {
            return false;
        }
//        return (this->_fileName < temp->_fileName);
    }
};


/**
 * in the abstract manner it represents null
 */
class V3 : public v3Base
{
public:
    V3()
    {

    }
};


vector<K2*> toDelete3;
vector<V2*> toDelete4;
vector<K3*> toDelete5;
vector<V3*> toDelete6;

/**
 * the implementation of the map and reduce that fits the
 * constructed k's and v's.
 */
class stringSearcher: public MapReduceBase
{
public:

    void Map(const k1Base *const key, const v1Base *const val) const
    {
        DIR *pDIR;
        struct dirent *entry;
        K1* k1 = (K1*)key;

        if (pDIR = opendir(k1->_path.c_str()))
        {
            while(entry = readdir(pDIR))
            {
                //cout << "loop6";
                if( strcmp(entry->d_name, ".") != 0 && strcmp(entry->d_name, "..") != 0 )
                {
                    string strEntry(entry->d_name);
                    if (strEntry.find(k1->_subString) != std::string::npos)
                    {
                        K2* k2 = new K2(k1->_path.c_str());
                        toDelete3.push_back(k2);
                        V2* v2 = new V2(entry->d_name);
                        toDelete4.push_back(v2);
                        Emit2(k2,v2);
                    }
                }
            }
            closedir(pDIR);
        }
    }

    void Reduce(const k2Base *const key, const V2_LIST &vals) const
    {
        vector <v2Base*> vectorList;

        for (list<v2Base*>::const_iterator it = vals.begin(); it != vals.end(); ++it)
        {
            vectorList.push_back(*it);
        }

        int i;
        for (i = 0; i < vectorList.size(); i++)
        {
            V2* temp = (V2*)vectorList[i];
            K3* k3 = new K3(temp->_fileName);
            toDelete5.push_back(k3);
            V3* v3 = new V3();
            toDelete6.push_back(v3);
            Emit3(k3,v3);
        }
    }

    ~stringSearcher()
    {
        int size1 = toDelete3.size();
        int size2 = toDelete5.size();

        for(int i = 0; i < size1; i++)
        {
            delete (toDelete3.at(i));
            delete (toDelete4.at(i));
        }

        for(int i = 0; i < size2; i++)
        {
            delete (toDelete5[i]);
            delete (toDelete6[i]);
        }
    }


};

/**
 * this function checks if a given file is valid in the manner of
 * requested
 */
bool isDir(string path)
{
    DIR* dir;
    dir = opendir(path.c_str());
    if(dir == NULL)
    {
        return false;
    }
    else
    {
        return true;
    }
}



bool k(int i, int j)
{
    return i < j;
}

/**
 * the program - uses mapreduceframework and prints the output.
 */
int main(int argc, char* argv[])
{

    if (argc < 3)
    {
        cerr << "input error" << endl;
        exit(1);
    }
    IN_ITEM temp;
    int numberOfPaths = argc;
    IN_ITEMS_LIST pathList;
    vector<K1*> toDelete1;
    vector<V1*> toDelete2;

    for (int i = 2; i < numberOfPaths; i++)
    {
        if(isDir(argv[i]))
        {
            K1* k1 = new K1(argv[i], argv[1]);
            toDelete1.push_back(k1);
            V1* v1 = new V1;
            toDelete2.push_back(v1);
            pathList.push_back(IN_ITEM(k1,v1));
        }
    }

    stringSearcher* func = new stringSearcher;

    OUT_ITEMS_LIST toPrint = runMapReduceFramework(*func, pathList, 1);

    vector <OUT_ITEM> outItemsList;
    for (list<OUT_ITEM>::const_iterator it = toPrint.begin(); it != toPrint.end(); ++it)
    {
        outItemsList.push_back(*it);
    }
    int j;
    for(j = 0; j < outItemsList.size(); j++)
    {
        K3* temp = (K3*)outItemsList[j].first;
        cout << temp->_fileName << endl;
    }

    int size1 = toDelete1.size();
    for(int i = 0; i < size1; i++)
    {
        delete toDelete1[i];
        delete toDelete2[i];
    }

    delete(func);

}

