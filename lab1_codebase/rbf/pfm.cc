#include <iostream>
#include <string>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <stdio.h>

#include "pfm.h"
 using namespace std; 
PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}

/* destoryer-joe*/
PagedFileManager::~PagedFileManager()
{
}


bool PagedFileManager::file_exists(string fileName){
	fn = fopen(fileName, "r");
	if(fn == NULL){
		return false;
	}else{
		fclose(fn);
		return true;
	}

}

RC PagedFileManager::createFile(const string &fileName)
{	
	FILE* openFile;
	openFile = fopen(fileName, "w");

	if(!file_exists(openFile)){
		if(openFile == NULL){
			return 1;
		}

		else{
			fclose(openFile);
			return 0;
		}
	}else{
		cout << "Error: File Already Exists"; 
		flose(openFile);
		return 0;
	}
}

/*char *thisIsaString;*/


RC PagedFileManager::destroyFile(const string fileName)
{
	if(fileName == NULL){
		cout<< "Error: File is NULL";
		return 1;
	}else{
		remove(fileName);
	}
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	/* check if fileHandle already exists for another file*/
    if(!file_exists(fileName)){
    	return 0;
    }else{
    	FILE* fn;
    	fn = fopen(fileName, "r+");
    }
    
}

/*  */
RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return -1;
}


FileHandle::FileHandle()
{
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
	dataBlock = NULL;
}

/*This method reads the page into the memory block pointed to by data. 
The page should exist. 
Note that page numbers start from 0.*/
/*size_t fread ( void * ptr, size_t size, size_t count, FILE * stream );
Reads an array of count elements, each one with a size of size bytes, 
from the stream and stores them 
in the block of memory specified by ptr.*/
RC FileHandle::readPage(PageNum pageNum, void *data)
{
	if(fread(dataBlock, ))

    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    return -1;
}


RC FileHandle::appendPage(const void *data)
{
    return -1;
}


unsigned FileHandle::getNumberOfPages()
{
    return -1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
        return -1;
}                                                                                                                             1,17          Top
