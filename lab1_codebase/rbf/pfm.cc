#include <iostream>
#include <string>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <stdio.h>
#include <cstdio>

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
	FILE* fn;
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
    	return -1;
    }else{
    	FILE* fn;
    	FileHandle.thefile = fopen(fileName, "r+")
    	return 0;
    }
    
}

/*  */
RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if(!file_exists(fileName)){
    	return -1;
    }else{
    	fclose(FileHandle.thefile);
    	return 0;
    }
}


FileHandle::FileHandle()
{
		*thefile = NULL;
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
}


FileHandle::~FileHandle()
{
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
	for(int i = 0; i<=pageNum; i++){
		fread(*data, PAGE_SIZE, 1, *thefile);
	}
	readPageCounter= readPageCounter + 1;
    return 0;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    return -1;
}

/*end_of_file returns true if end of file has been reached
(in this case we cant write to the file) if  it returns false 
the end of file has not been reached and we can append data*/
bool end_of_file(){
	if(fseek(thefile, 0, SEEK_END)!=SUCCESS){
		return TRUE;
	}
	return false;
}

/*This method appends a new page to the end of 
the file and writes the given data into the newly 
allocated page.*/
/*int fseek ( FILE * stream, long int offset, int origin );*/
RC FileHandle::appendPage(const void *data)
{

	if(end_of_file()!= TRUE){
		fwrite(data, 1, 4096, thefile);
		/*Should I put an fflush() method right here?*/
		return 0;

	}
	return 1;
}


unsigned FileHandle::getNumberOfPages()
{
    return -1;
}

/*This method should return the current counter values of this FileHandle in the three given variables. Here is some 
example code that gives you an idea how it will be applied.*/
RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
        return -1;
}                                                                                                                             1,17          Top
