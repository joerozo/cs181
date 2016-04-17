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

PagedFileManager::~PagedFileManager()
{
}


bool PagedFileManager::file_exists(string fileName){
	FILE* fn;
	fn = fopen(fileName.c_str(), "r");
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
	openFile = fopen(fileName.c_str(), "wt");

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
		fclose(openFile);
		return 0;
	}
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	if(!file_exists(fileName)){
		cout<< "Error: File does not exist";
		return -1;
	}else{
		remove(fileName);
		return 0;
	}
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	/* check if fileHandle already exists for another file*/
    if(!file_exists(fileName)){
    	return -1;
    }else{
    	FILE* fn;
    	FileHandle.thefile = fopen(fileName.c_str(), "r+")
    	return 0;
    }
    
}

/*  */
RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    if(!file_exists(fileName)){
    	return -1;
    }else{
    	fclose(fileHandle->thefile);
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
	size_t result;
	int rc =0;
	//Checking that pageNum
	if(pageNum >= getNumberOfPages)
		rc = -1;		

	//finding location in file
	if(fseek(thefile, pageNum*PAGE_SIZE, SEEK_SET) != 0)
		rc = -1;	

	//Writing to file
	if(rc == 0)
	{
		result = fwrite(data, 1, PAGE_SIZE, thefile); //Writing to file
		
		//Checking if data got written to the file
		rc = result == PAGE_SIZE ? 0:-1; // this is much easier
		/*if(result == PAGE_SIZE)
			rc = 0;
		else
			rc = -1;*/
	}

	if(rc ==0)
		writePageCounter++;
	return rc;
}

/*end_of_file returns true if end of file has been reached
(in this case we cant write to the file) if  it returns false 
the end of file has not been reached and we can append data*/
bool FileHandle::end_of_file(){
	if(fseek(thefile, 0, SEEK_END)==0){
		return true;
	}
	return false;
}

/*This method appends a new page to the end of 
the file and writes the given data into the newly 
allocated page.*/
/*int fseek ( FILE * stream, long int offset, int origin );*/
RC FileHandle::appendPage(const void *data)
{
	size_t result;
	int rc =-1;

	if(end_of_file() == true){
		result = fwrite(data, 1, PAGE_SIZE, thefile);
		rc = result == PAGE_SIZE ? 0:-1; 

	}

	if(rc ==0)
		appendPageCounter++;
	return rc;
}


unsigned FileHandle::getNumberOfPages()
{
	fseek(thefile, 0, SEEK_END);
	long filesize = ftell(thefile);
    return (unsigned)(filesize/PAGE_SIZE);
}

/*This method should return the current counter values of this FileHandle in the three given variables. Here is some 
example code that gives you an idea how it will be applied.*/
RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	readPageCounter = readPageCount;
    writePageCounter = writePageCount;
    appendPageCounter = appendPageCount;
    return 0;
}                                                                                                                             1,17          Top
