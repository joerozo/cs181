#include "rbfm.h"
#include "pfm.h"
#include <iostream>
#include <vector>
RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) {
    PagedFileManager* pfm=0;
    pfm->instance();
    return pfm->createFile(fileName);
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    PagedFileManager* pfm=0;
    pfm->instance();
    return pfm->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    PagedFileManager* pfm=0;
    pfm->instance();
    return pfm->openFile(fileName, fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    PagedFileManager* pfm=0;
    pfm->instance();
    return pfm->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    int result = -1;
    if(fileHandle.thefile == NULL)
        return result;

    int recordLength = getRecordLength(recordDescriptor, data);
    void *record = malloc(recordLength);//allocating space for record
    GetRecordFromData(recordDescriptor, data, record);

    char *page = (char*)malloc(PAGE_SIZE);// allocating space for page
    if(fileHandle.readPage(rid.pageNum, page) == 0) //getting a copy of page
    {
        if(page == NULL)
        {   
            PageStats *stats;
            Slot *slot;
            stats->numberOfSlots = 1;
            stats->freeSpaceOffset = recordLength+1;
            slot->length = recordLength;
            slot->offset = 0;
            memmove (page, record, recordLength);
            memmove (page + PAGE_SIZE -1 - sizeof(PageStats), stats, sizeof(PageStats));
            memmove (page + PAGE_SIZE -1 - sizeof(PageStats)- sizeof(Slot), slot, sizeof(Slot));

            if(fileHandle.writePage(rid.pageNum, page) == 0)
                result = 0;
            else
                result = -1;

        }
        else
        {
                        //get page stats
            PageStats stats;
            memcpy(&stats,  page + PAGE_SIZE - 1 -sizeof(PageStats), sizeof(PageStats));
            short pageoffset = stats.freeSpaceOffset;
            //update page states
            stats.freeSpaceOffset = pageoffset + recordLength +1;
            stats.numberOfSlots++;
            //create slot
            Slot newslot;
            newslot.offset = pageoffset;
            newslot.length = recordLength;
            //pointer 
            void *statsptr = &stats;
            void *slotptr = &newslot;

            memmove(page + pageoffset, record, recordLength);
            memmove(page + PAGE_SIZE -1 - sizeof(PageStats), statsptr, sizeof(PageStats));
            memmove(page + PAGE_SIZE -1 - sizeof(PageStats)-stats.numberOfSlots*sizeof(Slot),  slotptr, sizeof(Slot));
        }
    }
    if(fileHandle.writePage(rid.pageNum, page) == 0)
        result = 0;
    else
        result=-1;
    free(record);
    free(page);
    return result;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    char *page = (char*)malloc(PAGE_SIZE);// allocating space for page
    int rc=-1;
    if (fileHandle.readPage(rid.pageNum, page)) {
        PageStats stats;
        Slot slot;
        memcpy(&stats,  page + PAGE_SIZE - 1 -sizeof(PageStats), sizeof(PageStats));
        if (rid.slotNum <= stats.numberOfSlots){
            memcpy(&slot, page + PAGE_SIZE - 1 - sizeof(PageStats) - rid.slotNum*sizeof(Slot), sizeof(Slot));
            void *record = malloc(slot.length); //create a record length of slot.length 
            memcpy(record, page + slot.offset, slot.length);
            //then decode the record - what does this mean?
            rc=0;
        }
    }
    free(page);
    return rc;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    unsigned offset = 0;
    unsigned vcl;
    char* type_string; 
    int type_int;
    float type_real;
    char* stringVal;
    int i = 0;
    for(i = 0; i < recordDescriptor.size(); i++){
        if(recordDescriptor[i].type == TypeInt){
            if(recordDescriptor[i].type != NULL){
                memcpy(&type_int, ((char*) data + offset), intSize);
                offset += intSize;
                cout << "Attribute " << recordDescriptor[i].type << " (integer): " << type_int << endl;
            }
            else{
                cout << "Attribute " << recordDescriptor[i].type << " (integer): NULL " << endl;
            }
        break;
        }
        else if(recordDescriptor[i].type == TypeVarChar){
            if(recordDescriptor[i].type != NULL){
                memcpy(&vcl, ((char*) data + offset), stringSize);
                offset += stringSize;
                
                stringVal = (char*) malloc(vcl + 1);
                memcpy(stringVal, ((char*) data + offset), vcl);
                cout << "Attribute " << recordDescriptor[i].type << " (VarChar): " << stringVal << endl;
            }   
            else{
                cout << "Attribute " << recordDescriptor[i].type << " (VarChar): NULL " << endl;
            }     
        break;
        }
        else if(recordDescriptor[i].type == TypeReal){
            if(recordDescriptor[i].type != NULL){
                memcpy(&type_real, ((char*) data + offset), realSize);
                offset += realSize;
                cout << "Attribute: " << recordDescriptor[i].type << "Real: " <<type_real << endl; 
            }
            else{
                cout << "Attribute: " << recordDescriptor[i].type << "Real: NULL" << endl; 
            }    
            break;
        }
        
    }
    }

short RecordBasedFileManager::getRecordLength(const vector<Attribute> &recordDescriptor, const void *data)
{
    short length = 2*sizeof(short);//tombstone
    int offset = 0;
    int vCharLen  = 0;
    for(int i = 0; i < recordDescriptor.size(); i++)
    {
        length +=sizeof(short);//delimiter
        if(recordDescriptor[i].type == TypeInt)
        {
            offset += recordDescriptor[i].length;
            length += recordDescriptor[i].length;
        }
        else if(recordDescriptor[i].type == TypeReal) 
        {
            offset += recordDescriptor[i].length;
            length += recordDescriptor[i].length;
        }
        else if(recordDescriptor[i].type == TypeVarChar)     
        {
            memcpy(&vCharLen, data + offset, sizeof(int));
            length += vCharLen;
            offset += vCharLen + sizeof(int);// length of char and the delimiter
        }  
    }
    return length;
}

RC RecordBasedFileManager::GetRecordFromData(const vector<Attribute> &recordDescriptor, const void *data, void *record)
{
    short dataOffset = (recordDescriptor.size() + 2)*sizeof(short);
    short recordOffset = 0;
    int vCharLen  = 0;
    int i =0;
    *((short*)record) = 0;// clear record

    for(int i = 0; i < recordDescriptor.size(); i++)
    {
        if(recordDescriptor[i].type == TypeInt)
        {
            memcpy((char*)record + recordOffset, (char*)data+ dataOffset, sizeof(int));
            dataOffset   += sizeof(int);
            recordOffset += sizeof(int);
        }
        else if(recordDescriptor[i].type == TypeReal) 
        {
            memcpy((char*)record + recordOffset, (char*)data+ dataOffset, sizeof(float));
            dataOffset   += sizeof(float);
            recordOffset += sizeof(float);
        }
        else if(recordDescriptor[i].type == TypeVarChar)     
        {
            memcpy((char*)record + recordOffset, (char*)data+ dataOffset, sizeof(int));
            dataOffset += sizeof(int);
            memcpy((char*)record + recordOffset, (char*)data + dataOffset, vCharLen);
            dataOffset += vCharLen;
            recordOffset += vCharLen + sizeof(int);// length of char and the delimiter
        }  
    }

    *((short*)record + i +1) = recordOffset;//size of data

    return 0;
}