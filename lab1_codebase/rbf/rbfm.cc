#include "rbfm.h"
#include "pfm.h"
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
            PageStats *stats = (PageStats*)page[PAGE_SIZE - 1 -sizeof(PageStats)];
            short pageoffset = stats->freeSpaceOffset
            //update page states
            stats->freeSpaceOffset = pageoffset + recordLength +1;
            stats->numberOfSlots++;
            //create slot
            Slot *newslot;
            newslot->offset = pageoffset;
            newslot->length = recordLength;

            memmove(page + pageoffset, record, recordLength);
            memmove(page + PAGE_SIZE -1 - sizeof(PageStats), stats, sizeof(PageStats));
            memmove(page + PAGE_SIZE -1 - sizeof(PageStats)-stats->numberOfSlots*sizeof(Slot), slot, sizeof(Slot));


        }


    }
    free(record);
    free(page);
    return result;

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}
/*int main ()
{
  std::vector<int> myvector;
  for (int i=1; i<=5; i++) myvector.push_back(i);

  std::cout << "myvector contains:";
  for (std::vector<int>::iterator it = myvector.begin() ; it != myvector.end(); ++it)
    std::cout << ' ' << *it;
  std::cout << '\n';

  return 0;
}*/
/*
void * memcpy ( void * destination, const void * source, size_t num );
Copy block of memory
Copies the values of num bytes from the location pointed to by source directly to the memory block pointed to by destination.*/
/*#define intSize 4
#define realSize 4
#define stringSize 4*/
RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    unsigned offset = 0;
    unsigned vcl;
    char* type_string; 
    int type_int;
    float type_real;

    for(std::vector<Attribute>recordDescriptor::iterator c = recordDescriptor.begin(); c != recordDescriptor.end(); ++c, i++){
        if(recordDescriptor.type() == TypeInt){
            if(recordDescriptor.type() != NULL){
                memcpy(&type_int, ((char*) data + offset), intSize);
                offset += intSize;
                cout << "Attribute " << recordDescriptor[i].type() << " (integer): " << type_int << endl;
            }
            else{
                cout << "Attribute " << recordDescriptor[i].type() << " (integer): NULL " << endl;
            }
        break;
        }
        else if(recordDescriptor.type() == TypeVarChar){
            if(recordDescriptor.type() != NULL){
                memcpy(&vcl, ((char*) data + offset), stringSize);
                offset += stringSize;
                
                stringVal = (char*) malloc(vcl + 1);
                memcpy((void*) stringVal, ((char*) data + offset), vcl);
                stringVal[vcl]="\0";
                cout << "Attribute " << recordDescriptor[i].type() << " (VarChar): " << stringVal << endl;
            }   
            else{
                cout << "Attribute " << recordDescriptor[i].type() << " (VarChar): NULL " << endl;
            }     
        break;
        }
        else if(recordDescriptor.type() == TypeReal){
            if(recordDescriptor.type() != NULL){
                memcpy(type_real, ((char*) data + offset), realSize);
                offset += realSize;
                cout << "Attribute: " << recordDescriptor[i].type() << "Real: " <<type_real << endl; 
            }
            else{
                cout << "Attribute: " << recordDescriptor[i].type() << "Real: NULL" << endl; 
            }    
        }
        
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
        if(recordDescriptor[i].Type == TypeInt)
        {
            offset += recordDescriptor[i];
            length += recordDescriptor[i];
        }
        else if(recordDescriptor[i].Type == TypeReal) 
        {
            offset += recordDescriptor[i];
            length += recordDescriptor[i];
        }
        else if(recordDescriptor[i].Type == TypeVarChar)     
        {
            memcpy(&vCharLen, (*char)data + offset, sizeof(int));
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
        if(recordDescriptor[i].Type == TypeInt)
        {
            memcpy((char*)record + recordOffset, (char*)data+ dataOffset, sizeof(int));
            dataOffset   += sizeof(int);
            recordOffset += sizeof(int);
        }
        else if(recordDescriptor[i].Type == TypeReal) 
        {
            memcpy((char*)record + recordOffset, (char*)data+ dataOffset, sizeof(float));
            dataOffset   += sizeof(float);
            recordOffset += sizeof(float);
        }
        else if(recordDescriptor[i].Type == TypeVarChar)     
        {
            memcpy((char*)record + recordOffset, (char*)data+ dataOffset, sizeof(int));
            dataOffset += sizeof(int);
            memcpy(recordOffset, (*char)data + dataOffset, vCharLen);
            dataOffset += vCharLen;
            recordOffset += vCharLen + sizeof(int);// length of char and the delimiter
        }  
    }

    *((short*)record + i +1) = recordOffset;//size of data

    return 0;
}