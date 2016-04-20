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
    return -1;
}

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
    char * type_string; 
    int type_int;
    float type_real;

    for(std::vector<Attribute>recordDescriptor::iterator c = recordDescriptor.begin(); c != recordDescriptor.end(); ++c, i++){
        if(recordDescriptor.type() == TypeInt){
            if(recordDescriptor.type() != NULL){
                memcpy(&type_int, ((char*) data + offset), intSize);
                offset += intSize;
                cout << "Attribute " << recordDescriptor[i].type() << " (integer): " << type_int << endl;
                break;
            }
            else{
                cout << "Attribute " << recordDescriptor[i].type() << " (integer): NULL " << endl;
                break;
            }
        }
        else if(recordDescriptor.type() == TypeVarChar){
            if(recordDescriptor.type() != NULL){
                memcpy(&vcl, ((char*) data + offset), stringSize);
                offset += stringSize;
                
                stringVal = (char*) malloc(vcl + 1);
                memcpy((void*) stringVal, ((char*) data + offset), vcl);
                stringVal[vcl]="\0";
                cout << "Attribute " << recordDescriptor[i].type() << " (VarChar): " << stringVal << endl;
                break;
            }   
            else{
                cout << "Attribute " << recordDescriptor[i].type() << " (VarChar): NULL " << endl;
                break;
            }     

        }
        else if(recordDescriptor.type() == TypeReal){
            if(recordDescriptor.type() != NULL){
                memcpy(type_real, ((char*) data + offset), realSize);
                offset += realSize;
                cout << "Attribute: " << recordDescriptor[i].type() << "Real: " <<type_real << endl; 
                break;
            }
            else{
                cout << "Attribute: " << recordDescriptor[i].type() << "Real: NULL" << endl; 
                break;
            }    
        }
        
    }
    }
}








