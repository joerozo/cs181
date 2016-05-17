
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
}

IndexManager::~IndexManager()
{
}

RC IndexManager::createFile(const string &fileName)
{
    unsigned char* root_page = new unsigned char[PAGE_SIZE];
    FileHandle handle;
    RC x = rbfm->createFile(fileName.c_str());
    if(x==0){
        return -1;
    }
    RC rc = openFile(fileName, handle);

    void* root = malloc(PAGE_SIZE);
    handle.writePage(0, root);



    
}

RC IndexManager::destroyFile(const string &fileName)
{
    if(_rbf_manager->destroyFile(fileName)!=SUCCESS){
        return -1;
    }
    return SUCCESS;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    if(_rbf_manager->openFile(fileName.c_str(), ixfileHandle) != SUCCESS){
        return -1;
    }
    return SUCCESS;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle){
    if (_rbf_manager->closeFile(ixfileHandle) != SUCCESS) {
        return -1;
    }
    return SUCCESS;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    uint32_t pageNum=rootPageNumber(ixfileHandle);
    //find where entry should go
    bool isLeaf=false;
    void *data=malloc//finish that;
    ixfileHandle->readPage(pageNum, data);
    isLeaf=pageIsLeaf(data);
    uint32_t num;
    while (isLeaf == false) {
        num = numOnPage(data);
        bool match = false;
        //offset starts past leaf indicator, number on page, and free space offset
        unsigned offset = 3 * sizeOf(uint32_t);
        while (match == false) {
            if (attribute.type == TypeInt) {
                uint32_t prevPage;
                memcpy(prevPage, data+offset, sizeOf(uint32_t));
                offset +=sizeOf(uint32_t);
                int32_t currentInfo;
                memcpy(currentInfo, data + offset, sizeOf(int32_t));
                offset +=sizeOf(int32_t);
                int32_t keyVal;
                memcpy(&keyVal, key, sizeOf(int32_t));
                if (keyVal<currentInfo) {
                    pageNum=prevPage;
                    match=true;
                }
            }
            else if (attribute.type == TypeReal) {
                uint32_t prevPage;
                memcpy(prevPage, data+offset, sizeOf(uint32_t));
                offset +=sizeOf(uint32_t);
                float currentInfo;
                memcpy(currentInfo, data + offset, REAL_SIZE);
                offset +=REAL_SIZE;
                uint32_t intKey = (int) key;
                if (intKey<currentInfo) {
                    pageNum=prevPage;
                    match=true;
                }
            }

            }
        }
        //by end of while (match == false) loop, pageNum is the next level down where the <key, rid> pair should go
        ixfileHandle->readPage(pageNum, data);
        isLeaf=pageIsLeaf(data);
        
    }
    //by end of while(isLeaf == false) loop, pageNum is the leaf where the <key, rid> pair should go

    //if leaf has room, insert entry
    //else, split leaf and push split up 
    //each node is a page, each pointer to a page is a page number 
    return -1;
}

bool IndexManager::pageIsLeaf(void *data) {
    uint32_t isLeaf;
    memcpy(isLeaf, data, sizeOf(uint32_t));
    //leaf pages will have first value zero, non-leaf(internal) pages will have first value one
    if (isLeaf==0) return true;
    else return false;
}

uint32_t IndexManager::numOnPage(void *data) {
    uint32_t num;
    memcpy(num, data+sizeOf(int), sizeOf(uint32_t));
    return num;
}

uint32_t IndexManager::rootPageNumber(IXFileHandle &ixfileHandle) {
    void *data;
    ixfileHandle->readPage(0, data);
    uint32_t rootNum;
    memcpy(rootNum, data, sizeOf(uint32_t));
    return rootNum;
}

RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    //find where entry is 
    //delete it 
    //lazy deletion: do not bother with post-deletion reorganization
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}

