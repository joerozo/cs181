
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
    void *newChildEntry;
    RC rc = insertRecursive(ixfileHandle, attribute, key, rid, newChildEntry, pageNum);

    
        ixfileHandle->readPage(pageNum, data);
        isLeaf=pageIsLeaf(data);
    //by end of while(isLeaf == false) loop, pageNum is the leaf where the <key, rid> pair should go

    return rc;
}

RC IndexManager::insertRecursive(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, childEntry *newChildEntry, uint32_t pageNum) {
    bool isLeaf=false;
    void *data=malloc(PAGE_SIZE);
    ixfileHandle->readPage(pageNum, data);
    isLeaf=pageIsLeaf(data);
    if (!isLeaf) {
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
                float keyval;
                memcpy(&keyval, key, REAL_SIZE);
                if (keyval<currentInfo) {
                    pageNum=prevPage;
                    match=true;
                }
            }
            else { //type is varchar
                uint32_t prevPage;
                memcpy(prevPage, data+offset, sizeOf(uint32_t));
                offset +=sizeOf(uint32_t);
                uint32_t currentInfoSize;
                string currentInfo;
                memcpy(currentInfoSize, data + offset, sizeOf(int32_t));
                offset +=sizeOf(int32_t);
                memcpy(currentInfo, data+offset, currentInfoSize);
                offset+=currentInfoSize;
                string KeyVal=malloc(sizeOf(key));
                memcpy(&keyval, key, sizeOf(key));
                if (KeyVal.compare(currentInfo) >0) {
                    pageNum=prevPage;
                    match=true;
                }
            }
        }
        //by end of while (match == false) loop, pageNum is the next level down where the <key, rid> pair should go
        RC rc =insertRecursive(ixfileHandle, attribute, key, rid, newChildEntry, pageNum);
        if (!newChildEntry) {
            return rc;
        }
        else{
            //child was split, must insert new child into current page
            if (PAGE_SIZE-getFreeSpacePointer(data) >= sizeOf(newChildEntry.key) + sizeOf(uint32_t)) {
                //if here, there is room on current page for this entry. find spot to insert.
                num = numOnPage(data);
                bool match = false;
                //offset starts past leaf indicator, number on page, and free space offset
                unsigned offset = 3 * sizeOf(uint32_t);
                while (match == false) {
                    if (attribute.type == TypeInt) {
                        offset +=sizeOf(uint32_t);
                        int32_t currentInfo;
                        memcpy(currentInfo, data + offset, sizeOf(int32_t));
                        offset +=sizeOf(int32_t);
                        int32_t keyVal;
                        memcpy(&keyVal, key, sizeOf(int32_t));
                        if (keyVal<currentInfo) {
                            //first, move the rest of the data forward by the amount of the key and the pointer
                            memcpy(data+offset+sizeOf(int32_t)+sizeOf(uint32_t), data+offset-sizeOf(uint32_t)-sizeOf(int32_t), sizeOf(data+offset-sizeOf(uint32_t)-sizeOf(int32_t)));
                            //second, copy the information into data
                            memcpy(data+offset, newChildEntry.pageNum, sizeOf(uint32_t));
                            offset+=sizeOf(uint32_t);
                            memcpy(data+offset, newChildEntry.key, sizeOf(int32_t));
                            match=true;
                        }
                    }
                    else if (attribute.type == TypeReal) {
                        offset +=sizeOf(uint32_t);
                        float currentInfo;
                        memcpy(currentInfo, data + offset, REAL_SIZE);
                        offset +=REAL_SIZE;
                        float keyval;
                        memcpy(&keyval, key, REAL_SIZE);
                        if (keyval<currentInfo) {
                            //first, move the rest of the data forward by the amount of the key and the pointer
                            memcpy(data+offset+REAL_SIZE+sizeOf(uint32_t), data+offset-sizeOf(uint32_t)-REAL_SIZE, sizeOf(data+offset-sizeOf(uint32_t)-REAL_SIZE));
                            //second, copy the information into data
                            memcpy(data+offset, newChildEntry.pageNum, sizeOf(uint32_t));
                            offset+=sizeOf(uint32_t);
                            memcpy(data+offset, newChildEntry.key, REAL_SIZE);
                            match=true;
                        }
                    }
                    else { //type is varchar
                        offset +=sizeOf(uint32_t);
                        uint32_t currentInfoSize;
                        string currentInfo;
                        memcpy(currentInfoSize, data + offset, sizeOf(int32_t));
                        offset +=sizeOf(int32_t);
                        memcpy(currentInfo, data+offset, currentInfoSize);
                        offset+=currentInfoSize;
                        string KeyVal=malloc(sizeOf(key));
                        memcpy(&keyval, key, sizeOf(key));
                        if (KeyVal.compare(currentInfo) >0) {
                            //first, move the rest of the data forward by the amount of the key and the pointer
                            memcpy(data+offset+sizeOf(newChildEntry.key)+sizeOf(uint32_t), data+offset-sizeOf(uint32_t)-sizeOf(newChildEntry.key), sizeOf(data+offset-sizeOf(uint32_t)-sizeOf(newChildEntry.key)));
                            //second, copy the information into data
                            memcpy(data+offset, newChildEntry.pageNum, sizeOf(uint32_t));
                            offset+=sizeOf(uint32_t);
                            memcpy(data+offset, newChildEntry.key, sizeOf(newChildEntry.key));
                            match=true;
                        }
                    }
                }//end of while loop
                newChildEntry=NULL;
                return rc;
        }//end of if current page has space
        else{
            //current node does not have space, must be split
            void *newpage=malloc(PAGE_SIZE);
            
        }
    }


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
    memcpy(num, data+sizeOf(uint32_t), sizeOf(uint32_t));
    return num;
}

uint32_t IndexManager::getFreeSpacePointer(void *data) {
    uint32_t num;
    memcpy(num, data+(2*sizeOf(uint32_t)), sizeOf(uint32_t));
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

