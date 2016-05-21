
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
                uint32_t num = numOnPage(data);
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
            uint32_t num = numOnPage(data)+1; //the one is for the one about to be inserted 
            uint32_t halfnum = num/2;
            uint32_t secondhalf=num-halfnum; //same if even, not if odd
            //overwrite number and free space pointer in original page
            unsigned offset = sizeOf(uint32_t);
            memcpy(data+ offset, halfnum, sizeOf(uint32_t)); //new number is on new page
            //scan through data of old page to find cutoff point and insertion point for new data and write to new page
            uint32_t numScanned=0;
            bool insert_in_first_page=false;
            unsigned loc_in_first_page;
            bool match = false;
            //offset starts past leaf indicator, number on page, and free space offset
            unsigned offset = 3 * sizeOf(uint32_t);
            unsigned newpageoffset =offset;
                while (match == false) {
                    if (attribute.type == TypeInt) {
                        offset +=sizeOf(uint32_t);
                        int32_t currentInfo;
                        memcpy(currentInfo, data + offset, sizeOf(int32_t));
                        offset +=sizeOf(int32_t);
                        int32_t keyVal;
                        memcpy(&keyVal, key, sizeOf(int32_t));
                        if (keyVal<currentInfo) { //need to insert the newchild now
                            //should it go onto new page?
                            if (numScanned>halfnum) {
                                memcpy(newpage+newpageoffset, newChildEntry.pageNum, sizeOf(uint32_t));
                                newpageoffset += sizeOf(uint32_t);
                                memcpy(newpage+newpageoffset, newChildEntry.key, sizeOf(int32_t));
                                newpageoffset += sizeOf(int32_t);
                            }
                            else {
                                //add it to this page at end to avoid overriding data (can't push rest now because no room)
                                insert_in_first_page=true;
                                loc_in_first_page = offset;
                            }
                        }
                        else {
                            //not inserting new child
                            //check if past halfway point
                            if (numScanned>halfnum) {
                                //we need to write the data from here on to the new page
                                memcpy(newpage+newpageoffset, data+offset-sizeOf(int32_t)-sizeOf(uint32_t), sizeOf(uint32_t));
                                newpageoffset+=sizeOf(uint32_t);
                                memcpy(newpage+newpageoffset, data+offset-sizeOf(int32_t), sizeOf(int32_t));
                                newpageoffset+=sizeOf(int32_t);
                            }
                        }
                        numScanned++;
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
            //write to new page
            memcpy(newpage, (uint32_t) 1, sizeOf(uint32_t)); //page is non leaf
            offset = sizeOf(uint32_t);
            memcpy(newpage+offset, secondhalf, sizeOf(uint32_t)); //page will have second half number of entries
            offset+=sizeOf(uint32_t); 
            int newpagenum = ixfileHandle.appendPage(newpage);
            //split is done, pass newchild up 
            newChildEntry.pageNum= newpagenum;
            newChildEntry.key=firstKeyNewPage;
            if(pageNum==rootPageNumber(data)) {
                //we just split the root
                //create a new node to point to 2 split pages
                void *newroot = malloc(PAGE_SIZE);
                memcpy(newroot, 1, sizeOf(uint32_t));
                memcpy(newroot+sizeOf(uint32_t), 1, sizeOf(uint32_t));
                memcpy(newroot+offset, pageNum, sizeOf(pageNum));
                offset+=sizeOf(pageNum);
                memcpy(newroot+offset, newChildEntry, sizeOf(newChildEntry));
                uint32_t newrootnum = ixfileHandle.appendPage(newroot);
                //make the first page point to this new node
                void *newfirstpage=malloc(PAGE_SIZE);
                memcpy(newfirstpage, newrootnum, sizeOf(uint32_t));
            }
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
    uint32_t pageNum=rootPageNumber(ixfileHandle);
    RC rc = deleteRecursive(ixfileHandle, attribute, key, rid, pageNum);
    return rc;
}

RC IndexManager::deleteRecursive(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, uint32_t pageNum){
    //find where entry is 
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
        RC rc =deleteRecursive(ixfileHandle, attribute, key, rid, pageNum);
        return rc;
    }
    //in this case, we are at the leaf
    else{
        //delete it and move everything else up - lazy delete, no combining pages
        uint32_t num = numOnPage(data);
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
                        if (keyVal==currentInfo) {
                            //move the rest of the data back by the amount of the key and the pointer
                            memcpy(data+offset-sizeOf(int32_t)-sizeOf(uint32_t), data+offset, sizeOf(data+offset));
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
                        if (keyval==currentInfo) {
                             //move the rest of the data back by the amount of the key and the pointer
                            memcpy(data+offset-REAL_SIZE-sizeOf(uint32_t), data+offset, sizeOf(data+offset));
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
                        if (KeyVal.compare(currentInfo) ==0) {
                             //move the rest of the data back by the amount of the key and the pointer
                            memcpy(data+offset-sizeOf(currentInfo)-sizeOf(uint32_t), data+offset, sizeOf(data+offset));
                            match=true;
                        }
                    }
                }//end of while loop
        }
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
    //outputs to cout
    cout<<"{"<< endl;
    uint32_t root=rootPageNumber(ixfileHandle);
    printRecursive(ixfileHandle, attribute, root);
    cout<<"}"<<endl;
}

void IndexManager::printRecursive(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum){
    bool isLeaf=false;
    void *data=malloc(PAGE_SIZE);
    ixfileHandle->readPage(pageNum, data);
    isLeaf=pageIsLeaf(data);
    if (!isLeaf) { //non leaf node
        std::vector<attribute.type> keys;
        std::vector<uint32_t> children;
        unsigned offset = 3 * sizeOf(uint32_t);
        for(int i = 0; i<numOnPage(data);i++){
            if (attribute.type == TypeInt) {
                uint32_t prevPage;
                memcpy(prevPage, data+offset, sizeOf(uint32_t));
                offset +=sizeOf(uint32_t);
                int32_t currentInfo;
                memcpy(currentInfo, data + offset, sizeOf(int32_t));
                offset +=sizeOf(int32_t);
                keys.push_back(currentInfo);
                children.push_back(prevPage);
            }
            else if (attribute.type == TypeReal) {
                uint32_t prevPage;
                memcpy(prevPage, data+offset, sizeOf(uint32_t));
                offset +=sizeOf(uint32_t);
                float currentInfo;
                memcpy(currentInfo, data + offset, REAL_SIZE);
                offset +=REAL_SIZE;
                keys.push_back(currentInfo);
                children.push_back(prevPage);
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
                keys.push_back(currentInfo);
                children.push_back(prevPage);
            }
        }//end of for loop
        cout<<"keys:[";
        for (i=0; i<keys.size();i++){
            cout<<keys.at(i)<<",";
        }
        cout<<"],"<<endl;
        cout<<"children: [";
        for (i=0; i<children.size();i++){
            printRecursize(ixfileHandle, attribute, children.at(i));
        }
        cout<<"]"
        }
        else{ //leaf node
            cout<<"{ keys: [";
            unsigned offset = 4 * sizeOf(uint32_t);
            for(int i = 0; i<numOnPage(data);i++){
                int32_t currentkey;
                memcpy(currentKey, data+offset, sizeOf(int32_t));
                offset +=sizeOf(int32_t);
                RID currentRID;
                memcpy(currentRID, data + offset, sizeOf(RID));
                offset +=sizeOf(RID);
                cout<<currentkey<<":[ ("<<currentRID.pageNum<<","<<currentRID.slot<<")] }"
                children.push_back(prevPage);
            }//end of for loop
        }
}

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    void *data=malloc(PAGE_SIZE);
    uint32_t pageNum  = idm.rootPageNumber(ixfh);
    unsigned offset =  sizeOf(uint32_t);
    bool found = false;
    ixfileHandle->readPage(pageNum, data);
    while(found ! = false)
    {        
        ixfileHandle->readPage(pageNum, data);

        for(offset = sizeof(uint32_t); offset < PAGE_SIZE && found != true; offset += 3*sizeof(uint32_t))
        {
            uint32_t recKey;
            memcpy(&recKey, data+offset, sizeof(uint32_t));
            if(memcmp(recKey, key, sizeof(uint32_t)) == 0)
            {
                found = true;
                return SUCCESS;
            }
        }
    }
    return IX_EOF;
}

RC IX_ScanIterator::close()
{
    idm.closeFile(ixfh);
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

