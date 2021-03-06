#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>

#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
    // Initialize the internal PagedFileManager instance
    _pf_manager = PagedFileManager::instance();
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

RC RecordBasedFileManager::createFile(const string &fileName) 
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newRecordBasedPage(firstPageData);

    // Adds the first record based page.
    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;
    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) 
{
    return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) 
{
    return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) 
{
    return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) 
{
    // Gets the size of the record.
    unsigned recordSize = getRecordSize(recordDescriptor, data);

    // Cycles through pages looking for enough free space for the new entry.
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool pageFound = false;
    unsigned i;
    unsigned numPages = fileHandle.getNumberOfPages();
    for (i = 0; i < numPages; i++)
    {
        if (fileHandle.readPage(i, pageData))
            return RBFM_READ_FAILED;

        // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
        if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
        {
            pageFound = true;
            break;
        }
    }

    // If we can't find a page with enough space, we create a new one
    if(!pageFound)
    {
        newRecordBasedPage(pageData);
    }

    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    // Setting the return RID.
    rid.pageNum = i;
    rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
    setSlotDirectoryHeader(pageData, slotHeader);

    // Adding the record data.
    setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

    // Writing the page to disk.
    if (pageFound)
    {
        if (fileHandle.writePage(i, pageData))
            return RBFM_WRITE_FAILED;
    }
    else
    {
        if (fileHandle.appendPage(pageData))
            return RBFM_APPEND_FAILED;
    }

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) 
{
    // Retrieve the specific page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

    free(pageData);
    return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Parse the null indicator into an array
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, data, nullIndicatorSize);
    
    // We've read in the null indicator, so we can skip past it now
    unsigned offset = nullIndicatorSize;

    cout << "----" << endl;
    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        cout << setw(10) << left << recordDescriptor[i].name << ": ";
        // If the field is null, don't print it
        bool isNull = fieldIsNull(nullIndicator, i);
        if (isNull)
        {
            cout << "NULL" << endl;
            continue;
        }
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                uint32_t data_integer;
                memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
                offset += INT_SIZE;

                cout << "" << data_integer << endl;
            break;
            case TypeReal:
                float data_real;
                memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
                offset += REAL_SIZE;

                cout << "" << data_real << endl;
            break;
            case TypeVarChar:
                // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
                uint32_t varcharSize;
                memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                // Gets the actual string.
                char *data_string = (char*) malloc(varcharSize + 1);
                if (data_string == NULL)
                    return RBFM_MALLOC_FAILED;
                memcpy(data_string, ((char*) data + offset), varcharSize);

                // Adds the string terminator.
                data_string[varcharSize] = '\0';
                offset += varcharSize;

                cout << data_string << endl;
                free(data_string);
            break;
        }
    }
    cout << "----" << endl;

    return SUCCESS;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
{
    void * pageData = malloc(PAGE_SIZE);
    signed int space;
    signed int freespace;
    vector<void*> pageRecData
    vector<SlotDirectoryRecordEntry> recordEntries;
    unsigned int recordOffset = PAGE_SIZE;

    // Retrieve the specific page
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    //read all records from
    for(unsigned i =0; i < slotHeader.recordEntriesNumber; i++)
    {
        void* recData;
        // Gets the slot directory record entry data
        SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, i);
        // Retrieve the actual entry data
        getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, recData);
        //Add Data to vector
        pageRecData.push_back(recData);
        //Add Slot info to vector
        recordEntries.push_back(recordEntry);
    }    

    //Check that updated_data will fit in page and update vector data and offset
    space  = sizeof(slotHeader); 
    space += sizeof(SlotDirectoryRecordEntry) * slotHeader.recordEntriesNumber;

    for(int i = 0; i < recordEntries.size(); i++)
    {
        if(rid.slotNum == i)
        {
            pageRecData[i] = data;
            recordEntries[i].length = getRecordSize(recordDescriptor,data)
        }
        space += recordEntries[i].length;     
    }
    //calculating freespace
    freespace = PAGE_SIZE - space
    
    //Checking if there is free space
    if(freespace >= 0)
    {
        recordOffset = PAGE_SIZE;
        for(int i = 0; i < recordEntries.size(); i++)
        {
            unsigned int length = recordEntries[i].length
            recordOffset -= length;
            recordEntries[i].offset = recordOffset
            //adding record
            memcpy(page+recordEntries[i].offset, pageRecData,length);
            //slot offset
            setSlotDirectoryRecordEntry(page, i, recordEntries[i]);          
        }

        //Updating SlotDirectoryHeader
        slotHeader.freeSpaceOffset = recordOffset;
        setSlotDirectoryHeader(page, slotHeader);
        //writing to page
        if (fileHandle.writePage(i, pageData))
        {

            free(pageData);
            return RBFM_WRITE_FAILED;
        }
    }
    else
    {
        RID refRID;
        recordOffset = PAGE_SIZE;
        unsigned result = insertRecord(fileHandle, recordDescriptor, data, refRID)
        if(result != SUCCESS)
        {
            free(pageData);
            return result;
        }
        else
        {
            for(int i = 0; i < recordEntries.size(); i++)
            {
                if(i == rid.Slot)
                {
                    recordEntries[i].offset = -1;
                    recordEntries[i].length = sizeof(RID);
                    recordOffset -= recordEntries[i].length;
                    memcpy(page+recordOffset, &refRID, recordEntries[i].length);
                }
                else
                {
                    recordOffset -= recordEntries[i].length;
                    recordEntries[i].offset = recordOffset;
                    memcpy(page+recordOffset, pageRecData[i], recordEntries[i].length);
                }
            }

            //Updating SlotDirectoryHeader
            slotHeader.freeSpaceOffset = recordOffset;
            setSlotDirectoryHeader(page, slotHeader);
            //writing to page
            if (fileHandle.writePage(i, pageData))
            {

            free(pageData);
            return RBFM_WRITE_FAILED;
            }
        }
    }

    free(pageData);
    return SUCCESS;
}

RC deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid) {
    void * pageData = malloc(PAGE_SIZE);
    signed int space;
    signed int freespace;
    vector<void*> pageRecData
    vector<SlotDirectoryRecordEntry> recordEntries;
    unsigned int recordOffset = PAGE_SIZE;

    // Retrieve the specific page
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    //read all records from
    for(unsigned i =0; i < slotHeader.recordEntriesNumber; i++)
    {
        void* recData;
        // Gets the slot directory record entry data
        SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, i);
        // Retrieve the actual entry data
        getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, recData);
        //Add Data to vector
        pageRecData.push_back(recData);
        //Add Slot info to vector
        recordEntries.push_back(recordEntry);
    }    
     for(int i = 0; i < recordEntries.size(); i++)
            {
                if(i == rid.Slot)
                {
                    recordEntries[i].offset = -2;
                    recordEntries[i].length = 0;
                    recordOffset -= recordEntries[i].length;
                }
                else
                {
                    recordOffset -= recordEntries[i].length;
                    recordEntries[i].offset = recordOffset;
                    memcpy(page+recordOffset, pageRecData[i], recordEntries[i].length);
                }
            }

            //Updating SlotDirectoryHeader
            slotHeader.freeSpaceOffset = recordOffset;
            slotHeader.recordEntriesNumber--;
            setSlotDirectoryHeader(page, slotHeader);
            //writing to page
            if (fileHandle.writePage(i, pageData))
            {

            free(pageData);
            return RBFM_WRITE_FAILED;
            }
        }
    }

    free(pageData);
    return SUCCESS;
}

// Private helper methods

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    SlotDirectoryHeader slotHeader;
    slotHeader.freeSpaceOffset = PAGE_SIZE;
    slotHeader.recordEntriesNumber = 0;
    setSlotDirectoryHeader(page, slotHeader);
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
    // Getting the slot directory header.
    SlotDirectoryHeader slotHeader;
    memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
    return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
    // Setting the slot directory header.
    memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
    // Getting the slot directory entry data.
    SlotDirectoryRecordEntry recordEntry;
    memcpy  (
            &recordEntry,
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            sizeof(SlotDirectoryRecordEntry)
            );

    return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
    // Setting the slot directory entry data.
    memcpy  (
            ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
            &recordEntry,
            sizeof(SlotDirectoryRecordEntry)
            );
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page) 
{
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
    return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data) 
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Offset into *data. Start just after null indicator
    unsigned offset = nullIndicatorSize;
    // Running count of size. Initialize to size of header
    unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

    for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
    {
        // Skip null fields
        if (fieldIsNull(nullIndicator, i))
            continue;
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                size += INT_SIZE;
                offset += INT_SIZE;
            break;
            case TypeReal:
                size += REAL_SIZE;
                offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
                size += varcharSize;
                offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    return size;
}

// Calculate actual bytes for nulls-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount) 
{
    return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
    int indicatorIndex = i / CHAR_BIT;
    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
    return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
    // Read in the null indicator
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset (nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    // Points to start of record
    char *start = (char*) page + offset;

    // Offset into *data
    unsigned data_offset = nullIndicatorSize;
    // Offset into page header
    unsigned header_offset = 0;

    RecordLength len = recordDescriptor.size();
    memcpy(start + header_offset, &len, sizeof(len));
    header_offset += sizeof(len);

    memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
    header_offset += nullIndicatorSize;

    // Keeps track of the offset of each record
    // Offset is relative to the start of the record and points to the END of a field
    ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++)
    {
        if (!fieldIsNull(nullIndicator, i))
        {
            // Points to current position in *data
            char *data_start = (char*) data + data_offset;

            // Read in the data for the next column, point rec_offset to end of newly inserted data
            switch (recordDescriptor[i].type)
            {
                case TypeInt:
                    memcpy (start + rec_offset, data_start, INT_SIZE);
                    rec_offset += INT_SIZE;
                    data_offset += INT_SIZE;
                break;
                case TypeReal:
                    memcpy (start + rec_offset, data_start, REAL_SIZE);
                    rec_offset += REAL_SIZE;
                    data_offset += REAL_SIZE;
                break;
                case TypeVarChar:
                    unsigned varcharSize;
                    // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                    memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                    memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                    // We also have to account for the overhead given by that integer.
                    rec_offset += varcharSize;
                    data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
                break;
            }
        }
        // Copy offset into record header
        // Offset is relative to the start of the record and points to END of field
        memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
        header_offset += sizeof(ColumnOffset);
    }
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
// Memset null indicator as 1?
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator. The returned null indicator may be larger than
    // the null indicator in the table has had fields added to it
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
    memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write data to data.
    unsigned data_offset = nullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;
    
    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;
        
        // Grab pointer to end of this column
        ColumnOffset endPointer;
        memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

        // rec_offset keeps track of start of column, so end-start = total size
        uint32_t fieldSize = endPointer - rec_offset;

        // Special case for varchar, we must give data the size of varchar first
        if (recordDescriptor[i].type == TypeVarChar)
        {
            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
            data_offset += VARCHAR_LENGTH_SIZE;
        }
        // Next we copy bytes equal to the size of the field and increase our offsets
        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
        rec_offset += fieldSize;
        data_offset += fieldSize;
    }
}

RC RecordBasedFileManager::getAttributeType(){

}

/*
RC readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data)
Given a record descriptor, read a specific attribute of a record identified by a given rid.*/
/*(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) returns SUCCESS*/
/* types int real varchar*/
/* attributes have a .name right ???*/
RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string attributeName, void *data){
  void* record_desc = malloc(PAGE_SIZE);  
  int stringSize = 0;
  unsigned offset = 0;
  unsigned offsetType = NULL;
  if (readRecord(fileHandle, recordDescriptor, rid, record_desc) != 1){
    return -1;
    cout << "invalid entry" << endl;
  }
  /* x gets attribute of recordDescriptor[i]*/
  for(int i = 0; i < recordDescriptor.size(); i++){
    Attribute x = recordDescriptor[i];
    bool get_offset_type = true;
    /* this while loop stores the attribute type in offsetType so we know of what type the attribute is*/
    while(get_offset_type){
        if(x.type == TypeVarChar){
            offsetType = VARCHAR_LENGTH_SIZE;
            get_offset_type = false;
        } 
        else if(x.type == TypeInt){
            offsetType = TypeInt;
            get_offset_type = false;
        }else{
            offsetType = TypeReal;
            get_offset_type = false;
        }
    }

    /* if Attribute x.name == attributeName we just store the attribute (this is what we were looking for) and don't increment the offset */
    if(x.name == attributeName){
        if(offsetType ==  TypeInt){
            memcpy(data, (char*)record_desc + offsetType, INT_SIZE);
        }else if(offsetType == TypeVarChar){
            memcpy(&stringSize, (char*)record_desc + offset), VARCHAR_LENGTH_SIZE);
            memcpy(data, (char*)record_desc + offset, stringSize + VARCHAR_LENGTH_SIZE);
        }else if(offsetType == TypeReal){
            memcpy(data, (char*)record_desc + offset, REAL_SIZE);
        }
    }
    /* if Attribute x.name != attributeName we need to increment the offset by the appropriate amount and then return 
    to beginning of loop and check recordDescriptor[i++] all over but with incremented offset*/
    if(offsetType == TypeInt){
        offset += INT_SIZE;
    }else if(offsetType == TypeReal){
        offset += REAL_SIZE;
    }else if(offsetType == TypeVarChar){
        memcpy(&stringSize, (char*)record_desc + offset), VARCHAR_LENGTH_SIZE);
        offset += VARCHAR_LENGTH_SIZE;
    }
    break;
  }
  return data; 
}

RC RecordBasedFileManager::scan(FileHandle &fileHandle,
      const vector<Attribute> &recordDescriptor,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparision type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RBFM_ScanIterator &rbfm_ScanIterator) {
    //set all values of scan iterator to these given values
    rbfm_ScanIterator->fileHandle = fileHandle;
    rbfm_ScanIterator->recordDescriptor = recordDescriptor;
    rbfm_ScanIterator->conditionAttribute=conditionAttribute;
    rbfm_ScanIterator->compOp=compOp;
    rbfm_ScanIterator->value=value;
    rbfm_ScanIterator->attributeNames=attributeNames;
    rbfm_ScanIterator->currentPage=0;
    rbfm_ScanIterator->currentSlot=0;
}

 RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) { 
    int totPages = fileHandle->getNumberOfPages();
    //if we are past last page, we are done
    if (currentPage>=totPages) {
        return RBFM_EOF;
    }
    //if we are past the last record on this page, go to next page
    void *thepage = malloc(PAGE_SIZE);
    fileHandle->readPage(currentPage, thepage);
    SlotDirectoryHeader header =rbfm->getSlotDirectoryHeader(thepage);
    int totSlots = header->recordEntriesNumber;
    if (currentSlot>=totRecords) {
        currentSlot=0;
        currentPage++;
        return getNextRecord(rid, data);
    }
    void *currentAttribute = malloc(PAGE_SIZE);
    RecordBasedFileManager rbfm = RecordBasedFileManager::instance();
    //create new RID currentrid using current slot and current page
    //check if current record has been incremented or moved
    int RC=rbfm->readAttribute(fileHandle, recordDescriptor, currentrid, conditionAttribute, currentAttribute);
    if (RC=0) {
        //get attribute type of condition attribute
        AttrType currentType;
        for (Attribute attr: recordDescriptor) {
            if (attr->name == conditionAttribute) {
                currentType=attr->type;
            }
        }
        bool result;
        switch(currentType) {
            case TypeInt :
                int catt;
                catt = (int) currentAttribute;
                result = rbfm->performCompOpInt(compOp, catt, value);
                break;
            case TypeReal :
                float catt;
                catt=(float) currentAttribute;
                result = rbfm->performCompOpFloat(compOp, catt, value);
                break;
            case TypeVarChar :
                string catt;
                catt= (string) currentAttribute;
                result=rbfm->performCompOpString(compOp, catt, calue);
                break;
        }
        if(result==true) {
            //get null indicator size
            int nullIndicatorSize=rbfm->getNullIndicatorSize(attributeNames.size());
            char nullIndicator[nullIndicatorSize] = 0;
            // create offset
            int32_t offset=0;
            //put null indicator into data
            memcpy(data, &nullIndicator, nullIndicatorSize);
            offset+=nullIndicatorSize;
            //i is used to see which # attribute we're on 
            int i=0;
            for (attr: attributeNames) {
                void *thisatt;
                int RC=rbfm->readAttribute(fileHandle, recordDescriptor, currentrid, attr, thisatt);
                //check if it's not null
                if (thisatt) {
                    //get type
                    //get attribute type of current attribute
                    AttrType thisType;
                    for (Attribute allatts: recordDescriptor) {
                        if (allatts->name == attr) {
                            thisType=allatts->type;
                        }
                    }
                    //switch on type
                    switch(thisType) {
                        case TypeInt :
                            memcpy(data+offset, thisatt, sizeof(int32_t));
                            offset+=sizeof(int32_t);
                            break;
                        case TypeReal :
                            memcpy(data+offset, thisatt, sizeof(float));
                            offset+=sizeof(float);
                            break;
                        case TypeVarChar :
                            int32_t stringSize = thisatt.length();
                            memcpy(data+offset, &stringSize, sizeof(stringSize);
                            offset+=sizeof(stringSize);
                            memcpy(data+offset, thisatt.c_str(), stringSize);
                            offset+=stringSize);
                            break;
                    }
                }
                else {
                    //write nullindicator corresponding bit to be null
                    int indicatorIndex = (i+1) / CHAR_BIT;
                    int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
                    nullIndicator[indicatorIndex] |= indicatorMask;
                    // Write out null indicator
                    memcpy(data, nullIndicator, nullIndicatorSize);
                }
                i++;
            }
            return RC;
        }
        else {
            currentSlot++;
            return getNextRecord(rid, data);
        }
    }
}

 RC RBFM_ScanIterator::close() { return 0; };

 bool RecordBasedFileManager::performCompOpInt(const CompOp compOp, int currentAttribute, int value) {
    bool result = false;
    switch(compOp){
    case NO_OP  :
       result = true;
       return result;
       break; 
    case EQ_OP  :
       if (currentAttribute=value) {
        result=true;
        return result;
       }
       else return result;
       break; 
    case LT_OP  :
       if (currentAttribute<value) {
        result=true;
        return result;
       }
       else return result;
       break; 
    case LE_OP  :
       if (currentAttribute<=value) {
        result=true;
        return result;
       }
       else return result;
       break; 
    case GT_OP  :
       if (currentAttribute>value) {
        result=true;
        return result;
       }
       else return result;
       break; 
    case GE_OP  :
       if (currentAttribute>=value) {
        result=true;
        return result;
       }
       else return result;
       break;
    case NE_OP  :
       if (currentAttribute!=value) {
        result=true;
        return result;
       }
       else return result;
       break;
    }
    
 }

  bool RecordBasedFileManager::performCompOpFloat(const CompOp compOp, float currentAttribute, float value) {
    bool result = false;
    switch(compOp){
    case NO_OP  :
       result = true;
       return result;
       break; 
    case EQ_OP  :
       if (currentAttribute=value) {
        result=true;
        return result;
       }
       else return result;
       break; 
    case LT_OP  :
       if (currentAttribute<value) {
        result=true;
        return result;
       }
       else return result;
       break; 
    case LE_OP  :
       if (currentAttribute<=value) {
        result=true;
        return result;
       }
       else return result;
       break; 
    case GT_OP  :
       if (currentAttribute>value) {
        result=true;
        return result;
       }
       else return result;
       break; 
    case GE_OP  :
       if (currentAttribute>=value) {
        result=true;
        return result;
       }
       else return result;
       break;
    case NE_OP  :
       if (currentAttribute!=value) {
        result=true;
        return result;
       }
       else return result;
       break;
    }
}

  bool RecordBasedFileManager::performCompOpString(const CompOp compOp, string currentAttribute, string value) {
     bool result = false;
     int compared = currentAttribute.compare(value);
     switch(compOp){
    case NO_OP  :
       result = true;
       return result;
       break; 
    case EQ_OP  :
       if (compared ==0) {
        result=true;
        return result;
       }
       else return result;
       break; 
    case LT_OP  :
       if (compared>0) {
        result=true;
        return result;
       }
       else return result;
       break; 
    case LE_OP  :
       if (compared>=0) {
        result=true;
        return result;
       }
       else return result;
       break; 
    case GT_OP  :
       if (compared<0) {
        result=true;
        return result;
       }
       else return result;
       break; 
    case GE_OP  :
       if (compared<=0) {
        result=true;
        return result;
       }
       else return result;
       break;
    case NE_OP  :
       if (compared !=0) {
        result=true;
        return result;
       }
       else return result;
       break;
    }
    

  }




