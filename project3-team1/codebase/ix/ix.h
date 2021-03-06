#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan

class IX_ScanIterator;
class IXFileHandle;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const;
    protected:
        IndexManager();
        ~IndexManager();

    private:
        // new Child Entry
        typedef struct
        {
            uint32_t pageNum; // page number
            AttrType key; // slot number in the page
        } childEntry;

        RC insertRecursive(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, childEntry *newChildEntry, uint32_t pageNum);
        RC deleteRecursive(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid, uint32_t pageNum);
        void printRecursive(IXFileHandle &ixfileHandle, const Attribute &attribute, int pageNum);
        static IndexManager *_index_manager;
        bool pageIsLeaf(void *data);
        uint32_t getFreeSpacePointer(void *data);
        int numOnPage(void *data);
        uint32_t rootPageNumber(IXFileHandle &ixfileHandle);
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    private:
        IndexManager idm;
        IXFileHandle ixfh;
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    RC readPage(PageNum pageNum, void *data);
    RC writePage(PageNum pageNum, void *data);
    int appendPage(void *data); //returns number of pages

	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

};

#endif
