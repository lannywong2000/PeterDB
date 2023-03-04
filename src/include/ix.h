#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <climits>

#include "pfm.h"
#include "rbfm.h" // for some type declarations only, e.g., RID and Attribute

namespace PeterDB {

#define IX_EOF (-1)  // end of the index scan

#define UNDEFINED_PAGE_NUM UINT_MAX

    typedef unsigned short PageIndex;

    const PageIndex RID_SIZE = sizeof(PageNum) + sizeof(SlotNum);

    typedef struct Entry {
        bool isNull = true;
        PageNum nodeNum;
        char *key = nullptr;
        RID rid;
        int keyLength;

        Entry() {};
        Entry(Entry &entry) : isNull(entry.isNull), nodeNum(entry.nodeNum), rid(entry.rid), keyLength(entry.keyLength) {
            key = new char[entry.keyLength];
            std::memcpy(key, entry.key, entry.keyLength);
        }
    } Entry;

    class IX_ScanIterator;

    class IXFileHandle;

    class IndexManager {

    public:
        static IndexManager &instance();

        // File Functions

        // Create an index file.
        RC createFile(const std::string &fileName);

        // Delete an index file.
        RC destroyFile(const std::string &fileName);

        // Open an index and return an ixFileHandle.
        RC openFile(const std::string &fileName, IXFileHandle &ixFileHandle);

        // Close an ixFileHandle for an index.
        RC closeFile(IXFileHandle &ixFileHandle);

        // Write the page num of root to disk.
        RC writeRootPageNum(IXFileHandle &ixFileHandle);

        // Read the page num of root from disk.
        RC readRootPageNum(IXFileHandle &ixFileHandle);


        // General Index Functions

        // Get start of free space of an index page.
        PageIndex getStartOfFreeSpace(const char *pageBuffer);

        // Get key list size of an index page.
        PageIndex getKeyListSize(const char *pageBuffer);

        // Get key length;
        PageIndex getKeyLength(const void *key, unsigned keyType);

        // Get composite key length.
        PageIndex getCompKeyLength(const void *key, unsigned keyType);

        // Get RID from page buffer.
        void getRID(const char *pageBuffer, RID &ridBuffer);

        // Compare key in a page buffer with the one given.
        int compareKey(const char *pageBuffer, const void *key, unsigned keyType);

        // Compare two given RIDs.
        int compareRID(const RID &ridBuffer, const RID &rid);

        // Compare composite key in page buffer with the one given.
        int compareCompKey(const char *pageBuffer, const void *key, const RID &rid, unsigned keyType);


        // Node Functions

        // Create a index node template.
        RC createNode(char *nodeBuffer);

        // Create a new root node.
        RC createNewRoot(IXFileHandle &ixFileHandle, const Entry &childEntry);

        // Whether a node page has space for an entry.
        bool hasNodeSpace(const char *nodeBuffer, int keyLength);

        // Find the page num pointer by the given key.
        PageNum findInNode(const char *nodeBuffer, const void *key, unsigned keyType, int &pageNumOffset, int &keyOffset, const RID &rid);

        // Insert a key-RID pair in a node page, free space guaranteed.
        void insertNode(char *nodeBuffer, int pageNumOffset, int keyOffset, const Entry &entry);

        // Get the page num offset and key offset of the middle entry in node.
        void getMidNode(const char *nodeBuffer, unsigned keyType, PageIndex &midPageNumOffset, PageIndex &midKeyOffset, PageIndex &oldListSize, PageIndex &newListSize);


        // Leaf Functions

        // Create a index leaf template.
        RC createLeaf(char *leafBuffer);

        // Whether a page is leaf.
        bool isLeaf(char *pageBuffer);

        // Get next page num in a leaf page.
        PageNum getNextPageNum(const char *leafBuffer);

        // Whether a composite key is in leaf.
        int findInLeaf(const char *leafBuffer, const void *key, const RID &rid, unsigned keyType, PageIndex &index);

        // Whether a leaf page has space for a composite key.
        bool hasLeafSpace(const char *leafBuffer, const void *key, unsigned keyType);

        // Get the offset of the middle key in leaf.
        PageIndex getLeafMidKey(const char *leafBuffer, unsigned keyType, PageIndex &oldListSize);

        // Insert a key-RID pair in a leaf page, free space guaranteed.
        void insertLeaf(char *leafBuffer, int offset, const void *key, const RID &rid, unsigned keyType);

        // Get the page num of the leftmost leaf.
        PageNum getLeftMostLeaf(IXFileHandle &ixFileHandle, char *leafBuffer);

        // Get the page num of leaf by key.
        PageNum getLeafByKey(IXFileHandle &ixFileHandle, const void *key, unsigned keyType, char *leafBuffer, const RID &rid);


        // B+ Tree recursive insertion.
        RC insert(IXFileHandle &ixFileHandle, PageNum nodeNum, const void *key, const RID &rid, Entry &childEntry, unsigned keyType);

        // Insert an entry into the given index that is indicated by the given ixFileHandle.
        RC insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixFileHandle.
        RC deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixFileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print a B+ Tree Entry key.
        int printBTreeKey(const char *pageBuffer, std::ostream &out, unsigned keyType) const;

        // Print a B+ Tree Entry RID.
        void printBTreeRID(const char *pageBuffer, std::ostream &out) const;

        // Print a B+ tree node entry.
        int printBTreeNodeEntry(const char *nodeBuffer, std::ostream &out, unsigned keyType) const;

        // Print a B+ tree leaf entry.
        int printBTreeLeafEntry(const char *leafBuffer, std::ostream &out, const PageIndex &keyListSize, PageIndex &index, unsigned keyType) const;

        // Print a B+ tree node recursively.
        RC printBTreeNode(IXFileHandle &ixFileHandle, PageNum nodeNum, std::ostream &out) const;

        // Print the B+ tree in pre-order (in a JSON record format).
        RC printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const;

    protected:
        IndexManager() = default;                                                   // Prevent construction
        ~IndexManager() = default;                                                  // Prevent unwanted destruction
        IndexManager(const IndexManager &) = default;                               // Prevent construction by copying
        IndexManager &operator=(const IndexManager &) = default;                    // Prevent assignment

    };

    class IX_ScanIterator {
    public:
        IndexManager &ix = IndexManager::instance();
        IXFileHandle *ixFileHandle;

        char *leafBuffer = nullptr;
        PageIndex keyListSize, index, offset;
        unsigned keyType;
        char *highKey = nullptr;
        bool highKeyInclusive;

        // Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
    };

    class IXFileHandle {
    public:
        FileHandle fileHandle;

        //index variables
        unsigned rootPageNum = UNDEFINED_PAGE_NUM;
        unsigned keyType; // 0: INTEGER, 1: FLOAT, 2: VARCHAR

        // variables to keep counter for each operation
        unsigned ixReadPageCounter;
        unsigned ixWritePageCounter;
        unsigned ixAppendPageCounter;

        // Constructor
        IXFileHandle();

        // Destructor
        ~IXFileHandle();

        // Put the current counter values of associated PF FileHandles into variables
        RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    };
}// namespace PeterDB
#endif // _ix_h_
