#ifndef _rbfm_h_
#define _rbfm_h_

#include <utility>
#include <vector>
#include <ostream>
#include <cstring>

#include "pfm.h"

namespace PeterDB {

#define RBFM_EOF (-1)

    typedef unsigned short SlotNum;

    //Slot Directory
    typedef struct Slot {
        short offset;      // offset of the record in the page
        unsigned short length;      // record length in bytes

        Slot() {};
        Slot(short offset, unsigned short length): offset(offset), length(length) {};
    } Slot;

    // Record ID
    typedef struct RID{
        PageNum pageNum;     // page number
        SlotNum slotNum;     // slot number in the page

        RID() {};
        RID(PageNum pageNum, SlotNum slotNum) : pageNum(pageNum), slotNum(slotNum) {};
    } RID;

    // Attribute
    typedef enum {
        TypeInt = 0, TypeReal, TypeVarChar
    } AttrType;

    typedef unsigned AttrLength;

    typedef struct Attribute {
        std::string name;  // attribute name
        AttrType type;     // attribute type
        AttrLength length; // attribute length

        Attribute() {};
        Attribute(std::string name, AttrType type, AttrLength length): name(std::move(name)), type(type), length(length) {};
    } Attribute;

    // Comparison Operator (NOT needed for part 1 of the project)
    typedef enum {
        EQ_OP = 0,  // =
        LT_OP,      // <
        LE_OP,      // <=
        GT_OP,      // >
        GE_OP,      // >=
        NE_OP,      // !=
        NO_OP       // no condition
    } CompOp;


    /********************************************************************
    * The scan iterator is NOT required to be implemented for Project 1 *
    ********************************************************************/

    //  RBFM_ScanIterator is an iterator to go through records
    //  The way to use it is like the following:
    //  RBFM_ScanIterator rbfmScanIterator;
    //  rbfm.open(..., rbfmScanIterator);
    //  while (rbfmScanIterator(rid, data) != RBFM_EOF) {
    //    process the data;
    //  }
    //  rbfmScanIterator.close();

    class RBFM_ScanIterator {
    public:
        RBFM_ScanIterator() = default;

        ~RBFM_ScanIterator() = default;

        FileHandle fileHandle;

        std::vector<RID> rids;

        int pos;

        std::vector<Attribute> recordDescriptor;

        std::vector<std::string> attributeNames;

        // Never keep the results in the memory. When getNextRecord() is called,
        // a satisfying record needs to be fetched from the file.
        // "data" follows the same format as RecordBasedFileManager::insertRecord().
        RC getNextRecord(RID &rid, void *data);

        RC close();
    };

    class RecordBasedFileManager {
    public:
        static RecordBasedFileManager &instance();                          // Access to the singleton instance

        RC createFile(const std::string &fileName);                         // Create a new record-based file

        RC destroyFile(const std::string &fileName);                        // Destroy a record-based file

        RC openFile(const std::string &fileName, FileHandle &fileHandle);   // Open a record-based file

        RC closeFile(FileHandle &fileHandle);                               // Close a record-based file

        RC getPageBuffer(FileHandle &fileHandle, unsigned pageNum);          // Load page pageNum to the page buffer

        RC appendEmptyPage(FileHandle &fileHandle);                         // Append a structured empty page to the end of the paged file

        unsigned short getFreeSpace(FileHandle &fileHandle);                // Get the free space of current page

        unsigned short getStartOfFreeSpace(FileHandle &fileHandle);                               // Get the offset to the free space

        unsigned short getNumberOfSlot(FileHandle &fileHandle);                                   // Get the number of slot in page pageNum

        unsigned short getFreeSlotNum(FileHandle &fileHandle);       // Get the number of free slot in the slot directory

        RC getSlot(FileHandle &fileHandle, Slot &slot, unsigned short slotNum);        // Get the Slot slotNum of the current page

        RC writeSlot(FileHandle &fileHandle, Slot &slotBuffer, unsigned short slotNum);       // Write thr slot buffer to the slotNum slot of page buffer

        bool isCurrentPageFree(FileHandle &fileHandle);

        unsigned findFreePage(FileHandle &fileHandle);                      // Find a free page for a record buffer data to store

        RC toRecordBuffer(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data);     // convert data to record using descriptor

        RC getRecordBuffer(FileHandle &fileHandle, const RID &rid, bool recursive);         // Get the record from file to buffer

        RC toData(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, void *data);      // Transform record buffer to data

        void increaseVersion(FileHandle &fileHandle);

        int getRecordVersion(FileHandle &fileHandle, const RID &rid);

        //  Format of the data passed into the function is the following:
        //  [n byte-null-indicators for y fields] [actual value for the first field] [actual value for the second field] ...
        //  1) For y fields, there is n-byte-null-indicators in the beginning of each record.
        //     The value n can be calculated as: ceil(y / 8). (e.g., 5 fields => ceil(5 / 8) = 1. 12 fields => ceil(12 / 8) = 2.)
        //     Each bit represents whether each field value is null or not.
        //     If k-th bit from the left is set to 1, k-th field value is null. We do not include anything in the actual data part.
        //     If k-th bit from the left is set to 0, k-th field contains non-null values.
        //     If there are more than 8 fields, then you need to find the corresponding byte first,
        //     then find a corresponding bit inside that byte.
        //  2) Actual data is a concatenation of values of the attributes.
        //  3) For Int and Real: use 4 bytes to store the value;
        //     For Varchar: use 4 bytes to store the length of characters, then store the actual characters.
        //  !!! The same format is used for updateRecord(), the returned data of readRecord(), and readAttribute().
        // For example, refer to the Q8 of Project 1 wiki page.

        // Insert a record into a file
        RC insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        RID &rid);

        // Read a record identified by the given rid.
        RC
        readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid, void *data);

        // Print the record that is passed to this utility method.
        // This method will be mainly used for debugging/testing.
        // The format is as follows:
        // field1-name: field1-value  field2-name: field2-value ... \n
        // (e.g., age: 24  height: 6.1  salary: 9000
        //        age: NULL  height: 7.5  salary: 7500)
        RC printRecord(const std::vector<Attribute> &recordDescriptor, const void *data, std::ostream &out);

        /*****************************************************************************************************
        * IMPORTANT, PLEASE READ: All methods below this comment (other than the constructor and destructor) *
        * are NOT required to be implemented for Project 1                                                   *
        *****************************************************************************************************/
        RC shiftLeft(FileHandle &fileHandle, unsigned short offset, unsigned short length);

        // Delete a record identified by the given rid.
        RC deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid);

        // Assume the RID does not change after an update
        RC updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data,
                        const RID &rid);

        // Read an attribute given its name and the rid.
        RC readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const RID &rid,
                         const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        RC scan(FileHandle &fileHandle,
                const std::vector<Attribute> &recordDescriptor,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RBFM_ScanIterator &rbfm_ScanIterator);

    protected:
        RecordBasedFileManager();                                                   // Prevent construction
        ~RecordBasedFileManager();                                                  // Prevent unwanted destruction
        RecordBasedFileManager(const RecordBasedFileManager &);                     // Prevent construction by copying
        RecordBasedFileManager &operator=(const RecordBasedFileManager &);          // Prevent assignment

    };

} // namespace PeterDB

#endif // _rbfm_h_