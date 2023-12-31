#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <vector>

#include "ix.h"

namespace PeterDB {

#define RM_EOF (-1)

#define tablesName "Tables"

#define columnsName "Columns"

#define indexesName "Indexes"

#define indexSuffix ".idx"

    // RM_ScanIterator is an iterator to go through tuples
    class RM_ScanIterator {
    public:
        RBFM_ScanIterator rbfm_ScanIterator;

        RM_ScanIterator();

        ~RM_ScanIterator();

        // "data" follows the same format as RelationManager::insertTuple()
        RC getNextTuple(RID &rid, void *data);

        RC close();
    };

    // RM_IndexScanIterator is an iterator to go through index entries
    class RM_IndexScanIterator {
    public:
        IXFileHandle ixFileHandle;
        IX_ScanIterator ix_ScanIterator;

        RM_IndexScanIterator();    // Constructor
        ~RM_IndexScanIterator();    // Destructor

        // "key" follows the same format as in IndexManager::insertEntry()
        RC getNextEntry(RID &rid, void *key);    // Get next matching entry
        RC close();                              // Terminate index scan
    };

    // Relation Manager
    class RelationManager {
    private:
        void *tableIdBuffer, *attributesBuffer, *positionBuffer, *tupleBuffer;

        static std::vector<Attribute> getTablesAttrs();

        static std::vector<Attribute> getColumnsAttrs();

        static std::vector<Attribute> getIndexesAttrs();

        static std::vector<std::string> getAttributeAttrs();

        RC deleteFromSystemFiles(const std::string &tableName, const RID &rid);

    public:
        static RelationManager &instance();

        bool checkTableExists(const std::string &tableName);

        int increaseTableVersion(const std::string &tableName);

        RC insertTables(FileHandle &tablesHandle, int tableId, const std::string &tableName);

        RC insertColumn(FileHandle &columnsHandle, int tableId, const Attribute &attr, int position, int version);

        RC insertColumns(FileHandle &columnsHandle, int tableId, const std::vector<Attribute> &attrs);

        RC insertIndexes(FileHandle &indexesHandle, int tableId, int columnPosition);

        RC createCatalog();

        RC deleteCatalog();

        int getTableId(const std::string &tableName);

        std::string getFileName(const std::string &tableName);

        int generateTableId();

        int generatePosition(int tableId);

        int getDataBufferSize(const std::vector<Attribute> &attrs);

        RC createTable(const std::string &tableName, const std::vector<Attribute> &attrs);

        RC deleteTable(const std::string &tableName);

        RC getVersionedAttributes(const std::string &tableName, int version, std::vector<Attribute> &attrs, std::vector<int> &positions);

        RC getAttributes(const std::string &tableName, std::vector<Attribute> &attrs);

        RC insertTuple(const std::string &tableName, const void *data, RID &rid);

        RC deleteTuple(const std::string &tableName, const RID &rid);

        RC updateTuple(const std::string &tableName, const void *data, const RID &rid);

        RC readTuple(const std::string &tableName, const RID &rid, void *data);

        // Print a tuple that is passed to this utility method.
        // The format is the same as printRecord().
        RC printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out);

        RC readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName, void *data);

        // Scan returns an iterator to allow the caller to go through the results one by one.
        // Do not store entire results in the scan iterator.
        RC scan(const std::string &tableName,
                const std::string &conditionAttribute,
                const CompOp compOp,                  // comparison type such as "<" and "="
                const void *value,                    // used in the comparison
                const std::vector<std::string> &attributeNames, // a list of projected attributes
                RM_ScanIterator &rm_ScanIterator);

        void convert(const std::string &tableName, int fromVersion, int toVersion, const void *dataBuffer, int dataBufferLength, void* data);

        // Extra credit work (10 points)
        RC addAttribute(const std::string &tableName, const Attribute &attr);

        RC dropAttribute(const std::string &tableName, const std::string &attributeName);

        // QE IX related
        RC modifyIndex(const std::string &tableName, const void *data, const RID &rid, const std::vector<Attribute> &attrs, const std::vector<int> &positions, bool insertion = true);

        int getColumnPosition(const std::string &tableName, const std::string &attributeName, Attribute &attrBuffer);

        std::string getIndexFileName(const std::string &tableName, int columnPosition);

        bool hasIndexOn(const std::string &tableName, int columnPosition, RID &rid);

        RC createIndex(const std::string &tableName, const std::string &attributeName);

        RC destroyIndex(const std::string &tableName, const std::string &attributeName);

        // indexScan returns an iterator to allow the caller to go through qualified entries in index
        RC indexScan(const std::string &tableName,
                     const std::string &attributeName,
                     const void *lowKey,
                     const void *highKey,
                     bool lowKeyInclusive,
                     bool highKeyInclusive,
                     RM_IndexScanIterator &rm_IndexScanIterator);

    protected:
        RelationManager();                                                  // Prevent construction
        ~RelationManager();                                                 // Prevent unwanted destruction
        RelationManager(const RelationManager &);                           // Prevent construction by copying
        RelationManager &operator=(const RelationManager &);                // Prevent assignment

    };

} // namespace PeterDB

#endif // _rm_h_