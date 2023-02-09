#include "src/include/rm.h"
#include <iostream>

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() {
        tableIdBuffer = new char[5];
        std::memset(tableIdBuffer, 0, 5);
        attributesBuffer = new char[67];
        std::memset(attributesBuffer, 0, 67);
    }

    RelationManager::~RelationManager() {
        delete[] tableIdBuffer;
        delete[] attributesBuffer;
    }

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    std::vector<Attribute> RelationManager::getTablesAttrs() {
        std::vector<Attribute> tablesAttrs;
        tablesAttrs.emplace_back("table-id", TypeInt, 4);
        tablesAttrs.emplace_back("table-name", TypeVarChar, 50);
        tablesAttrs.emplace_back("file-name", TypeVarChar, 50);
        return tablesAttrs;
    }

    std::vector<Attribute> RelationManager::getColumnsAttrs() {
        std::vector<Attribute> columnsAttrs;
        columnsAttrs.emplace_back("table-id", TypeInt, 4);
        columnsAttrs.emplace_back("column-name", TypeVarChar, 50);
        columnsAttrs.emplace_back("column-type", TypeInt, 4);
        columnsAttrs.emplace_back("column-length", TypeInt, 4);
        columnsAttrs.emplace_back("column-position", TypeInt, 4);
        return columnsAttrs;
    }

    std::vector<std::string> RelationManager::getAttributeAttrs() {
        std::vector<std::string> attributeAttrs = {"column-name", "column-type", "column-length", "column-position"};
        return attributeAttrs;
    }

    bool RelationManager::checkTableExists(const std::string &tableName) {
        FileHandle fileHandle;
        RC rc = rbfm.openFile(getFileName(tableName), fileHandle);
        rbfm.closeFile(fileHandle);
        return rc == 0;
    }

    RC RelationManager::insertTables(FileHandle &tablesHandle, int tableId, const std::string &tableName) {
        RID rid;
        int tableNameLength = tableName.size();
        char *data = new char[1 + 3 * sizeof(int) + 2 * tableNameLength];
        std::memset(data, 0, 1);
        std::memcpy(data + 1, &tableId, sizeof(int));
        std::memcpy(data + 1 + sizeof(int), &tableNameLength, sizeof(int));
        std::memcpy(data + 1 + 2 * sizeof(int), tableName.c_str(), tableNameLength);
        std::memcpy(data + 1 + 2 * sizeof(int) + tableNameLength, &tableNameLength, sizeof(int));
        std::memcpy(data + 1 + 3 * sizeof(int) + tableNameLength, tableName.c_str(), tableNameLength);
        RC rc = rbfm.insertRecord(tablesHandle, getTablesAttrs(), data, rid);
        delete[] data;
        return rc;
    }

    RC RelationManager::insertColumns(FileHandle &columnsHandle, int tableId, const std::vector<Attribute> &attrs) {
        RID rid;
        int attrsLength = attrs.size();
        for (int pos = 1; pos <= attrsLength; pos = pos + 1) {
            const Attribute &attr = attrs[pos - 1];
            int attrNameLength = attr.name.size();
            char *data = new char[1 + 5 * sizeof(int) + attrNameLength];
            std::memset(data, 0, 1);
            std::memcpy(data + 1, &tableId, sizeof(int));
            std::memcpy(data + 1 + sizeof(int), &attrNameLength, sizeof(int));
            std::memcpy(data + 1 + 2 * sizeof(int), attr.name.c_str(), attrNameLength);
            std::memcpy(data + 1 + 2 * sizeof(int) + attrNameLength, &attr.type, sizeof(int));
            std::memcpy(data + 1 + 3 * sizeof(int) + attrNameLength, &attr.length, sizeof(int));
            std::memcpy(data + 1 + 4 * sizeof(int) + attrNameLength, &pos, sizeof(int));
            RC rc = rbfm.insertRecord(columnsHandle, getColumnsAttrs(), data, rid);
            delete[] data;
            if (rc != 0) return rc;
        }
        return 0;
    }

    RC RelationManager::deleteFromSystemFiles(const std::string &tableName, const RID &rid) {
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        RC rc = rbfm.deleteRecord(fileHandle, attrs, rid);
        rbfm.closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::createCatalog() {
        RC rc = rbfm.createFile(tablesName);
        if (rc != 0) return rc;
        rc = rbfm.createFile(columnsName);
        if (rc != 0) return rc;

        FileHandle tablesHandle, columnsHandle;
        rbfm.openFile(tablesName, tablesHandle);
        rbfm.openFile(columnsName, columnsHandle);

        insertTables(tablesHandle, -2, tablesName);
        insertTables(tablesHandle, -1, columnsName);

        insertColumns(columnsHandle, -2, getTablesAttrs());
        insertColumns(columnsHandle, -1, getColumnsAttrs());

        rbfm.closeFile(tablesHandle);
        rbfm.closeFile(columnsHandle);

        return 0;
    }

    RC RelationManager::deleteCatalog() {
        RC rc = rbfm.destroyFile(tablesName);
        if (rc != 0) return rc;
        rc = rbfm.destroyFile(columnsName);
        if (rc != 0) return rc;
        return 0;
    }

    int RelationManager::getTableId(const std::string &tableName) {
        RM_ScanIterator rm_ScanIterator;
        std::vector<std::string> attributeNames = {"table-id"};
        scan(tablesName, "table-name", EQ_OP, &tableName, attributeNames, rm_ScanIterator);
        RID rid;
        int tableId;
        rm_ScanIterator.getNextTuple(rid, tableIdBuffer);
        std::memcpy(&tableId, tableIdBuffer + 1, sizeof(int));
        rm_ScanIterator.close();
        return tableId;
    }

    std::string RelationManager::getFileName(const std::string &tableName) {
        return tableName;
    }

    int RelationManager::generateTableId() {
        RM_ScanIterator rm_ScanIterator;
        std::vector<std::string> attributeNames = {"table-id"};
        scan(tablesName, "table-name", NO_OP, nullptr, attributeNames, rm_ScanIterator);
        RID rid;
        int tableId = -1, nextTableId;
        while (rm_ScanIterator.getNextTuple(rid, tableIdBuffer) != RM_EOF) {
            std::memcpy(&nextTableId, tableIdBuffer + 1, sizeof(int));
            tableId = nextTableId > tableId ? nextTableId : tableId;
        }
        rm_ScanIterator.close();
        std::cout << tableId + 1 << std::endl;
        return tableId + 1;
    }

    RC RelationManager::createTable(const std::string &tableName, const std::vector<Attribute> &attrs) {
        if (!checkTableExists(tablesName)) return ERR_CATALOG_NOT_EXISTS;
        if (checkTableExists(tableName)) return ERR_TABLE_NAME_EXISTS;

        FileHandle tablesHandle, columnsHandle;
        rbfm.openFile(tablesName, tablesHandle);
        rbfm.openFile(columnsName, columnsHandle);

        int tableId = generateTableId();
        insertTables(tablesHandle, tableId, tableName);
        insertColumns(columnsHandle, tableId, attrs);

        rbfm.createFile(tableName);

        rbfm.closeFile(tablesHandle);
        rbfm.closeFile(columnsHandle);

        return 0;
    }

    RC RelationManager::deleteTable(const std::string &tableName) {
        if (tableName == tablesName || tableName == columnsName) return ERR_CATALOG_ILLEGAL_DELETE;
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        RM_ScanIterator rm_ScanIterator;
        std::vector<std::string> attributeNames = {"table-id"};
        scan(tablesName, "table-name", EQ_OP, &tableName, attributeNames, rm_ScanIterator);
        RID rid;
        int tableId;
        std::cout << rm_ScanIterator.rbfm_ScanIterator.rids.size() << std::endl;
        RC rc = rm_ScanIterator.getNextTuple(rid, tableIdBuffer);
        std::memcpy(&tableId, tableIdBuffer + 1, sizeof(int));
        rm_ScanIterator.close();
        if (rc != 0) return rc;
        deleteFromSystemFiles(tablesName, rid);

        std::vector<RID> toBeDeleted;
        scan(columnsName, "table-id", EQ_OP, &tableId, attributeNames, rm_ScanIterator);
        while (rm_ScanIterator.getNextTuple(rid, tableIdBuffer) != RM_EOF) {
            std::memcpy(&tableId, tableIdBuffer + 1, sizeof(int));
            toBeDeleted.push_back(rid);
        }
        rm_ScanIterator.close();
        for (const RID &ridBuffer : toBeDeleted) deleteFromSystemFiles(columnsName, ridBuffer);

        return rbfm.destroyFile(getFileName(tableName));
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        int tableId = getTableId(tableName);
        std::cout << tableId << std::endl;
        RM_ScanIterator rm_ScanIterator;
        scan(columnsName, "table-id", EQ_OP, &tableId, getAttributeAttrs(), rm_ScanIterator);

        RID rid;
        int attributeNameLength, position;
        attrs = std::vector<Attribute>(rm_ScanIterator.rbfm_ScanIterator.rids.size());
        while (rm_ScanIterator.getNextTuple(rid, attributesBuffer) != RM_EOF) {
            std::memcpy(&attributeNameLength, attributesBuffer + 1, sizeof(int));
            std::memcpy(&position, attributesBuffer + 1 + 3 * sizeof(int) + attributeNameLength, sizeof(int));
            char *attributeName = new char[attributeNameLength];
            std::memset(attributeName, 0, attributeNameLength);
            std::memcpy(attributeName, attributesBuffer + 1 + sizeof(int), attributeNameLength);
            attrs[position - 1].name = attributeName;
            std::memcpy(&attrs[position - 1].type, attributesBuffer + 1 + sizeof(int) + attributeNameLength, sizeof(int));
            std::memcpy(&attrs[position - 1].length, attributesBuffer + 1 + 2 * sizeof(int) + attributeNameLength, sizeof(int));
            delete[] attributeName;
        }

        rm_ScanIterator.close();

        return 0;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if (tableName == tablesName || tableName == columnsName) return ERR_CATALOG_ILLEGAL_MODIFY;
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        RC rc = rbfm.insertRecord(fileHandle, attrs, data, rid);
        rbfm.closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if (tableName == tablesName || tableName == columnsName) return ERR_CATALOG_ILLEGAL_MODIFY;
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        RC rc = rbfm.deleteRecord(fileHandle, attrs, rid);
        rbfm.closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        RC rc = rbfm.updateRecord(fileHandle, attrs, data, rid);
        rbfm.closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        RC rc = rbfm.readRecord(fileHandle, attrs, rid, data);
        rbfm.closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return rbfm.printRecord(attrs, data, out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        RC rc = rbfm.readAttribute(fileHandle, attrs, rid, attributeName, data);
        rbfm.closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::scan(const std::string &tableName,
                             const std::string &conditionAttribute,
                             const CompOp compOp,
                             const void *value,
                             const std::vector<std::string> &attributeNames,
                             RM_ScanIterator &rm_ScanIterator) {
        FileHandle fileHandle;
        RC rc = rbfm.openFile(getFileName(tableName), fileHandle);
        if (rc != 0) return rc;
        std::vector<Attribute> attrs;
        if (tableName == tablesName) attrs = getTablesAttrs();
        else if (tableName == columnsName) attrs = getColumnsAttrs();
        else getAttributes(tableName, attrs);
        rc = rbfm.scan(fileHandle, attrs, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator.rbfm_ScanIterator);
        return rc;
    }

    RM_ScanIterator::RM_ScanIterator() = default;

    RM_ScanIterator::~RM_ScanIterator() = default;

    RC RM_ScanIterator::getNextTuple(RID &rid, void *data) {
        return rbfm_ScanIterator.getNextRecord(rid, data);
    }

    RC RM_ScanIterator::close() {
        return rbfm_ScanIterator.close();
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        return -1;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        return -1;
    }

    // QE IX related
    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        return -1;
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        return -1;
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return -1;
    }

    RC RM_IndexScanIterator::close(){
        return -1;
    }

} // namespace PeterDB