#include "src/include/rm.h"

namespace PeterDB {
    RelationManager &RelationManager::instance() {
        static RelationManager _relation_manager = RelationManager();
        return _relation_manager;
    }

    RelationManager::RelationManager() {
        tableIdBuffer = malloc(5);
        std::memset(tableIdBuffer, 0, 5);
        attributesBuffer = malloc(75);
        std::memset(attributesBuffer, 0, 75);
        positionBuffer = malloc(5);
        std::memset(positionBuffer, 0, 5);
    }

    RelationManager::~RelationManager() {
        free(tableIdBuffer);
        free(attributesBuffer);
        free(positionBuffer);
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
        columnsAttrs.emplace_back("table-version", TypeInt, 4);
        return columnsAttrs;
    }

    std::vector<std::string> RelationManager::getAttributeAttrs() {
        std::vector<std::string> attributeAttrs = {"table-id", "column-name", "column-type", "column-length", "column-position", "table-version"};
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
        char data[1 + 3 * sizeof(int) + 2 * tableNameLength];
        std::memset(data, 0, 1);
        std::memcpy(data + 1, &tableId, sizeof(int));
        std::memcpy(data + 1 + sizeof(int), &tableNameLength, sizeof(int));
        std::memcpy(data + 1 + 2 * sizeof(int), tableName.c_str(), tableNameLength);
        std::memcpy(data + 1 + 2 * sizeof(int) + tableNameLength, &tableNameLength, sizeof(int));
        std::memcpy(data + 1 + 3 * sizeof(int) + tableNameLength, tableName.c_str(), tableNameLength);
        RC rc = rbfm.insertRecord(tablesHandle, getTablesAttrs(), data, rid);
        return rc;
    }

    RC RelationManager::insertColumn(FileHandle &columnsHandle, int tableId, const Attribute &attr, int position,
                                     int version) {
        RID rid;
        int attrNameLength = attr.name.size();
        char data[1 + 6 * sizeof(int) + attrNameLength];
        std::memset(data, 0, 1);
        std::memcpy(data + 1, &tableId, sizeof(int));
        std::memcpy(data + 1 + sizeof(int), &attrNameLength, sizeof(int));
        std::memcpy(data + 1 + 2 * sizeof(int), attr.name.c_str(), attrNameLength);
        std::memcpy(data + 1 + 2 * sizeof(int) + attrNameLength, &attr.type, sizeof(int));
        std::memcpy(data + 1 + 3 * sizeof(int) + attrNameLength, &attr.length, sizeof(int));
        std::memcpy(data + 1 + 4 * sizeof(int) + attrNameLength, &position, sizeof(int));
        std::memcpy(data + 1 + 5 * sizeof(int) + attrNameLength, &version, sizeof(int));
        return rbfm.insertRecord(columnsHandle, getColumnsAttrs(), data, rid);
    }

    RC RelationManager::insertColumns(FileHandle &columnsHandle, int tableId, const std::vector<Attribute> &attrs) {
        RC rc;
        int attrsLength = attrs.size();
        for (int pos = 1; pos <= attrsLength; pos = pos + 1) {
            rc = insertColumn(columnsHandle, tableId, attrs[pos - 1], pos, 0);
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
        int tableNameLength = tableName.size();
        char tableNameBuffer[sizeof(int) + tableNameLength];
        std::memcpy(tableNameBuffer, &tableNameLength, sizeof(int));
        std::memcpy(tableNameBuffer + sizeof(int), tableName.c_str(), tableNameLength);
        scan(tablesName, "table-name", EQ_OP, tableNameBuffer, attributeNames, rm_ScanIterator);
        RID rid;
        int tableId;
        rm_ScanIterator.getNextTuple(rid, tableIdBuffer);
        std::memcpy(&tableId, (char *) tableIdBuffer + 1, sizeof(int));
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
            std::memcpy(&nextTableId, (char *) tableIdBuffer + 1, sizeof(int));
            tableId = nextTableId > tableId ? nextTableId : tableId;
        }
        rm_ScanIterator.close();
        return tableId + 1;
    }

    int RelationManager::generatePosition(int tableId) {
        RM_ScanIterator rm_ScanIterator;
        std::vector<std::string> attributeNames = {"table-version"};
        scan(columnsName, "table-id", EQ_OP, &tableId, attributeNames, rm_ScanIterator);
        RID rid;
        int position = 0, nextPosition;
        while (rm_ScanIterator.getNextTuple(rid, positionBuffer) != RM_EOF) {
            std::memcpy(&nextPosition, (char *) tableIdBuffer + 1, sizeof(int));
            position = nextPosition > position ? nextPosition : position;
        }
        rm_ScanIterator.close();
        return position + 1;
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
        int tableId = getTableId(tableName);
        RID rid;
        RM_ScanIterator rm_ScanIterator;
        std::vector<std::string> attributeNames;
        scan(tablesName, "table-id", EQ_OP, &tableId, attributeNames, rm_ScanIterator);
        RC rc = rm_ScanIterator.getNextTuple(rid, tableIdBuffer);
        rm_ScanIterator.close();
        if (rc != 0) return rc;
        deleteFromSystemFiles(tablesName, rid);

        std::vector<RID> toBeDeleted;
        scan(columnsName, "table-id", EQ_OP, &tableId, attributeNames, rm_ScanIterator);
        while (rm_ScanIterator.getNextTuple(rid, tableIdBuffer) != RM_EOF) {
            std::memcpy(&tableId, (char *) tableIdBuffer + 1, sizeof(int));
            toBeDeleted.push_back(rid);
        }
        rm_ScanIterator.close();
        for (const RID &ridBuffer : toBeDeleted) deleteFromSystemFiles(columnsName, ridBuffer);

        return rbfm.destroyFile(getFileName(tableName));
    }

    RC
    RelationManager::getVersionedAttributes(const std::string &tableName, std::vector<Attribute> &attrs, int targetVersion) {
        int tableId = getTableId(tableName);
        RM_ScanIterator rm_ScanIterator;
        scan(columnsName, "table-id", EQ_OP, &tableId, getAttributeAttrs(), rm_ScanIterator);

        RID rid;
        int attributeNameLength, position, version;
        std::string name;
        AttrType type;
        AttrLength length;
        while (rm_ScanIterator.getNextTuple(rid, attributesBuffer) != RM_EOF) {
            std::memcpy(&attributeNameLength, (char *) attributesBuffer + 1 + sizeof(int), sizeof(int));
            std::memcpy(&version, (char *) attributesBuffer + 1 + 5 * sizeof(int) + attributeNameLength, sizeof(int));
            if (version != targetVersion) continue;
            std::memcpy(&position, (char *) attributesBuffer + 1 + 4 * sizeof(int) + attributeNameLength, sizeof(int));
            char attributeName[attributeNameLength + 1];
            std::memset(attributeName, 0, attributeNameLength + 1);
            std::memcpy(attributeName, (char *) attributesBuffer + 1 + 2 * sizeof(int), attributeNameLength);
            name = std::string(attributeName);
            std::memcpy(&type, (char *) attributesBuffer + 1 + 2 * sizeof(int) + attributeNameLength, sizeof(int));
            std::memcpy(&length, (char *) attributesBuffer + 1 + 3 * sizeof(int) + attributeNameLength, sizeof(int));
            attrs.emplace_back(name, type, length);
        }

        rm_ScanIterator.close();

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        if (tableName == tablesName) {attrs = getTablesAttrs(); return 0;}
        if (tableName == columnsName) {attrs = getColumnsAttrs(); return 0;}
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        RC rc = getVersionedAttributes(tableName, attrs, fileHandle.version);
        rbfm.closeFile(fileHandle);
        return rc;
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

    int RelationManager::calculateDataBufferSize(const std::vector<Attribute> &attrs) {
        int attrNum = attrs.size(), size = attrNum % 8 == 0 ? attrNum / 8 : attrNum / 8 + 1;
        for (const Attribute &attr : attrs) {
            size = size + attr.length;
            if (attr.type == 2) size = size + sizeof(int);
        }
        return size;
    }

    RC RelationManager::fromVersion(const std::string &tableName, int recordVersion, const void *recordBuffer, int recordLength, void *data) {
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        int version = fileHandle.version;
        rbfm.closeFile(fileHandle);
        if (recordVersion == version) {
            std::memcpy(data, recordBuffer, recordLength);
            return 0;
        }
        return -1;
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        std::vector<Attribute> attrs;
        int recordVersion = rbfm.getRecordVersion(fileHandle, rid);
        getVersionedAttributes(tableName, attrs, recordVersion);
        int recordLength = calculateDataBufferSize(attrs);
        char recordBuffer[recordLength];
        RC rc = rbfm.readRecord(fileHandle, attrs, rid, recordBuffer);
        rbfm.closeFile(fileHandle);
        if (rc != 0) return rc;
        return fromVersion(tableName, recordVersion, recordBuffer, recordLength, data);
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
        getAttributes(tableName, attrs);
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

    int RelationManager::increaseTableVersion(const std::string &tableName) {
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        rbfm.increaseVersion(fileHandle);
        int version = fileHandle.version;
        rbfm.closeFile(fileHandle);
        return version;
    }

    // Extra credit work
    RC RelationManager::dropAttribute(const std::string &tableName, const std::string &attributeName) {
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        int newVersion = increaseTableVersion(tableName);

        int tableId = getTableId(tableName);

        FileHandle columnsHandle;
        rbfm.openFile(columnsName, columnsHandle);

        RC rc;
        RM_ScanIterator rm_ScanIterator;
        scan(columnsName, "table-id", EQ_OP, &tableId, getAttributeAttrs(), rm_ScanIterator);
        RID rid;
        std::string name;
        int attributeNameLength, version;
        while (rm_ScanIterator.getNextTuple(rid, attributesBuffer) != RM_EOF) {
            std::memcpy(&attributeNameLength, (char *) attributesBuffer + 1 + sizeof(int), sizeof(int));
            std::memcpy(&version, (char *) attributesBuffer + 1 + 5 * sizeof(int) + attributeNameLength, sizeof(int));
            char nameBuffer[attributeNameLength + 1];
            std::memset(nameBuffer, 0, attributeNameLength + 1);
            std::memcpy(nameBuffer, (char *) attributesBuffer + 1 + 2 * sizeof(int), attributeNameLength);
            name = std::string(nameBuffer);
            if (version != newVersion - 1 || name == attributeName) continue;
            std::memcpy((char *) attributesBuffer + 1 + 5 * sizeof(int) + attributeNameLength, &newVersion, sizeof(int));
            rc = rbfm.insertRecord(columnsHandle, getColumnsAttrs(), attributesBuffer, rid);
        }

        rm_ScanIterator.close();
        rbfm.closeFile(columnsHandle);

        return rc;
    }

    // Extra credit work
    RC RelationManager::addAttribute(const std::string &tableName, const Attribute &attr) {
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        std::vector<Attribute> attrs;
        getAttributes(tableName, attrs);
        int newVersion = increaseTableVersion(tableName);

        int tableId = getTableId(tableName);
        int position = generatePosition(tableId);

        FileHandle columnsHandle;
        rbfm.openFile(columnsName, columnsHandle);

        RC rc;
        RM_ScanIterator rm_ScanIterator;
        scan(columnsName, "table-id", EQ_OP, &tableId, getAttributeAttrs(), rm_ScanIterator);
        RID rid;
        int attributeNameLength, version;
        while (rm_ScanIterator.getNextTuple(rid, attributesBuffer) != RM_EOF) {
            std::memcpy(&attributeNameLength, (char *) attributesBuffer + 1 + sizeof(int), sizeof(int));
            std::memcpy(&version, (char *) attributesBuffer + 1 + 5 * sizeof(int) + attributeNameLength, sizeof(int));
            if (version != newVersion - 1) continue;
            std::memcpy((char *) attributesBuffer + 1 + 5 * sizeof(int) + attributeNameLength, &newVersion, sizeof(int));
            rc = rbfm.insertRecord(columnsHandle, getColumnsAttrs(), attributesBuffer, rid);
        }

        rm_ScanIterator.close();
        if (rc != 0) return rc;

        rc = insertColumn(columnsHandle, tableId, attr, position, newVersion);

        rbfm.closeFile(columnsHandle);
        return rc;
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