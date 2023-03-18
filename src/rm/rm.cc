#include "src/include/rm.h"
#include <iostream>

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
        tupleBuffer = malloc(PAGE_SIZE);
        std::memset(tupleBuffer, 0, PAGE_SIZE);
    }

    RelationManager::~RelationManager() {
        free(tableIdBuffer);
        free(attributesBuffer);
        free(positionBuffer);
        free(tupleBuffer);
    }

    RelationManager::RelationManager(const RelationManager &) = default;

    RelationManager &RelationManager::operator=(const RelationManager &) = default;

    RecordBasedFileManager &rbfm = RecordBasedFileManager::instance();

    IndexManager &ix = IndexManager::instance();

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

    std::vector<Attribute> RelationManager::getIndexesAttrs() {
        std::vector<Attribute> indexesAttrs;
        indexesAttrs.emplace_back("table-id", TypeInt, 4);
        indexesAttrs.emplace_back("column-position", TypeInt, 4);
        return indexesAttrs;
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

    RC RelationManager::insertIndexes(FileHandle &indexesHandle, int tableId, int columnPosition) {
        RID rid;
        char data[1 + 2 * sizeof(int)];
        std::memset(data, 0, 1);
        std::memcpy(data + 1, &tableId, sizeof(int));
        std::memcpy(data + 1 + sizeof(int), &columnPosition, sizeof(int));
        return rbfm.insertRecord(indexesHandle, getIndexesAttrs(), data, rid);
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
        rc = rbfm.createFile(indexesName);
        if (rc != 0) return rc;

        FileHandle tablesHandle, columnsHandle;
        rbfm.openFile(tablesName, tablesHandle);
        rbfm.openFile(columnsName, columnsHandle);

        insertTables(tablesHandle, -2, tablesName);
        insertTables(tablesHandle, -1, columnsName);

        insertTables(tablesHandle, -3, indexesName);

        insertColumns(columnsHandle, -2, getTablesAttrs());
        insertColumns(columnsHandle, -1, getColumnsAttrs());

        insertColumns(columnsHandle, -3, getIndexesAttrs());

        rbfm.closeFile(tablesHandle);
        rbfm.closeFile(columnsHandle);

        return 0;
    }

    RC RelationManager::deleteCatalog() {
        RC rc = rbfm.destroyFile(tablesName);
        if (rc != 0) return rc;
        rc = rbfm.destroyFile(columnsName);
        if (rc != 0) return rc;
        rc = rbfm.destroyFile(indexesName);
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
        std::vector<std::string> attributeNames = {"column-position"};
        scan(columnsName, "table-id", EQ_OP, &tableId, attributeNames, rm_ScanIterator);
        RID rid;
        int position = 0, nextPosition;
        while (rm_ScanIterator.getNextTuple(rid, positionBuffer) != RM_EOF) {
            std::memcpy(&nextPosition, (char *) positionBuffer + 1, sizeof(int));
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
        if (tableName == tablesName || tableName == columnsName || tableName == indexesName) return ERR_CATALOG_ILLEGAL_DELETE;
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
        while (rm_ScanIterator.getNextTuple(rid, tableIdBuffer) != RM_EOF) toBeDeleted.push_back(rid);
        rm_ScanIterator.close();
        for (const RID &ridBuffer : toBeDeleted) deleteFromSystemFiles(columnsName, ridBuffer);

        toBeDeleted.clear();
        int columnPosition;
        scan(indexesName, "table-id", EQ_OP, &tableId, {"column-position"}, rm_ScanIterator);
        while (rm_ScanIterator.getNextTuple(rid, tableIdBuffer) != RM_EOF) {
            std::memcpy(&columnPosition, (char *) tableIdBuffer + 1, sizeof(int));
            ix.destroyFile(getIndexFileName(tableName, columnPosition));
            toBeDeleted.push_back(rid);
        }
        rm_ScanIterator.close();
        for (const RID &ridBuffer : toBeDeleted) deleteFromSystemFiles(indexesName, ridBuffer);

        return rbfm.destroyFile(getFileName(tableName));
    }

    RC
    RelationManager::getVersionedAttributes(const std::string &tableName, int targetVersion, std::vector<Attribute> &attrs, std::vector<int> &positions) {
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
            positions.push_back(position);
        }

        rm_ScanIterator.close();

        return 0;
    }

    RC RelationManager::getAttributes(const std::string &tableName, std::vector<Attribute> &attrs) {
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        if (tableName == tablesName) {attrs = getTablesAttrs(); return 0;}
        if (tableName == columnsName) {attrs = getColumnsAttrs(); return 0;}
        if (tableName == indexesName) {attrs = getIndexesAttrs(); return 0;}
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        std::vector<int> positions;
        RC rc = getVersionedAttributes(tableName, fileHandle.version, attrs, positions);
        rbfm.closeFile(fileHandle);
        return rc;
    }

    RC RelationManager::insertTuple(const std::string &tableName, const void *data, RID &rid) {
        if (tableName == tablesName || tableName == columnsName || tableName == indexesName) return ERR_CATALOG_ILLEGAL_MODIFY;
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        std::vector<Attribute> attrs;
        std::vector<int> positions;
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        getVersionedAttributes(tableName, fileHandle.version, attrs, positions);
        RC rc = rbfm.insertRecord(fileHandle, attrs, data, rid);
        rbfm.closeFile(fileHandle);
        if (rc != 0) return rc;
        return modifyIndex(tableName, data, rid, attrs, positions);
    }

    RC RelationManager::deleteTuple(const std::string &tableName, const RID &rid) {
        if (tableName == tablesName || tableName == columnsName || tableName == indexesName) return ERR_CATALOG_ILLEGAL_MODIFY;
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        std::vector<Attribute> attrs;
        std::vector<int> positions;
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        getVersionedAttributes(tableName, fileHandle.version, attrs, positions);
        readTuple(tableName, rid, tupleBuffer);
        rbfm.openFile(getFileName(tableName), fileHandle);
        RC rc = rbfm.deleteRecord(fileHandle, attrs, rid);
        rbfm.closeFile(fileHandle);
        if (rc != 0) return rc;
        return modifyIndex(tableName, tupleBuffer, rid, attrs, positions, false);
    }

    RC RelationManager::updateTuple(const std::string &tableName, const void *data, const RID &rid) {
        if (tableName == tablesName || tableName == columnsName || tableName == indexesName) return ERR_CATALOG_ILLEGAL_MODIFY;
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        std::vector<Attribute> attrs;
        std::vector<int> positions;
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        getVersionedAttributes(tableName, fileHandle.version, attrs, positions);

        readTuple(tableName, rid, tupleBuffer);
        RC rc = modifyIndex(tableName, tupleBuffer, rid, attrs, positions, false);
        if (rc != 0) return rc;

        rc = rbfm.updateRecord(fileHandle, attrs, data, rid);
        rbfm.closeFile(fileHandle);
        if (rc != 0) return rc;

        return modifyIndex(tableName, data, rid, attrs, positions);
    }

    int RelationManager::getDataBufferSize(const std::vector<Attribute> &attrs) {
        int attrNum = attrs.size(), size = attrNum % 8 == 0 ? attrNum / 8 : attrNum / 8 + 1;
        for (const Attribute &attr : attrs) {
            size = size + attr.length;
            if (attr.type == 2) size = size + sizeof(int);
        }
        return size;
    }

    void RelationManager::convert(const std::string &tableName, int fromVersion, int toVersion, const void *dataBuffer,
                                int dataBufferLength, void *data) {
        if (fromVersion == toVersion) {
            std::memcpy(data, dataBuffer, dataBufferLength);
            return;
        }

        std::vector<Attribute> fromAttrs, toAttrs;
        std::vector<int> fromPositions, toPositions;

        getVersionedAttributes(tableName, fromVersion, fromAttrs, fromPositions);
        getVersionedAttributes(tableName, toVersion, toAttrs, toPositions);

        int fromAttrsLength = fromAttrs.size(), toAttrsLength = toAttrs.size();
        int fromBitmapBytes = fromAttrsLength % 8 == 0 ? fromAttrsLength / 8 : fromAttrsLength / 8 + 1;
        int toBitmapBytes = toAttrsLength % 8 == 0 ? toAttrsLength / 8 : toAttrsLength / 8 + 1;
        char fromBitmap[fromBitmapBytes], toBitmap[toBitmapBytes];
        std::memcpy(fromBitmap, dataBuffer, fromBitmapBytes);
        std::memset(toBitmap, 0, toBitmapBytes);

        int toPtr = toBitmapBytes;
        for (int toIndex = 0; toIndex < toAttrsLength; toIndex = toIndex + 1) {
            int fromIndex = 0, fromPtr = fromBitmapBytes;
            while (fromIndex < fromAttrsLength) {
                if (fromPositions[fromIndex] == toPositions[toIndex]) break;
                if (fromBitmap[fromIndex / 8] >> (7 - fromIndex % 8) & (unsigned) 1) {
                    fromIndex = fromIndex + 1;
                    continue;
                }
                if (fromAttrs[fromIndex].type == 2) {
                    int varCharLength;
                    std::memcpy(&varCharLength, (char *) dataBuffer + fromPtr, sizeof(int));
                    fromPtr = fromPtr + varCharLength;
                }
                fromPtr = fromPtr + sizeof(int);
                fromIndex = fromIndex + 1;
            }
            if (fromIndex >= fromAttrsLength || fromBitmap[fromIndex / 8] >> (7 - fromIndex % 8) & (unsigned) 1) {
                toBitmap[toIndex / 8] |= (unsigned) 1 << (7 - toIndex % 8);
                continue;
            }
            int length = 0;
            if (fromAttrs[fromIndex].type == 2) std::memcpy(&length, (char *) dataBuffer + fromPtr, sizeof(int));
            length = length + sizeof(int);
            std::memcpy((char *) data + toPtr, (char *) dataBuffer + fromPtr, length);
            toPtr = toPtr + length;
        }
        std::memcpy(data, toBitmap, toBitmapBytes);
    }

    RC RelationManager::readTuple(const std::string &tableName, const RID &rid, void *data) {
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        int version = fileHandle.version;
        std::vector<Attribute> attrs;
        int recordVersion = rbfm.getRecordVersion(fileHandle, rid);
        std::vector<int> positions;
        getVersionedAttributes(tableName, recordVersion, attrs, positions);
        int recordLength = getDataBufferSize(attrs);
        char recordBuffer[recordLength];
        RC rc = rbfm.readRecord(fileHandle, attrs, rid, recordBuffer);
        rbfm.closeFile(fileHandle);
        if (rc != 0) return rc;
        convert(tableName, recordVersion, version, recordBuffer, recordLength, data);
        return 0;
    }

    RC RelationManager::printTuple(const std::vector<Attribute> &attrs, const void *data, std::ostream &out) {
        return rbfm.printRecord(attrs, data, out);
    }

    RC RelationManager::readAttribute(const std::string &tableName, const RID &rid, const std::string &attributeName,
                                      void *data) {
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;

        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        int recordVersion = rbfm.getRecordVersion(fileHandle, rid);
        int curVersion = fileHandle.version;
        rbfm.closeFile(fileHandle);

        std::vector<Attribute> recordAttrs, curAttrs;
        std::vector<int> recordPositions, curPositions;

        getVersionedAttributes(tableName, recordVersion, recordAttrs, recordPositions);
        getVersionedAttributes(tableName, curVersion, curAttrs, curPositions);

        int curIndex = 0;
        while (curIndex < curAttrs.size()) {
            if (curAttrs[curIndex].name == attributeName) break;
            curIndex = curIndex + 1;
        }
        if (curIndex >= curAttrs.size()) return ERR_ATTRIBUTE_NOT_EXISTS;

        int recordIndex = 0;
        while (recordIndex < recordAttrs.size()) {
            if (recordPositions[recordIndex] == curPositions[curIndex]) break;
            recordIndex = recordIndex + 1;
        }

        if (recordIndex >= recordAttrs.size()) {
            unsigned char c = (unsigned char) 1 << 7;
            std::memcpy(data, &c, 1);
            return 0;
        }

        rbfm.openFile(getFileName(tableName), fileHandle);
        RC rc = rbfm.readAttribute(fileHandle, recordAttrs, rid, attributeName, data);
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
    RC RelationManager::modifyIndex(const std::string &tableName, const void *data, const RID &rid, const std::vector<Attribute> &attrs,
                                    const std::vector<int> &positions, bool insertion) {
        RC rc;
        int size = attrs.size();
        RID ridBuffer;
        IXFileHandle ixFileHandle;
        int numberOfNullBytes = size % 8 == 0 ? size / 8 : size / 8 + 1;
        char bitmaps[numberOfNullBytes];
        std::memcpy(bitmaps, data, numberOfNullBytes);
        int offset = numberOfNullBytes;
        for (int i = 0; i < size; i++) {
            int indexOfBitmap = i / 8;
            int offsetOfBitmap = i % 8;
            if (bitmaps[indexOfBitmap] >> (7 - offsetOfBitmap) & (unsigned) 1) continue;
            int keyLength;
            if (attrs[i].type != 2) keyLength = 4;
            else {
                std::memcpy(&keyLength, (char *) data + offset, sizeof(int));
                keyLength = keyLength + sizeof(int);
            }
            if (hasIndexOn(tableName, positions[i], ridBuffer)) {
                char key[keyLength];
                std::memcpy(&key, (char *) data + offset, keyLength);
                ix.openFile(getIndexFileName(tableName, positions[i]), ixFileHandle);
                if (insertion) rc = ix.insertEntry(ixFileHandle, attrs[i], key, rid);
                else rc = ix.deleteEntry(ixFileHandle, attrs[i], key, rid);
                ix.closeFile(ixFileHandle);
                if (rc != 0) return rc;
            }
            offset = offset + keyLength;
        }
        return 0;
    }

    int RelationManager::getColumnPosition(const std::string &tableName, const std::string &attributeName, Attribute &attrBuffer) {
        FileHandle fileHandle;
        rbfm.openFile(getFileName(tableName), fileHandle);
        std::vector<Attribute> attrs;
        std::vector<int> positions;
        getVersionedAttributes(tableName, fileHandle.version, attrs, positions);

        int size = attrs.size();
        for (int i = 0; i < size; i++) {
            if (attrs[i].name == attributeName) {
                rbfm.closeFile(fileHandle);
                attrBuffer = attrs[i];
                return positions[i];
            }
        }

        rbfm.closeFile(fileHandle);
        return -1;
    }

    std::string RelationManager::getIndexFileName(const std::string &tableName, int columnPosition) {
        return tableName + "_" + std::to_string(columnPosition) + indexSuffix;
    }

    bool RelationManager::hasIndexOn(const std::string &tableName, int columnPosition, RID &rid) {
        int tableId = getTableId(tableName);
        RM_ScanIterator rm_ScanIterator;
        scan(indexesName, "table-id", EQ_OP, &tableId, {"column-position"}, rm_ScanIterator);

        int columnPositionBuffer;
        while (rm_ScanIterator.getNextTuple(rid, tableIdBuffer) != RM_EOF) {
            std::memcpy(&columnPositionBuffer, (char *) tableIdBuffer + 1, sizeof(int));
            if (columnPositionBuffer == columnPosition) {
                rm_ScanIterator.close();
                return true;
            }
        }

        rm_ScanIterator.close();
        return false;
    }

    RC RelationManager::createIndex(const std::string &tableName, const std::string &attributeName){
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;

        Attribute attrBuffer;
        int columnPosition = getColumnPosition(tableName, attributeName, attrBuffer);
        if (columnPosition < 0) return ERR_INDEX_CREATE_ON_NON_EXISTS_COL;

        RID rid;
        if (hasIndexOn(tableName, columnPosition, rid)) return ERR_INDEX_CREATE_DUPLICATE;

        std::string indexFileName = getIndexFileName(tableName, columnPosition);
        RC rc = ix.createFile(indexFileName);
        if (rc != 0) return rc;

        FileHandle indexesHandle;
        rbfm.openFile(indexesName, indexesHandle);

        int tableId = getTableId(tableName);
        rc = insertIndexes(indexesHandle, tableId, columnPosition);
        if (rc != 0) return rc;

        rc = rbfm.closeFile(indexesHandle);
        if (rc != 0) return rc;

        RM_ScanIterator rm_ScanIterator;
        scan(tableName, "", NO_OP, nullptr, {attributeName}, rm_ScanIterator);
        IXFileHandle ixFileHandle;
        ix.openFile(indexFileName, ixFileHandle);
        while (rm_ScanIterator.getNextTuple(rid, tupleBuffer) != RM_EOF) {
            if (((char *) tupleBuffer)[0] >> 7u & 1u) continue;
            int keyLength = 0;
            if (attrBuffer.type == 2) std::memcpy(&keyLength, (char *) tupleBuffer + 1, sizeof(int));
            keyLength = keyLength + sizeof(int);
            char key[keyLength];
            std::memcpy(key, (char *) tupleBuffer + 1, keyLength);
            rc = ix.insertEntry(ixFileHandle, attrBuffer, key, rid);
            if (rc != 0) return rc;
        }
        ix.closeFile(ixFileHandle);
        rm_ScanIterator.close();

        return 0;
    }

    RC RelationManager::destroyIndex(const std::string &tableName, const std::string &attributeName){
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;

        Attribute attrBuffer;
        int columnPosition = getColumnPosition(tableName, attributeName, attrBuffer);
        if (columnPosition < 0) return ERR_INDEX_DELETE_ON_NON_EXISTS_COL;

        RID rid;
        if (!hasIndexOn(tableName, columnPosition, rid)) return ERR_INDEX_DELETE_ON_NON_EXISTS_INDEX;

        std::string indexFileName = getIndexFileName(tableName, columnPosition);
        RC rc = ix.destroyFile(indexFileName);
        if (rc != 0) return rc;

        return deleteFromSystemFiles(indexesName, rid);
    }

    // indexScan returns an iterator to allow the caller to go through qualified entries in index
    RC RelationManager::indexScan(const std::string &tableName,
                 const std::string &attributeName,
                 const void *lowKey,
                 const void *highKey,
                 bool lowKeyInclusive,
                 bool highKeyInclusive,
                 RM_IndexScanIterator &rm_IndexScanIterator){
        if (!checkTableExists(tableName)) return ERR_TABLE_NOT_EXISTS;

        Attribute attrBuffer;
        int columnPosition = getColumnPosition(tableName, attributeName, attrBuffer);
        if (columnPosition < 0) return ERR_INDEX_SCAN_ON_NON_EXISTS_COL;

        RID rid;
        if (!hasIndexOn(tableName, columnPosition, rid)) return ERR_INDEX_SCAN_ON_NON_EXISTS_INDEX;

        ix.openFile(getIndexFileName(tableName, columnPosition), rm_IndexScanIterator.ixFileHandle);
        return ix.scan(rm_IndexScanIterator.ixFileHandle, attrBuffer, lowKey, highKey, lowKeyInclusive, highKeyInclusive, rm_IndexScanIterator.ix_ScanIterator);
    }


    RM_IndexScanIterator::RM_IndexScanIterator() = default;

    RM_IndexScanIterator::~RM_IndexScanIterator() = default;

    RC RM_IndexScanIterator::getNextEntry(RID &rid, void *key){
        return ix_ScanIterator.getNextEntry(rid, key);
    }

    RC RM_IndexScanIterator::close(){
        RC rc = ix.closeFile(ixFileHandle);
        if (rc != 0) return 0;
        return ix_ScanIterator.close();
    }

} // namespace PeterDB