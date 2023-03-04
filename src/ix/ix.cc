#include "src/include/ix.h"

namespace PeterDB {
    IndexManager &IndexManager::instance() {
        static IndexManager _index_manager = IndexManager();
        return _index_manager;
    }

    PagedFileManager &pfm = PagedFileManager::instance();

    RC IndexManager::createFile(const std::string &fileName) {
        return pfm.createFile(fileName);
    }

    bool IndexManager::isLeaf(char *pageBuffer) {
        return pageBuffer[0] >> 7 & (unsigned) 1;
    }

    RC IndexManager::destroyFile(const std::string &fileName) {
        return pfm.destroyFile(fileName);
    }

    RC IndexManager::openFile(const std::string &fileName, IXFileHandle &ixFileHandle) {
        return pfm.openFile(fileName, ixFileHandle.fileHandle);
    }

    RC IndexManager::closeFile(IXFileHandle &ixFileHandle) {
        return pfm.closeFile(ixFileHandle.fileHandle);
    }

    RC IndexManager::writeRootPageNum(IXFileHandle &ixFileHandle) {
        char pointerBuffer[PAGE_SIZE];
        std::memset(pointerBuffer, 0, PAGE_SIZE);
        std::memcpy(pointerBuffer, &ixFileHandle.rootPageNum, sizeof(PageNum));
        std::memcpy(pointerBuffer + sizeof(PageNum), &ixFileHandle.keyType, sizeof(unsigned));
        return ixFileHandle.fileHandle.writePage(0, pointerBuffer);
    }

    RC IndexManager::readRootPageNum(IXFileHandle &ixFileHandle) {
        char pointerBuffer[PAGE_SIZE];
        RC rc = ixFileHandle.fileHandle.readPage(0, pointerBuffer);
        if (rc != 0) return rc;
        std::memcpy(&ixFileHandle.rootPageNum, pointerBuffer, sizeof(PageNum));
        std::memcpy(&ixFileHandle.keyType, pointerBuffer + sizeof(PageNum), sizeof(unsigned));
        return 0;
    }

    RC IndexManager::createNode(char *nodeBuffer) {
        std::memset(nodeBuffer, 0, PAGE_SIZE);
        PageIndex startOfFreeSpace = 1 + sizeof(PageIndex);
        std::memcpy(nodeBuffer + PAGE_SIZE - sizeof(PageIndex), &startOfFreeSpace, sizeof(PageIndex));
        return 0;
    }

    RC IndexManager::createLeaf(char *leafBuffer) {
        std::memset(leafBuffer, 0, PAGE_SIZE);
        leafBuffer[0] |= (unsigned) 1 << 7;
        PageIndex startOfFreeSpace = 1 + sizeof(PageIndex);
        std::memcpy(leafBuffer + PAGE_SIZE - sizeof(PageIndex), &startOfFreeSpace, sizeof(PageIndex));
        PageNum nextPage = UNDEFINED_PAGE_NUM;
        std::memcpy(leafBuffer + PAGE_SIZE - sizeof(PageIndex) - sizeof(PageNum), &nextPage, sizeof(PageNum));
        return 0;
    }

    PageIndex IndexManager::getStartOfFreeSpace(const char *pageBuffer) {
        PageIndex startOfFreeSpace;
        std::memcpy(&startOfFreeSpace, pageBuffer + PAGE_SIZE - sizeof(PageIndex), sizeof(PageIndex));
        return startOfFreeSpace;
    }

    PageIndex IndexManager::getKeyListSize(const char *pageBuffer) {
        PageIndex keyListSize;
        std::memcpy(&keyListSize, pageBuffer + 1, sizeof(PageIndex));
        return keyListSize;
    }

    PageIndex IndexManager::getKeyLength(const void *key, unsigned keyType) {
        int length = 0;
        if (keyType == 2) std::memcpy(&length, key, sizeof(int));
        return length + sizeof(int);
    }

    PageIndex IndexManager::getCompKeyLength(const void *key, unsigned keyType) {
        return getKeyLength(key, keyType) + RID_SIZE;
    }

    PageNum IndexManager::getNextPageNum(const char *leafBuffer) {
        PageNum nextPageNum;
        std::memcpy(&nextPageNum, leafBuffer + PAGE_SIZE - sizeof(PageIndex) - sizeof(PageNum), sizeof(PageNum));
        return nextPageNum;
    }

    void IndexManager::getRID(const char *pageBuffer, RID &ridBuffer) {
        std::memcpy(&ridBuffer.pageNum, pageBuffer, sizeof(PageNum));
        std::memcpy(&ridBuffer.slotNum, pageBuffer + sizeof(PageNum), sizeof(SlotNum));
    }

    int IndexManager::compareKey(const char *pageBuffer, const void *key, unsigned keyType) {
        if (keyType == 0) {
            int intBuffer, intKey;
            std::memcpy(&intBuffer, pageBuffer, sizeof(int));
            std::memcpy(&intKey, key, sizeof(int));
            return intBuffer - intKey;
        } else if (keyType == 1) {
            float floatBuffer, floatKey;
            std::memcpy(&floatBuffer, pageBuffer, sizeof(float));
            std::memcpy(&floatKey, key, sizeof(float));
            if (floatBuffer < floatKey) return -1;
            if (floatBuffer > floatKey) return 1;
            return 0;
        } else {
            int lengthBuffer, keyLength;
            std::memcpy(&lengthBuffer, pageBuffer, sizeof(int));
            std::memcpy(&keyLength, key, sizeof(int));
            char varCharBuffer[lengthBuffer + 1];
            std::memset(varCharBuffer, 0, lengthBuffer + 1);
            std::memcpy(varCharBuffer, pageBuffer + sizeof(int), lengthBuffer);
            char varCharKey[keyLength + 1];
            std::memset(varCharKey, 0, keyLength + 1);
            std::memcpy(varCharKey, (char *) key + sizeof(int), keyLength);
            return std::strcmp(varCharBuffer, varCharKey);
        }
    }

    int IndexManager::compareRID(const RID &ridBuffer, const RID &rid) {
        if (ridBuffer.pageNum != rid.pageNum) {
            if (ridBuffer.pageNum > rid.pageNum) return 1;
            return -1;
        }
        return ridBuffer.slotNum - rid.slotNum;
    }

    int IndexManager::compareCompKey(const char *pageBuffer, const void *key, const RID &rid,
                                     unsigned keyType) {
        int cmp = compareKey(pageBuffer, key, keyType);
        if (cmp != 0) return cmp;
        RID ridBuffer;
        getRID(pageBuffer + getKeyLength(key, keyType), ridBuffer);
        return compareRID(ridBuffer, rid);
    }

    int IndexManager::findInLeaf(const char *leafBuffer, const void *key, const RID &rid, unsigned keyType, PageIndex &index) {
        index = 0;
        PageIndex keyListLength = getKeyListSize(leafBuffer);
        int offset = 1 + sizeof(PageIndex);
        while (index < keyListLength) {
            int cmp = compareCompKey(leafBuffer + offset, key, rid, keyType);
            if (cmp == 0) return -offset;
            if (cmp > 0) break;
            offset = offset + getCompKeyLength(leafBuffer + offset, keyType);
            index = index + 1;
        }
        return offset;
    }

    bool IndexManager::hasNodeSpace(const char *nodeBuffer, int keyLength) {
        PageIndex startOfFreeSpace = getStartOfFreeSpace(nodeBuffer);
        PageIndex freeSpace = PAGE_SIZE - startOfFreeSpace - sizeof(PageIndex);
        return freeSpace >= sizeof(PageNum) + keyLength + RID_SIZE;
    }

    bool IndexManager::hasLeafSpace(const char *leafBuffer, const void *key, unsigned keyType) {
        PageIndex startOfFreeSpace = getStartOfFreeSpace(leafBuffer);
        PageIndex freeSpace = PAGE_SIZE - startOfFreeSpace - sizeof(PageNum) - sizeof(PageIndex);
        return freeSpace >= getCompKeyLength(key, keyType);
    }

    void IndexManager::insertNode(char *nodeBuffer, int pageNumOffset, int keyOffset, const Entry &entry) {
        PageIndex startOfFreeSpace = getStartOfFreeSpace(nodeBuffer);
        std::memmove(nodeBuffer + keyOffset + entry.keyLength + RID_SIZE, nodeBuffer + keyOffset, startOfFreeSpace - keyOffset);
        std::memcpy(nodeBuffer + keyOffset, entry.key, entry.keyLength);
        std::memcpy(nodeBuffer + keyOffset + entry.keyLength, &entry.rid.pageNum, sizeof(PageNum));
        std::memcpy(nodeBuffer + keyOffset + entry.keyLength + sizeof(PageNum), &entry.rid.slotNum, sizeof(SlotNum));
        startOfFreeSpace = startOfFreeSpace + entry.keyLength + RID_SIZE;
        std::memmove(nodeBuffer + pageNumOffset + sizeof(PageNum), nodeBuffer + pageNumOffset, startOfFreeSpace - pageNumOffset);
        std::memcpy(nodeBuffer + pageNumOffset, &entry.nodeNum, sizeof(PageNum));
        PageIndex keyListSize = getKeyListSize(nodeBuffer) + 1;
        std::memcpy(nodeBuffer + 1, &keyListSize, sizeof(PageIndex));
        startOfFreeSpace = startOfFreeSpace + sizeof(PageNum);
        std::memcpy(nodeBuffer + PAGE_SIZE - sizeof(PageIndex), &startOfFreeSpace, sizeof(PageIndex));
    }

    void IndexManager::insertLeaf(char *leafBuffer, int offset, const void *key, const RID &rid, unsigned keyType) {
        PageIndex compKeyLength = getCompKeyLength(key, keyType);
        PageIndex keyLength = compKeyLength - RID_SIZE;
        PageIndex startOfFreeSpace = getStartOfFreeSpace(leafBuffer);
        std::memmove(leafBuffer + offset + compKeyLength, leafBuffer + offset, startOfFreeSpace - offset);
        std::memcpy(leafBuffer + offset, key, keyLength);
        std::memcpy(leafBuffer + offset + keyLength, &rid.pageNum, sizeof(PageNum));
        std::memcpy(leafBuffer + offset + keyLength + sizeof(PageNum), &rid.slotNum, sizeof(SlotNum));
        PageIndex keyListSize = getKeyListSize(leafBuffer) + 1;
        std::memcpy(leafBuffer + 1, &keyListSize, sizeof(PageIndex));
        startOfFreeSpace = startOfFreeSpace + compKeyLength;
        std::memcpy(leafBuffer + PAGE_SIZE - sizeof(PageIndex), &startOfFreeSpace, sizeof(PageIndex));
    }

    void IndexManager::getMidNode(const char *nodeBuffer, unsigned int keyType, PageIndex &midPageNumOffset,
                                  PageIndex &midKeyOffset, PageIndex &oldListSize, PageIndex &newListSize) {
        PageIndex keyListLength = getKeyListSize(nodeBuffer);
        midPageNumOffset = 1 + sizeof(PageIndex) + sizeof(PageNum);
        midKeyOffset = 1 + sizeof(PageIndex) + (keyListLength + 1) * sizeof(PageNum);
        if (keyType != 2) {
            PageIndex midKeyIndex = keyListLength / 2;
            oldListSize = midKeyIndex;
            newListSize = keyListLength - oldListSize - 1;
            midPageNumOffset = midPageNumOffset + midKeyIndex * sizeof(PageNum);
            midKeyOffset = midKeyOffset + midKeyIndex * (sizeof(int) + RID_SIZE);
            return;
        }
        PageIndex index = 0;
        oldListSize = 0;
        PageIndex OVER_HEAD = 1 + sizeof(PageIndex) + (keyListLength + 1) * sizeof(PageNum);
        PageIndex HALF_PAGE_SIZE = OVER_HEAD + (PAGE_SIZE - OVER_HEAD) / 2;
        while (index < keyListLength && midKeyOffset < HALF_PAGE_SIZE) {
            oldListSize = oldListSize + 1;
            midPageNumOffset = midPageNumOffset + sizeof(PageNum);
            midKeyOffset = midKeyOffset + getCompKeyLength(nodeBuffer + midKeyOffset, keyType);
            index = index + 1;
        }
        newListSize = keyListLength - oldListSize - 1;
    }

    PageIndex IndexManager::getLeafMidKey(const char *leafBuffer, unsigned keyType, PageIndex &oldListSize) {
        PageIndex keyListLength = getKeyListSize(leafBuffer);
        PageIndex offset = 1 + sizeof(PageIndex);
        if (keyType != 2) {
            oldListSize = keyListLength / 2;
            return offset + oldListSize * (sizeof(int) + RID_SIZE);
        }
        PageIndex index = 0;
        oldListSize = 0;
        while (index < keyListLength && offset < PAGE_SIZE / 2) {
            oldListSize = oldListSize + 1;
            offset = offset + getCompKeyLength(leafBuffer + offset, keyType);
            index = index + 1;
        }
        return offset;
    }

    RC IndexManager::createNewRoot(IXFileHandle &ixFileHandle, const Entry &childEntry) {
        char newRootBuffer[PAGE_SIZE];
        createNode(newRootBuffer);
        PageIndex keyListSize = 1, offset = 1;
        std::memcpy(newRootBuffer + offset, &keyListSize, sizeof(PageIndex));
        offset = offset + sizeof(PageIndex);
        std::memcpy(newRootBuffer + offset, &ixFileHandle.rootPageNum, sizeof(PageNum));
        offset = offset + sizeof(PageNum);
        std::memcpy(newRootBuffer + offset, &childEntry.nodeNum, sizeof(PageNum));
        offset = offset + sizeof(PageNum);
        std::memcpy(newRootBuffer + offset, childEntry.key, childEntry.keyLength);
        offset = offset + childEntry.keyLength;
        std::memcpy(newRootBuffer + offset, &childEntry.rid.pageNum, sizeof(PageNum));
        offset = offset + sizeof(PageNum);
        std::memcpy(newRootBuffer + offset, &childEntry.rid.slotNum, sizeof(SlotNum));
        offset = offset + sizeof(SlotNum);
        std::memcpy(newRootBuffer + PAGE_SIZE - sizeof(PageIndex), &offset, sizeof(PageIndex));
        ixFileHandle.rootPageNum = ixFileHandle.fileHandle.numberOfPages;
        RC rc = writeRootPageNum(ixFileHandle);
        if (rc != 0) return rc;
        return ixFileHandle.fileHandle.appendPage(newRootBuffer);
    }

    RC IndexManager::insert(IXFileHandle &ixFileHandle, PageNum nodeNum, const void *key, const RID &rid,
                            Entry &childEntry, unsigned keyType) {
        RC rc;
        char pageBuffer[PAGE_SIZE];
        rc = ixFileHandle.fileHandle.readPage(nodeNum, pageBuffer);
        if (rc != 0) return rc;

        if (!isLeaf(pageBuffer)) {
            int pageNumOffset, keyOffset;
            PageNum pageNum = findInNode(pageBuffer, key, keyType, pageNumOffset, keyOffset, rid);
            rc = insert(ixFileHandle, pageNum, key, rid, childEntry, keyType);
            if (rc != 0) return rc;
            if (childEntry.isNull) return 0;
            if (hasNodeSpace(pageBuffer, childEntry.keyLength)) {
                insertNode(pageBuffer, pageNumOffset, keyOffset, childEntry);
                childEntry.isNull = true;
            } else {
                char newNodeBuffer[PAGE_SIZE];
                createNode(newNodeBuffer);

                PageIndex keyListSize = getKeyListSize(pageBuffer);
                PageIndex midPageNumOffset, midKeyOffset, oldListSize, newListSize;
                getMidNode(pageBuffer, keyType, midPageNumOffset, midKeyOffset, oldListSize, newListSize);

                Entry childEntryCopy(childEntry);

                int midKeyLength = getKeyLength(pageBuffer + midKeyOffset, keyType);
                delete[] childEntry.key;
                childEntry.key = new char[midKeyLength];
                std::memcpy(childEntry.key, pageBuffer + midKeyOffset, midKeyLength);
                childEntry.keyLength = midKeyLength;
                childEntry.isNull = false;
                childEntry.nodeNum = ixFileHandle.fileHandle.numberOfPages;;
                getRID(pageBuffer + midKeyOffset + midKeyLength, childEntry.rid);

                PageIndex startOfFreeSpace = getStartOfFreeSpace(pageBuffer);
                std::memcpy(pageBuffer + 1, &oldListSize, sizeof(PageIndex));
                std::memcpy(newNodeBuffer + 1, &newListSize, sizeof(PageIndex));
                std::memcpy(newNodeBuffer + 1 + sizeof(PageIndex), pageBuffer + midPageNumOffset, (newListSize + 1) * sizeof(PageNum));
                std::memcpy(newNodeBuffer + 1 + sizeof(PageIndex) + (newListSize + 1) * sizeof(PageNum), pageBuffer + midKeyOffset + midKeyLength + RID_SIZE, startOfFreeSpace - midKeyOffset - midKeyLength - RID_SIZE);
                PageIndex newStartOfFreeSpace = 1 + sizeof(PageIndex) + (newListSize + 1) * sizeof(PageNum) + startOfFreeSpace - midKeyOffset - midKeyLength - RID_SIZE;
                std::memcpy(newNodeBuffer + PAGE_SIZE - sizeof(PageIndex), &newStartOfFreeSpace, sizeof(PageIndex));
                std::memmove(pageBuffer + midPageNumOffset, pageBuffer + 1 + sizeof(PageIndex) + (keyListSize + 1) * sizeof(PageNum), midKeyOffset - 1 - sizeof(PageIndex) - (keyListSize + 1) * sizeof(PageNum));
                PageIndex oldStartOfFreeSpace = startOfFreeSpace - midKeyLength - RID_SIZE - newStartOfFreeSpace + 1 + sizeof(PageIndex);
                std::memset(pageBuffer + oldStartOfFreeSpace, 0, startOfFreeSpace - oldStartOfFreeSpace);
                std::memcpy(pageBuffer + PAGE_SIZE - sizeof(PageIndex), &oldStartOfFreeSpace, sizeof(PageIndex));

                if (keyOffset <= midKeyOffset) insertNode(pageBuffer, pageNumOffset, keyOffset - (keyListSize - oldListSize) * sizeof(PageNum), childEntryCopy);
                else insertNode(newNodeBuffer, pageNumOffset - (oldListSize + 1) * sizeof(PageNum), keyOffset - midKeyOffset - midKeyLength - RID_SIZE + (newListSize + 1) * sizeof(PageNum), childEntryCopy);
                delete[] childEntryCopy.key;

                rc = ixFileHandle.fileHandle.appendPage(newNodeBuffer);
                if (rc != 0) return rc;

                if (nodeNum == ixFileHandle.rootPageNum) {
                    rc = createNewRoot(ixFileHandle, childEntry);
                    if (rc != 0) return rc;
                }
            }
        } else {
            PageIndex index;
            int offset = findInLeaf(pageBuffer, key, rid, keyType, index);
            if (offset < 0) return ERR_INDEX_INSERT_DUPLICATE;
            if (hasLeafSpace(pageBuffer, key, keyType)) {
                insertLeaf(pageBuffer, offset, key, rid, keyType);
                childEntry.isNull = true;
            } else {
                char newLeafBuffer[PAGE_SIZE];
                createLeaf(newLeafBuffer);

                PageNum nextPage = getNextPageNum(pageBuffer);
                std::memcpy(newLeafBuffer + PAGE_SIZE - sizeof(PageIndex) - sizeof(PageNum), &nextPage, sizeof(PageNum));
                PageNum newPage = ixFileHandle.fileHandle.numberOfPages;
                std::memcpy(pageBuffer + PAGE_SIZE - sizeof(PageIndex) - sizeof(PageNum), &newPage, sizeof(PageNum));

                PageIndex keyListSize = getKeyListSize(pageBuffer);
                PageIndex oldListSize;
                PageIndex midKeyOffset = getLeafMidKey(pageBuffer, keyType, oldListSize);
                PageIndex newListSize = keyListSize - oldListSize;
                std::memcpy(pageBuffer + 1, &oldListSize, sizeof(PageIndex));
                PageIndex startOfFreeSpace = getStartOfFreeSpace(pageBuffer);
                std::memcpy(pageBuffer + PAGE_SIZE - sizeof(PageIndex), &midKeyOffset, sizeof(PageIndex));
                std::memcpy(newLeafBuffer + 1, &newListSize, sizeof(PageIndex));
                std::memcpy(newLeafBuffer + 1 + sizeof(PageIndex), pageBuffer + midKeyOffset, startOfFreeSpace - midKeyOffset);
                std::memset(pageBuffer + midKeyOffset, 0, startOfFreeSpace - midKeyOffset);
                PageIndex newStartOfFreeSpace = 1 + sizeof(PageIndex) + startOfFreeSpace - midKeyOffset;
                std::memcpy(newLeafBuffer + PAGE_SIZE - sizeof(PageIndex), &newStartOfFreeSpace, sizeof(PageIndex));

                if (offset <= midKeyOffset) insertLeaf(pageBuffer, offset, key, rid, keyType);
                else insertLeaf(newLeafBuffer, 1 + sizeof(PageIndex) + offset - midKeyOffset, key, rid, keyType);

                int length = getKeyLength(newLeafBuffer + 1 + sizeof(PageIndex), keyType);
                delete[] childEntry.key;
                childEntry.key = new char[length];
                std::memcpy(childEntry.key, newLeafBuffer + 1 + sizeof(PageIndex), length);
                childEntry.keyLength = length;
                childEntry.isNull = false;
                childEntry.nodeNum = newPage;
                getRID(newLeafBuffer + 1 + sizeof(PageIndex) + length, childEntry.rid);

                rc = ixFileHandle.fileHandle.appendPage(newLeafBuffer);
                if (rc != 0) return rc;

                if (nodeNum == ixFileHandle.rootPageNum) {
                    rc = createNewRoot(ixFileHandle, childEntry);
                    if (rc != 0) return rc;
                }
            }
        }
        return ixFileHandle.fileHandle.writePage(nodeNum, pageBuffer);
    }

    RC
    IndexManager::insertEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        RC rc;
        if (ixFileHandle.fileHandle.numberOfPages == 0) {
            ixFileHandle.rootPageNum = 1;
            ixFileHandle.keyType = attribute.type;
            char pointerBuffer[PAGE_SIZE];
            std::memset(pointerBuffer, 0, PAGE_SIZE);
            std::memcpy(pointerBuffer, &ixFileHandle.rootPageNum, sizeof(PageNum));
            std::memcpy(pointerBuffer + sizeof(PageNum), &ixFileHandle.keyType, sizeof(unsigned));
            rc = ixFileHandle.fileHandle.appendPage(pointerBuffer);
            if (rc != 0) return rc;
            char leafBuffer[PAGE_SIZE];
            createLeaf(leafBuffer);
            insertLeaf(leafBuffer, 1 + sizeof(PageIndex), key, rid, attribute.type);
            return ixFileHandle.fileHandle.appendPage(leafBuffer);
        }
        if (attribute.type != ixFileHandle.keyType) return ERR_INDEX_UNMATCHED_TYPE;
        readRootPageNum(ixFileHandle);
        Entry childEntry;
        rc =  insert(ixFileHandle, ixFileHandle.rootPageNum, key, rid, childEntry, attribute.type);
        delete[] childEntry.key;
        return rc;
    }

    PageNum IndexManager::getLeftMostLeaf(IXFileHandle &ixFileHandle, char *leafBuffer) {
        ixFileHandle.fileHandle.readPage(ixFileHandle.rootPageNum, leafBuffer);
        PageNum leftMostLeaf = ixFileHandle.rootPageNum;
        while (!isLeaf(leafBuffer)) {
            std::memcpy(&leftMostLeaf, leafBuffer + 1 + sizeof(PageIndex), sizeof(PageNum));
            ixFileHandle.fileHandle.readPage(leftMostLeaf, leafBuffer);
        }
        return leftMostLeaf;
    }

    PageNum IndexManager::getLeafByKey(IXFileHandle &ixFileHandle, const void *key, unsigned keyType, char *leafBuffer, const RID &rid) {
        if (key == nullptr) return getLeftMostLeaf(ixFileHandle, leafBuffer);

        ixFileHandle.fileHandle.readPage(ixFileHandle.rootPageNum, leafBuffer);
        PageNum pageNum = ixFileHandle.rootPageNum;

        int pageNumOffset, keyOffset;
        while (!isLeaf(leafBuffer)) {
            pageNum = findInNode(leafBuffer, key, keyType, pageNumOffset, keyOffset, rid);
            ixFileHandle.fileHandle.readPage(pageNum, leafBuffer);
        }

        return pageNum;
    }

    RC
    IndexManager::deleteEntry(IXFileHandle &ixFileHandle, const Attribute &attribute, const void *key, const RID &rid) {
        readRootPageNum(ixFileHandle);
        char leafBuffer[PAGE_SIZE];
        PageNum leafPageNum = getLeafByKey(ixFileHandle, key, attribute.type, leafBuffer, rid);
        PageIndex index;
        int offset = findInLeaf(leafBuffer, key, rid, attribute.type, index);
        if (offset >= 0) return ERR_INDEX_DELETE_NOT_EXISTS;
        offset = - offset;
        PageIndex keyListSize = getKeyListSize(leafBuffer) - 1;
        std::memcpy(leafBuffer + 1, &keyListSize, sizeof(PageIndex));
        PageIndex startOfFreeSpace = getStartOfFreeSpace(leafBuffer);
        PageIndex compKeyLength = getCompKeyLength(leafBuffer + offset, attribute.type);
        std::memmove(leafBuffer + offset, leafBuffer + offset + compKeyLength, startOfFreeSpace - offset - compKeyLength);
        std::memset(leafBuffer + startOfFreeSpace - compKeyLength, 0, compKeyLength);
        startOfFreeSpace = startOfFreeSpace - compKeyLength;
        std::memcpy(leafBuffer + PAGE_SIZE - sizeof(PageIndex), &startOfFreeSpace, sizeof(PageIndex));
        return ixFileHandle.fileHandle.writePage(leafPageNum, leafBuffer);
    }

    int IndexManager::printBTreeKey(const char *pageBuffer, std::ostream &out, unsigned int keyType) const {
        int length = 0;
        if (keyType == 2) std::memcpy(&length, pageBuffer, sizeof(int));

        switch (keyType) {
            case 0:
                int intBuffer;
                std::memcpy(&intBuffer, pageBuffer, sizeof(int));
                out << intBuffer;
                break;
            case 1:
                float floatBuffer;
                std::memcpy(&floatBuffer, pageBuffer, sizeof(float));
                out << floatBuffer;
                break;
            default:
                char varCharBuffer[length + 1];
                std::memset(varCharBuffer, 0, length + 1);
                std::memcpy(varCharBuffer, pageBuffer, length);
                std::string varChar(varCharBuffer);
                out << varChar;
        }

        return length + sizeof(int);
    }

    void IndexManager::printBTreeRID(const char *pageBuffer, std::ostream &out) const {
        RID ridBuffer;
        std::memcpy(&ridBuffer.pageNum, pageBuffer, sizeof(PageNum));
        std::memcpy(&ridBuffer.slotNum, pageBuffer + sizeof(PageNum), sizeof(SlotNum));

        out << "(" << ridBuffer.pageNum << "," << ridBuffer.slotNum << ")";
    }

    int IndexManager::printBTreeNodeEntry(const char *nodeBuffer, std::ostream &out, unsigned keyType) const {
        out << "\"(";

        int lengthKey = printBTreeKey(nodeBuffer, out, keyType);

        out << ",";

        printBTreeRID(nodeBuffer + lengthKey, out);

        out << ")\"";
        return lengthKey + RID_SIZE;
    }

    int IndexManager::printBTreeLeafEntry(const char *leafBuffer, std::ostream &out, const PageIndex &keyListSize, PageIndex &index, unsigned keyType) const {
        out << "\"";

        int offset = 0;
        int keyLength = const_cast<IndexManager *>(this)->getKeyLength(leafBuffer, keyType);
        char keyBuffer[keyLength];
        std::memcpy(keyBuffer, leafBuffer, keyLength);

        offset = offset + printBTreeKey(leafBuffer, out, keyType);

        out << ":[";

        printBTreeRID(leafBuffer + offset, out);
        offset = offset + RID_SIZE;
        index = index + 1;

        while (index < keyListSize && const_cast<IndexManager *>(this)->compareKey(leafBuffer + offset, keyBuffer, keyType) == 0) {
            out << ",";

            offset = offset + keyLength;
            printBTreeRID(leafBuffer + offset, out);
            offset = offset + RID_SIZE;
            index = index + 1;
        }

        out << "]\"";
        return offset;
    }

    RC IndexManager::printBTreeNode(IXFileHandle &ixFileHandle, PageNum nodeNum, std::ostream &out) const {
        RC rc;
        char pageBuffer[PAGE_SIZE];
        rc = ixFileHandle.fileHandle.readPage(nodeNum, pageBuffer);
        if (rc != 0) return rc;

        PageIndex index, offset;
        PageIndex keyListSize = const_cast<IndexManager *>(this)->getKeyListSize(pageBuffer);

        out << "{\"keys\":";

        if (keyListSize == 0) {
            out << "[]}";
            return 0;
        }

        out << "[";

        if ((pageBuffer[0] >> 7 & (unsigned) 1) == 0) {
            offset = 1 + sizeof(PageIndex) + (keyListSize + 1) * sizeof(PageNum);
            offset = offset + printBTreeNodeEntry(pageBuffer + offset, out, ixFileHandle.keyType);
            index = 1;
            while (index < keyListSize) {
                out << ",";
                offset = offset + printBTreeNodeEntry(pageBuffer + offset, out, ixFileHandle.keyType);
                index = index + 1;
            }

            out << "],\"children\":[";

            PageNum pageNum;
            offset = 1 + sizeof(PageIndex);
            std::memcpy(&pageNum, pageBuffer + offset, sizeof(PageNum));
            rc = printBTreeNode(ixFileHandle, pageNum, out);
            if (rc != 0) return rc;
            offset = offset + sizeof(PageNum);
            index = 0;
            while (index < keyListSize) {
                out << ",";
                std::memcpy(&pageNum, pageBuffer + offset, sizeof(PageNum));
                rc = printBTreeNode(ixFileHandle, pageNum, out);
                if (rc != 0) return rc;
                offset = offset + sizeof(PageNum);
                index = index + 1;
            }
        } else {
            index = 0;
            offset = 1 + sizeof(PageIndex);
            offset = offset + printBTreeLeafEntry(pageBuffer + offset, out, keyListSize, index, ixFileHandle.keyType);

            while (index < keyListSize) {
                out << ",";
                offset = offset + printBTreeLeafEntry(pageBuffer + offset, out, keyListSize, index, ixFileHandle.keyType);
            }
        }

        out << "]}";

        return 0;
    }

    RC IndexManager::printBTree(IXFileHandle &ixFileHandle, const Attribute &attribute, std::ostream &out) const {
        if (attribute.type != ixFileHandle.keyType) return ERR_INDEX_UNMATCHED_TYPE;
        return printBTreeNode(ixFileHandle, ixFileHandle.rootPageNum, out);
    }

    PageNum IndexManager::findInNode(const char *nodeBuffer, const void *key, unsigned keyType, int &pageNumOffset, int &keyOffset, const RID &rid) {
        int keyListSize = getKeyListSize(nodeBuffer);
        pageNumOffset = 1 + sizeof(PageIndex);
        keyOffset = pageNumOffset + (keyListSize + 1) * sizeof(PageNum);
        int keyIndex = 0, cmp;
        while (keyIndex < keyListSize) {
            cmp = compareCompKey(nodeBuffer + keyOffset, key, rid, keyType);
            if (cmp > 0) break;
            pageNumOffset = pageNumOffset + sizeof(PageNum);
            keyOffset = keyOffset + getCompKeyLength(nodeBuffer + keyOffset, keyType);
            keyIndex = keyIndex + 1;
            if (cmp == 0) break;
        }
        PageNum pageNum;
        std::memcpy(&pageNum, nodeBuffer + pageNumOffset, sizeof(PageNum));
        pageNumOffset = pageNumOffset + sizeof(PageNum);
        return pageNum;
    }

    RC IndexManager::scan(IXFileHandle &ixFileHandle,
                          const Attribute &attribute,
                          const void *lowKey,
                          const void *highKey,
                          bool lowKeyInclusive,
                          bool highKeyInclusive,
                          IX_ScanIterator &ix_ScanIterator) {
        if (ixFileHandle.fileHandle.pFile == nullptr) return ERR_INDEX_FILE_NOT_EXISTS;

        delete[] ix_ScanIterator.leafBuffer;
        ix_ScanIterator.leafBuffer = new char[PAGE_SIZE];
        ix_ScanIterator.ixFileHandle = &ixFileHandle;
        ix_ScanIterator.keyType = attribute.type;
        ix_ScanIterator.highKey = nullptr;
        if (highKey) {
            int highKeyLength = getKeyLength(highKey, attribute.type);
            ix_ScanIterator.highKey = new char[highKeyLength];
            std::memcpy(ix_ScanIterator.highKey, highKey, highKeyLength);
        }
        ix_ScanIterator.highKeyInclusive = highKeyInclusive;

        readRootPageNum(ixFileHandle);
        RID rid(0, 0);
        getLeafByKey(ixFileHandle, lowKey, attribute.type, ix_ScanIterator.leafBuffer, rid);
        ix_ScanIterator.keyListSize = getKeyListSize(ix_ScanIterator.leafBuffer);
        ix_ScanIterator.index = 0;
        ix_ScanIterator.offset = 1 + sizeof(PageIndex);

        if (lowKey == nullptr) return 0;

        while (ix_ScanIterator.index < ix_ScanIterator.keyListSize) {
            int cmp = compareCompKey(ix_ScanIterator.leafBuffer + ix_ScanIterator.offset, lowKey, rid, attribute.type);
            if (cmp > 0 || lowKeyInclusive && cmp == 0) break;
            ix_ScanIterator.offset = ix_ScanIterator.offset + getCompKeyLength(ix_ScanIterator.leafBuffer + ix_ScanIterator.offset, attribute.type);
            ix_ScanIterator.index = ix_ScanIterator.index + 1;
        }

        return 0;
    }

    IX_ScanIterator::IX_ScanIterator() {
        leafBuffer = new char[PAGE_SIZE];
    }

    IX_ScanIterator::~IX_ScanIterator() {
        close();
    }

    RC IX_ScanIterator::getNextEntry(RID &rid, void *key) {
        if (index >= keyListSize) {
            PageNum nextPageNum = ix.getNextPageNum(leafBuffer);
            if (nextPageNum == UNDEFINED_PAGE_NUM) return IX_EOF;
            ixFileHandle->fileHandle.readPage(nextPageNum, leafBuffer);
            keyListSize = ix.getKeyListSize(leafBuffer);
            index = 0;
            offset = 1 + sizeof(PageIndex);
        }
        if (highKey) {
            if (highKeyInclusive) {
                RID highKeyRid(UNDEFINED_PAGE_NUM, USHRT_MAX);
                int cmp = ix.compareCompKey(leafBuffer + offset, highKey, highKeyRid, keyType);
                if (cmp > 0) return IX_EOF;
            } else {
                RID highKeyRid(0, 0);
                int cmp = ix.compareCompKey(leafBuffer + offset, highKey, highKeyRid, keyType);
                if (cmp >= 0) return IX_EOF;
            }
        }
        int keyLength = ix.getKeyLength(leafBuffer + offset, keyType);
        std::memcpy(key, leafBuffer + offset, keyLength);
        offset = offset + keyLength;
        ix.getRID(leafBuffer + offset, rid);
        offset = offset + RID_SIZE;
        index = index + 1;
        return 0;
    }

    RC IX_ScanIterator::close() {
        delete[] leafBuffer;
        leafBuffer = nullptr;
        delete[] highKey;
        highKey = nullptr;
        return 0;
    }

    IXFileHandle::IXFileHandle() {
        ixReadPageCounter = 0;
        ixWritePageCounter = 0;
        ixAppendPageCounter = 0;
    }

    IXFileHandle::~IXFileHandle() {
    }

    RC
    IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        fileHandle.collectCounterValues(ixReadPageCounter, ixWritePageCounter, ixAppendPageCounter);
        readPageCount = ixReadPageCounter;
        writePageCount = ixWritePageCounter;
        appendPageCount = ixAppendPageCounter;
        return 0;
    }

} // namespace PeterDB