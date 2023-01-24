#include "src/include/rbfm.h"

namespace PeterDB {
    RecordBasedFileManager &RecordBasedFileManager::instance() {
        static RecordBasedFileManager _rbf_manager = RecordBasedFileManager();
        return _rbf_manager;
    }

    RecordBasedFileManager::RecordBasedFileManager() = default;

    RecordBasedFileManager::~RecordBasedFileManager() = default;

    RecordBasedFileManager::RecordBasedFileManager(const RecordBasedFileManager &) = default;

    RecordBasedFileManager &RecordBasedFileManager::operator=(const RecordBasedFileManager &) = default;

    PeterDB::PagedFileManager &pfm = PeterDB::PagedFileManager::instance();

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return pfm.createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return pfm.destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return pfm.openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return pfm.closeFile(fileHandle);
    }

    RC RecordBasedFileManager::toPageBuffer(FileHandle &fileHandle, unsigned pageNum) {
        return fileHandle.readPage(pageNum, fileHandle.pageBuffer);
    }

    RC RecordBasedFileManager::appendEmptyPage(FileHandle &fileHandle) {
        memset(fileHandle.pageBuffer, 0, PAGE_SIZE);
        RC rc = fileHandle.appendPage(fileHandle.pageBuffer);
        return rc;
    }

    unsigned short RecordBasedFileManager::getNumberOfSlot(const void *pageBuffer) {
        unsigned short numberOfSlot;
        std::memcpy(&numberOfSlot, (char *) pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short), sizeof(unsigned short));
        return numberOfSlot;
    }

    std::vector<Slot> RecordBasedFileManager::getSlotDirectory(FileHandle &fileHandle) {
        std::vector<Slot> slotDirectory;
        unsigned short numberOfSlot = getNumberOfSlot(fileHandle.pageBuffer);
        if (numberOfSlot == 0) return slotDirectory;
        Slot slot;
        char * pSlot = (char *) fileHandle.pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short) - sizeof(Slot);
        for (unsigned short i = 0; i < numberOfSlot; i++) {
            std::memcpy(&slot, pSlot, sizeof(Slot));
            slotDirectory.push_back(slot);
            pSlot = pSlot - sizeof(Slot);
        }
        return slotDirectory;
    }

    unsigned short RecordBasedFileManager::getFreeSlotNum(const std::vector<Slot> &slotDirectory) {
        unsigned short numberOfSlot = slotDirectory.size();
        for (unsigned short i = 0; i < numberOfSlot; i++) {
            if (slotDirectory[i].offset == -1) return i;
        }
        return numberOfSlot;
    }

    unsigned short RecordBasedFileManager::getStartOfFreeSpace(FileHandle &fileHandle) {
        unsigned short startOfFreeSpace;
        std::memcpy(&startOfFreeSpace, (char *) fileHandle.pageBuffer + PAGE_SIZE - sizeof(unsigned short), sizeof(unsigned short));
        return startOfFreeSpace;
    }

    unsigned short RecordBasedFileManager::getFreeSpace(FileHandle &fileHandle) {
        unsigned short startOfFreeSpace = getStartOfFreeSpace(fileHandle);
        unsigned short numberOfSlot = getNumberOfSlot(fileHandle.pageBuffer);
        unsigned short freeSpace = PAGE_SIZE - startOfFreeSpace - 2 * sizeof(unsigned short) - numberOfSlot * sizeof(Slot);
        return freeSpace;
    }

    bool RecordBasedFileManager::isLastPageFree(FileHandle &fileHandle) {
        unsigned short freeSpace = getFreeSpace(fileHandle);
        if (fileHandle.recordLength > freeSpace) return false;
        std::vector<Slot> slotDirectory = getSlotDirectory(fileHandle);
        bool hasFreeSlot = getFreeSlotNum(slotDirectory) != slotDirectory.size();
        if (hasFreeSlot || fileHandle.recordLength + sizeof(Slot) <= freeSpace) return true;
        return false;
    }

    unsigned RecordBasedFileManager::findFreePage(FileHandle &fileHandle) {
        unsigned numberOfPages = fileHandle.getNumberOfPages();
        if (numberOfPages == 0 || !isLastPageFree(fileHandle)) {
            appendEmptyPage(fileHandle);
            return numberOfPages;
        }
        return numberOfPages - 1;
    }

    RC RecordBasedFileManager::toRecordBuffer(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data) {
        if (recordDescriptor.empty()) return -1;

        unsigned short numberOfAttributes = recordDescriptor.size();
        unsigned numberOfNullBytes = numberOfAttributes % 8 == 0 ? numberOfAttributes / 8 : numberOfAttributes / 8 + 1;
        char bitmaps[numberOfNullBytes];
        std::memcpy(bitmaps, (char *) data, numberOfNullBytes);

        fileHandle.recordLength = (numberOfAttributes + 1) * sizeof(unsigned short);

        std::memcpy(fileHandle.recordBuffer, &numberOfAttributes, sizeof(unsigned short));

        int length;
        unsigned pData = numberOfNullBytes;
        unsigned short pRecord = (numberOfAttributes + 1) * sizeof(unsigned short);
        for (unsigned indexOfAttribute = 0; indexOfAttribute < numberOfAttributes; indexOfAttribute++) {
            const Attribute &attr = recordDescriptor[indexOfAttribute];
            unsigned indexOfBitmap = indexOfAttribute / 8;
            unsigned offsetOfBitmap = indexOfAttribute % 8;
            if (bitmaps[indexOfBitmap] >> (7 - offsetOfBitmap) & (unsigned) 1) {
                std::memset((char *) fileHandle.recordBuffer + (indexOfAttribute + 1) * sizeof(unsigned short), 0, sizeof(unsigned short));
                continue;
            }
            if (attr.type == 0) {
                std::memcpy((char *) fileHandle.recordBuffer + pRecord, (char *) data + pData, sizeof(int));
                fileHandle.recordLength = fileHandle.recordLength + sizeof(int);
                pRecord = pRecord + sizeof(int);
                pData = pData + sizeof(int);
            } else if (attr.type == 1) {
                std::memcpy((char *) fileHandle.recordBuffer + pRecord, (char *) data + pData, sizeof(float));
                fileHandle.recordLength = fileHandle.recordLength + sizeof(float);
                pRecord = pRecord + sizeof(float);
                pData = pData + sizeof(float);
            } else {
                std::memcpy(&length, (char *) data + pData, sizeof(int));
                pData = pData + sizeof(int);
                std::memcpy((char *) fileHandle.recordBuffer + pRecord, (char *) data + pData, length);
                fileHandle.recordLength = fileHandle.recordLength + length;
                pRecord = pRecord + length;
                pData = pData + length;
            }
            std::memcpy((char *) fileHandle.recordBuffer + (indexOfAttribute + 1) * sizeof(unsigned short), &pRecord, sizeof(unsigned short));
        }

        return 0;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        if (toRecordBuffer(fileHandle, recordDescriptor, data) != 0) return -1;

        rid.pageNum = findFreePage(fileHandle);
        unsigned short startOfFreeSpace = getStartOfFreeSpace(fileHandle);
        std::vector<Slot> slotDirectory = getSlotDirectory(fileHandle);
        rid.slotNum = getFreeSlotNum(slotDirectory);

        Slot slot;
        slot.offset = (short) startOfFreeSpace;
        slot.length = fileHandle.recordLength;

        std::memcpy((char *) fileHandle.pageBuffer + startOfFreeSpace, fileHandle.recordBuffer, fileHandle.recordLength);
        std::memcpy((char *) fileHandle.pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short) - (rid.slotNum + 1) * sizeof(Slot), &slot, sizeof(Slot));

        if (rid.slotNum == slotDirectory.size()) {
            unsigned short newSlotNum = rid.slotNum + 1;
            std::memcpy((char *) fileHandle.pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short), &newSlotNum, sizeof(unsigned short));
        }
        startOfFreeSpace = startOfFreeSpace + fileHandle.recordLength;
        std::memcpy((char *) fileHandle.pageBuffer + PAGE_SIZE - sizeof(unsigned short), &startOfFreeSpace, sizeof(unsigned short));

        RC rc = fileHandle.writePage(rid.pageNum, fileHandle.pageBuffer);

        return rc;
    }

    RC RecordBasedFileManager::getRecordBuffer(FileHandle &fileHandle, const RID &rid) {
        toPageBuffer(fileHandle, rid.pageNum);
        std::vector<Slot> slotDirectory = getSlotDirectory(fileHandle);
        const Slot &slot = slotDirectory[rid.slotNum];
        if (slot.offset == -1) return -1;
        fileHandle.recordLength = slot.length;
        std::memcpy(fileHandle.recordBuffer, (char *) fileHandle.pageBuffer + slot.offset, fileHandle.recordLength);
        return 0;
    }

    RC RecordBasedFileManager::toData(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, void *data) {
        unsigned numberOfAttributes = recordDescriptor.size();
        unsigned numberOfNullBytes = numberOfAttributes % 8 == 0 ? numberOfAttributes / 8 : numberOfAttributes / 8 + 1;
        char bitmaps[numberOfNullBytes];
        for (unsigned i = 0; i < numberOfNullBytes; i++) bitmaps[i] = '\0';

        unsigned short offset;
        unsigned short pData = numberOfNullBytes;
        unsigned short pRecord = (numberOfAttributes + 1) * sizeof(unsigned short);
        unsigned short prev = pRecord;
        int length;
        for (unsigned indexOfAttribute = 0; indexOfAttribute < numberOfAttributes; indexOfAttribute++) {
            const Attribute &attr = recordDescriptor[indexOfAttribute];
            unsigned indexOfBitmap = indexOfAttribute / 8;
            unsigned offsetOfBitmap = indexOfAttribute % 8;
            std::memcpy(&offset, (unsigned short *) fileHandle.recordBuffer + 1 + indexOfAttribute, sizeof(unsigned short));
            if (offset == 0) {
                bitmaps[indexOfBitmap] |= (unsigned) 1 << (7 - offsetOfBitmap);
                continue;
            }
            if (attr.type == 0) {
                std::memcpy((char *) data + pData, (char *) fileHandle.recordBuffer + pRecord, sizeof(int));
                pRecord = pRecord + sizeof(int);
                pData = pData + sizeof(int);
            } else if (attr.type == 1) {
                std::memcpy((char *) data + pData, (char *) fileHandle.recordBuffer + pRecord, sizeof(float));
                pRecord = pRecord + sizeof(float);
                pData = pData + sizeof(float);
            } else {
                length = offset - prev;
                std::memcpy((char *) data + pData, &length, sizeof(int));
                pData = pData + sizeof(int);
                std::memcpy((char *) data + pData, (char *) fileHandle.recordBuffer + pRecord, length);
                pRecord = pRecord + length;
                pData = pData + length;
            }
            prev = offset;
        }

        std::memcpy((char *) data, bitmaps, numberOfNullBytes);

        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        getRecordBuffer(fileHandle, rid);
        toData(fileHandle, recordDescriptor, data);

        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        if (recordDescriptor.empty()) return 0;
        unsigned numberOfAttributes = recordDescriptor.size();
        unsigned numberOfNullBytes = numberOfAttributes % 8 == 0 ? numberOfAttributes / 8 : numberOfAttributes / 8 + 1;
        char bitmaps[numberOfNullBytes];
        std::memcpy(bitmaps, (char *) data, numberOfNullBytes);
        unsigned pData = numberOfNullBytes;
        int length;
        for (unsigned indexOfAttribute = 0; indexOfAttribute < numberOfAttributes; indexOfAttribute++) {
            if (indexOfAttribute != 0) out << ", ";
            const Attribute &attr = recordDescriptor[indexOfAttribute];
            out << attr.name << ": ";
            unsigned indexOfBitmap = indexOfAttribute / 8;
            unsigned offsetOfBitmap = indexOfAttribute % 8;
            if (bitmaps[indexOfBitmap] >> (7 - offsetOfBitmap) & (unsigned) 1) out << "NULL";
            else if (attr.type == 0) {
                int intVal;
                std::memcpy(&intVal, (char *) data + pData, sizeof(int));
                out << intVal;
                pData = pData + sizeof(int);
            } else if (attr.type == 1) {
                float floatVal;
                std::memcpy(&floatVal, (char *) data + pData, sizeof(float));
                out << floatVal;
                pData = pData + sizeof(float);
            } else if (attr.type == 2) {
                std::memcpy(&length, (char *) data + pData, sizeof(int));
                pData = pData + sizeof(int);
                char stringVal[length];
                std::memcpy(stringVal, (char *) data + pData, length);
                for (int i = 0; i < length; i++) out << stringVal[i];
                pData = pData + length;
            } else return -1;
        }
        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        return -1;
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        return -1;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        return -1;
    }

} // namespace PeterDB

