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

    RC RecordBasedFileManager::toPageBuffer(FileHandle &fileHandle, unsigned int pageNum, void *pageBuffer) {
        return fileHandle.readPage(pageNum, pageBuffer);
    }

    RC RecordBasedFileManager::appendEmptyPage(FileHandle &fileHandle) {
        void *pageBuffer = malloc(PAGE_SIZE);
        memset(pageBuffer, 0, PAGE_SIZE);
        RC rc = fileHandle.appendPage(pageBuffer);
        free(pageBuffer);
        return rc;
    }

    unsigned short RecordBasedFileManager::getNumberOfSlot(const void *pageBuffer) {
        unsigned short numberOfSlot = *((unsigned short *) pageBuffer + PAGE_SIZE / sizeof(unsigned short) - 2);
        return numberOfSlot;
    }

    std::vector<Slot> RecordBasedFileManager::getSlotDirectory(const void *pageBuffer) {
        std::vector<Slot> slotDirectory;
        unsigned short numberOfSlot = getNumberOfSlot(pageBuffer);
        if (numberOfSlot == 0) return slotDirectory;
        Slot slot;
        char * pSlot = (char *) pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short) - sizeof(Slot);
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

    unsigned short RecordBasedFileManager::getStartOfFreeSpace(const void *pageBuffer) {
        unsigned short startOfFreeSpace = *((unsigned short *) pageBuffer + PAGE_SIZE / sizeof(unsigned short) - 1);
        return startOfFreeSpace;
    }

    unsigned short RecordBasedFileManager::getFreeSpace(const void *pageBuffer) {
        unsigned short startOfFreeSpace = getStartOfFreeSpace(pageBuffer);
        unsigned short numberOfSlot = getNumberOfSlot(pageBuffer);
        unsigned short freeSpace = PAGE_SIZE - startOfFreeSpace - 2 * sizeof(unsigned short) - numberOfSlot * sizeof(Slot);
        return freeSpace;
    }

    unsigned RecordBasedFileManager::findFreePage(FileHandle &fileHandle, void *pageBuffer, unsigned short recordLength) {
        unsigned numberOfPages = fileHandle.getNumberOfPages();
        for (unsigned pageNum = 0; pageNum < numberOfPages; pageNum++) {
            toPageBuffer(fileHandle, pageNum, pageBuffer);
            unsigned short freeSpace = getFreeSpace(pageBuffer);
            if (recordLength > freeSpace) continue;
            std::vector<Slot> slotDirectory = getSlotDirectory(pageBuffer);
            bool hasFreeSlot = getFreeSlotNum(slotDirectory) != slotDirectory.size();
            if (hasFreeSlot || recordLength + sizeof(Slot) <= freeSpace) return pageNum;
        }
        appendEmptyPage(fileHandle);
        toPageBuffer(fileHandle, numberOfPages, pageBuffer);
        return numberOfPages;
    }

    RC RecordBasedFileManager::toRecordBuffer(const std::vector<Attribute> &recordDescriptor, const void *data, void *recordBuffer, unsigned short &recordLength) { // recordBuffer should be initialized at a fixed length with 0s
        if (recordDescriptor.empty()) return -1;

        unsigned short numberOfAttributes = recordDescriptor.size();
        unsigned numberOfNullBytes = numberOfAttributes % 8 == 0 ? numberOfAttributes / 8 : numberOfAttributes / 8 + 1;
        char bitmaps[numberOfNullBytes];
        std::memcpy(bitmaps, (char *) data, numberOfNullBytes);

        recordLength = (numberOfAttributes + 1) * sizeof(unsigned short);
        unsigned pData = numberOfNullBytes;
        int length;
        for (unsigned indexOfAttribute = 0; indexOfAttribute < numberOfAttributes; indexOfAttribute++) {
            const Attribute &attr = recordDescriptor[indexOfAttribute];
            unsigned indexOfBitmap = indexOfAttribute / 8;
            unsigned offsetOfBitmap = indexOfAttribute % 8;
            if (bitmaps[indexOfBitmap] >> (7 - offsetOfBitmap) & (unsigned) 1) continue;
            else if (attr.type == 0) recordLength = recordLength + sizeof(int);
            else if (attr.type == 1) recordLength = recordLength + sizeof(float);
            else {
                std::memcpy(&length, (char *) data + pData, sizeof(int));
                pData = pData + length;
                recordLength = recordLength + length;
            }
            pData = pData + sizeof(unsigned);
        }

        std::memcpy(recordBuffer, &numberOfAttributes, sizeof(unsigned short));

        pData = numberOfNullBytes;
        unsigned short pRecord = (numberOfAttributes + 1) * sizeof(unsigned short);
        for (unsigned indexOfAttribute = 0; indexOfAttribute < numberOfAttributes; indexOfAttribute++) {
            const Attribute &attr = recordDescriptor[indexOfAttribute];
            unsigned indexOfBitmap = indexOfAttribute / 8;
            unsigned offsetOfBitmap = indexOfAttribute % 8;
            if (bitmaps[indexOfBitmap] >> (7 - offsetOfBitmap) & (unsigned) 1) continue;
            if (attr.type == 0) {
                std::memcpy((char *) recordBuffer + pRecord, (char *) data + pData, sizeof(int));
                pRecord = pRecord + sizeof(int);
                pData = pData + sizeof(int);
            } else if (attr.type == 1) {
                std::memcpy((char *) recordBuffer + pRecord, (char *) data + pData, sizeof(float));
                pRecord = pRecord + sizeof(float);
                pData = pData + sizeof(float);
            } else {
                std::memcpy(&length, (char *) data + pData, sizeof(int));
                pData = pData + sizeof(int);
                std::memcpy((char *) recordBuffer + pRecord, (char *) data + pData, length);
                pRecord = pRecord + length;
                pData = pData + length;
            }
            std::memcpy((char *) recordBuffer + (indexOfAttribute + 1) * sizeof(unsigned short), &pRecord, sizeof(unsigned short));
        }

        return 0;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        void *pageBuffer = malloc(PAGE_SIZE);
        memset(pageBuffer, 0, PAGE_SIZE);
        void *recordBuffer = malloc(RECORD_SIZE);
        memset(recordBuffer, 0, RECORD_SIZE);
        unsigned short recordLength;

        if (toRecordBuffer(recordDescriptor, data, recordBuffer, recordLength) != 0) return -1;

        rid.pageNum = findFreePage(fileHandle, pageBuffer, recordLength);
        unsigned short startOfFreeSpace = getStartOfFreeSpace(pageBuffer);
        std::vector<Slot> slotDirectory = getSlotDirectory(pageBuffer);
        rid.slotNum = getFreeSlotNum(slotDirectory);

        Slot slot;
        slot.offset = (short) startOfFreeSpace;
        slot.length = recordLength;

        std::memcpy((char *) pageBuffer + startOfFreeSpace, recordBuffer, recordLength);
        std::memcpy((char *) pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short) - (rid.slotNum + 1) * sizeof(Slot), &slot, sizeof(Slot));

        if (rid.slotNum == slotDirectory.size()) *((unsigned short *) pageBuffer + PAGE_SIZE / sizeof(unsigned short) - 2) = rid.slotNum + 1;
        startOfFreeSpace = startOfFreeSpace + recordLength;
        std::memcpy((char *) pageBuffer + PAGE_SIZE - sizeof(unsigned short), &startOfFreeSpace, sizeof(unsigned short));

        RC rc = fileHandle.writePage(rid.pageNum, pageBuffer);

        free(pageBuffer);
        free(recordBuffer);

        return rc;
    }

    RC RecordBasedFileManager::getRecordBuffer(FileHandle &fileHandle, const RID &rid, void *pageBuffer, void *recordBuffer, unsigned short &recordLength) {
        toPageBuffer(fileHandle, rid.pageNum, pageBuffer);
        std::vector<Slot> slotDirectory = getSlotDirectory(pageBuffer);
        const Slot &slot = slotDirectory[rid.slotNum];
        if (slot.offset == -1) return -1;
        recordLength = slot.length;
        std::memcpy(recordBuffer, (char *) pageBuffer + slot.offset, recordLength);
        return 0;
    }

    RC RecordBasedFileManager::toData(const std::vector<Attribute> &recordDescriptor, const void *recordBuffer, void *data) {
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
            std::memcpy(&offset, (unsigned short *) recordBuffer + 1 + indexOfAttribute, sizeof(unsigned short));
            if (offset == 0) {
                bitmaps[indexOfBitmap] |= (unsigned) 1 << (7 - offsetOfBitmap);
                continue;
            }
            if (attr.type == 0) {
                std::memcpy((char *) data + pData, (char *) recordBuffer + pRecord, sizeof(int));
                pRecord = pRecord + sizeof(int);
                pData = pData + sizeof(int);
            } else if (attr.type == 1) {
                std::memcpy((char *) data + pData, (char *) recordBuffer + pRecord, sizeof(float));
                pRecord = pRecord + sizeof(float);
                pData = pData + sizeof(float);
            } else {
                length = offset - prev;
                std::memcpy((char *) data + pData, &length, sizeof(int));
                pData = pData + sizeof(int);
                std::memcpy((char *) data + pData, (char *) recordBuffer + pRecord, length);
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
        void *pageBuffer = malloc(PAGE_SIZE);
        memset(pageBuffer, 0, PAGE_SIZE);
        void *recordBuffer = malloc(RECORD_SIZE);
        memset(recordBuffer, 0, RECORD_SIZE);
        unsigned short recordLength;

        getRecordBuffer(fileHandle, rid, pageBuffer, recordBuffer, recordLength);
        toData(recordDescriptor, recordBuffer, data);

        free(pageBuffer);
        free(recordBuffer);

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

