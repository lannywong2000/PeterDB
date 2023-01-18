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
        free(pageBuffer);
        free(recordBuffer);
        return pfm.closeFile(fileHandle);
    }

    RC RecordBasedFileManager::toPageBuffer(FileHandle &fileHandle, unsigned int pageNum) {
        free(pageBuffer);
        pageBuffer = malloc(PAGE_SIZE);
        return fileHandle.readPage(pageNum, pageBuffer);
    }

    RC RecordBasedFileManager::appendEmptyPage(FileHandle &fileHandle) {
        free(pageBuffer);
        pageBuffer = malloc(PAGE_SIZE);
        size_t n = PAGE_SIZE / sizeof(unsigned short);
        *((unsigned short *) pageBuffer + n - 1) = 0;
        *((unsigned short *) pageBuffer + n - 2) = 0;
        return fileHandle.appendPage(pageBuffer);
    }

    unsigned short RecordBasedFileManager::getNumberOfSlot() {
        unsigned short numberOfSlot = *((unsigned short *) pageBuffer + PAGE_SIZE / sizeof(unsigned short) - 2);
        return numberOfSlot;
    }

    std::vector<Slot> RecordBasedFileManager::getSlotDirectory() {
        std::vector<Slot> slotDirectory;
        unsigned short numberOfSlot = getNumberOfSlot();
        if (numberOfSlot == 0) return slotDirectory;
        Slot slot;
        char * pSlot = (char *) pageBuffer + PAGE_SIZE - 1 - 2 * sizeof(unsigned short) - sizeof(Slot);
        for (unsigned short i = 0; i < numberOfSlot; i++) {
            memcpy(&slot, pSlot, sizeof(Slot));
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

    unsigned short RecordBasedFileManager::getStartOfFreeSpace() {
        unsigned short startOfFreeSpace = *((unsigned short *) pageBuffer + PAGE_SIZE / sizeof(unsigned short) - 1);
        return startOfFreeSpace;
    }

    unsigned short RecordBasedFileManager::getFreeSpace() {
        unsigned short startOfFreeSpace = getStartOfFreeSpace();
        unsigned short numberOfSlot = getNumberOfSlot();
        unsigned short freeSpace = PAGE_SIZE - startOfFreeSpace - 2 * sizeof(unsigned short) - numberOfSlot * sizeof(Slot);
        return freeSpace;
    }

    unsigned RecordBasedFileManager::findFreePage(FileHandle &fileHandle) {
        for (unsigned pageNum = 0; pageNum < fileHandle.getNumberOfPages(); pageNum++) {
            toPageBuffer(fileHandle, pageNum);
            unsigned short freeSpace = getFreeSpace();
            if (sizeof(recordBuffer) > freeSpace) continue;
            std::vector<Slot> slotDirectory = getSlotDirectory();
            bool hasFreeSlot = getFreeSlotNum(slotDirectory) != slotDirectory.size();
            if (hasFreeSlot || sizeof(recordBuffer) + sizeof(Slot) <= freeSpace) return pageNum;
        }
        appendEmptyPage(fileHandle);
        return fileHandle.getNumberOfPages() - 1;
    }

    RC RecordBasedFileManager::toRecordBuffer(const std::vector<Attribute> &recordDescriptor, const void *data) {
        if (recordDescriptor.empty()) return -1;

        unsigned numberOfAttributes = recordDescriptor.size();
        unsigned numberOfNullBytes = numberOfAttributes % 8 == 0 ? numberOfAttributes / 8 : numberOfAttributes % 8 + 1;
        const char *dataBytes = (char *)data;
        char bitmaps[numberOfNullBytes];
        memcpy(bitmaps, dataBytes, numberOfNullBytes);

        unsigned recordLength = (numberOfAttributes + 1) * sizeof(unsigned short);
        for (unsigned indexOfAttribute = 0; indexOfAttribute < numberOfAttributes; indexOfAttribute++) {
            const Attribute &attr = recordDescriptor[indexOfAttribute];
            unsigned indexOfBitmap = indexOfAttribute / 8;
            unsigned offsetOfBitmap = indexOfAttribute % 8;
            if (bitmaps[indexOfBitmap] >> (8 - offsetOfBitmap - 1) & 1) continue;
            else if (attr.type == 0) recordLength = recordLength + sizeof(int);
            else if (attr.type == 1) recordLength = recordLength + sizeof(float);
            else recordLength = recordLength + attr.length;
        }

        free(recordBuffer);
        recordBuffer = malloc(recordLength);
        *((unsigned short *) recordBuffer) = numberOfAttributes;

        unsigned short pData = numberOfNullBytes;
        unsigned short pRecord = (numberOfAttributes + 1) * sizeof(unsigned short);
        for (unsigned indexOfAttribute = 0; indexOfAttribute < numberOfAttributes; indexOfAttribute++) {
            const Attribute &attr = recordDescriptor[indexOfAttribute];
            unsigned indexOfBitmap = indexOfAttribute / 8;
            unsigned offsetOfBitmap = indexOfAttribute % 8;
            if (bitmaps[indexOfBitmap] >> (7 - offsetOfBitmap) & 1) *((unsigned short *) recordBuffer + indexOfAttribute + 1) = 0;
            else if (attr.type == 0) {
                memcpy((char *) recordBuffer + pRecord, dataBytes + pData, sizeof(int));
                pRecord = pRecord + sizeof(int);
                pData = pData + sizeof(int);
            } else if (attr.type == 1) {
                memcpy((char *) recordBuffer + pRecord, dataBytes + pData, sizeof(float));
                pRecord = pRecord + sizeof(float);
                pData = pData + sizeof(float);
            } else {
                memcpy((char *) recordBuffer + pRecord, dataBytes + pData, attr.length);
                pRecord = pRecord + attr.length;
                pData = pData + attr.length;
            }
            *((unsigned short *) recordBuffer + indexOfAttribute + 1) = pRecord;
        }

        return 0;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        if (toRecordBuffer(recordDescriptor, data) != 0) return -1;

        rid.pageNum = findFreePage(fileHandle);
        unsigned short startOfFreeSpace = getStartOfFreeSpace();
        std::vector<Slot> slotDirectory = getSlotDirectory();
        rid.slotNum = getFreeSlotNum(slotDirectory);

        Slot slot;
        slot.offset = (short) startOfFreeSpace;
        slot.length = sizeof(recordBuffer);

        memcpy((char *) pageBuffer + startOfFreeSpace, recordBuffer, sizeof(recordBuffer));
        memcpy((char *) pageBuffer + PAGE_SIZE - 1 - 2 * sizeof(unsigned short) - (rid.slotNum + 1) * sizeof(Slot), &slot, sizeof(Slot));

        unsigned short originalNumberOfSlot = slotDirectory.size();
        bool useNewSlot = rid.slotNum == originalNumberOfSlot;
        unsigned short numberOfSlot = useNewSlot ? originalNumberOfSlot + 1 : originalNumberOfSlot;
        startOfFreeSpace = startOfFreeSpace + sizeof(recordBuffer);
        *((unsigned short *) pageBuffer + PAGE_SIZE / sizeof(unsigned short) - 1) = startOfFreeSpace;
        *((unsigned short *) pageBuffer + PAGE_SIZE / sizeof(unsigned short) - 2) = numberOfSlot;

        return fileHandle.writePage(rid.pageNum, pageBuffer);
    }

    RC RecordBasedFileManager::getRecordBuffer(FileHandle &fileHandle, const RID &rid) {
        free(recordBuffer);
        toPageBuffer(fileHandle, rid.pageNum);
        std::vector<Slot> slotDirectory = getSlotDirectory();
        const Slot &slot = slotDirectory[rid.slotNum];
        recordBuffer = malloc(slot.length);
        memcpy(recordBuffer, (char *) pageBuffer + slot.offset, sizeof(recordBuffer));
        return 0;
    }

    RC RecordBasedFileManager::toData(const std::vector<Attribute> &recordDescriptor, void *data) {
        unsigned numberOfAttributes = recordDescriptor.size();
        unsigned numberOfNullBytes = numberOfAttributes % 8 == 0 ? numberOfAttributes / 8 : numberOfAttributes % 8 + 1;
        char bitmaps[numberOfNullBytes];

        unsigned dataLength = numberOfNullBytes;
        unsigned short offset;
        for (unsigned indexOfAttribute = 0; indexOfAttribute < numberOfAttributes; indexOfAttribute++) {
            const Attribute &attr = recordDescriptor[indexOfAttribute];
            unsigned indexOfBitmap = indexOfAttribute / 8;
            unsigned offsetOfBitmap = indexOfAttribute % 8;
            memcpy(&offset, (unsigned short *) recordBuffer + 1 + indexOfAttribute, sizeof(unsigned short));
            if (offset == 0) bitmaps[indexOfBitmap] |= 1 << (7 - offsetOfBitmap);
            else if (attr.type == 0) dataLength = dataLength + sizeof(int);
            else if (attr.type == 1) dataLength = dataLength + sizeof(float);
            else dataLength = dataLength + attr.length;
        }

        free(data);
        data = malloc(dataLength);
        memcpy((char *) data, bitmaps, numberOfNullBytes);

        unsigned short pData = numberOfNullBytes;
        unsigned short pRecord = (numberOfAttributes + 1) * sizeof(unsigned short);
        for (unsigned indexOfAttribute = 0; indexOfAttribute < numberOfAttributes; indexOfAttribute++) {
            const Attribute &attr = recordDescriptor[indexOfAttribute];
            unsigned indexOfBitmap = indexOfAttribute / 8;
            unsigned offsetOfBitmap = indexOfAttribute % 8;
            if (bitmaps[indexOfBitmap] >> (7 - offsetOfBitmap) & 1) continue;
            else if (attr.type == 0) {
                memcpy((char *) data + pData, (char *) recordBuffer + pRecord, sizeof(int));
                pRecord = pRecord + sizeof(int);
                pData = pData + sizeof(int);
            } else if (attr.type == 1) {
                memcpy((char *) data + pData, (char *) recordBuffer + pRecord, sizeof(float));
                pRecord = pRecord + sizeof(float);
                pData = pData + sizeof(float);
            } else {
                memcpy((char *) data + pData, (char *) recordBuffer + pRecord, attr.length);
                pRecord = pRecord + attr.length;
                pData = pData + attr.length;
            }
        }

        return 0;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        getRecordBuffer(fileHandle, rid);
        toData(recordDescriptor, data);
        return 0;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        if (recordDescriptor.empty()) return 0;
        unsigned numberOfAttributes = recordDescriptor.size();
        unsigned numberOfNullBytes = numberOfAttributes % 8 == 0 ? numberOfAttributes / 8 : numberOfAttributes % 8 + 1;
        const char *dataBytes = (char *)data;
        char bitmaps[numberOfNullBytes];
        memcpy(bitmaps, dataBytes, numberOfNullBytes);
        unsigned pData = numberOfNullBytes;
        for (unsigned indexOfAttribute = 0; indexOfAttribute < numberOfAttributes; indexOfAttribute++) {
            if (indexOfAttribute != 0) out << ", ";
            const Attribute &attr = recordDescriptor[indexOfAttribute];
            out << attr.name << ": ";
            unsigned indexOfBitmap = indexOfAttribute / 8;
            unsigned offsetOfBitmap = indexOfAttribute % 8;
            if (bitmaps[indexOfBitmap] >> (7 - offsetOfBitmap) & 1) out << "NULL";
            else if (attr.type == 0) {
                int intVal;
                memcpy(&intVal, dataBytes + pData, sizeof(int));
                out << intVal;
                pData = pData + sizeof(int);
            } else if (attr.type == 1) {
                float floatVal;
                memcpy(&floatVal, dataBytes + pData, sizeof(float));
                out << floatVal;
                pData = pData + sizeof(float);
            } else if (attr.type == 2) {
                char stringVal[attr.length];
                memcpy(stringVal, dataBytes + pData, attr.length);
                out << stringVal;
                pData = pData + attr.length;
            } else return -1;
        }
        out << '\n';
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

