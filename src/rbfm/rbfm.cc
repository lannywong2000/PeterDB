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

    RC RecordBasedFileManager::createFile(const std::string &fileName) {
        return PagedFileManager::instance().createFile(fileName);
    }

    RC RecordBasedFileManager::destroyFile(const std::string &fileName) {
        return PagedFileManager::instance().destroyFile(fileName);
    }

    RC RecordBasedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        return PagedFileManager::instance().openFile(fileName, fileHandle);
    }

    RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
        return PagedFileManager::instance().closeFile(fileHandle);
    }

    RC RecordBasedFileManager::getPageBuffer(FileHandle &fileHandle, unsigned pageNum) {
        if (fileHandle.curPageNum == pageNum) return 0;
        fileHandle.curPageNum = pageNum;
        return fileHandle.readPage(pageNum, fileHandle.pageBuffer);
    }

    RC RecordBasedFileManager::appendEmptyPage(FileHandle &fileHandle) {
        fileHandle.curPageNum = fileHandle.getNumberOfPages();
        std::memset(fileHandle.pageBuffer, 0, PAGE_SIZE);
        return fileHandle.appendPage(fileHandle.pageBuffer);
    }

    unsigned short RecordBasedFileManager::getNumberOfSlot(FileHandle &fileHandle) {
        unsigned short numberOfSlot;
        std::memcpy(&numberOfSlot, (char *) fileHandle.pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short), sizeof(unsigned short));
        return numberOfSlot;
    }

    RC RecordBasedFileManager::getSlot(FileHandle &fileHandle, Slot &slot, unsigned short slotNum) {
        if (fileHandle.curPageNum == -1) return ERR_PAGE_BUFFER_INVALID;
        if (slotNum >= getNumberOfSlot(fileHandle)) return ERR_SLOT_READ_EXCEED;
        std::memcpy(&slot, (char *) fileHandle.pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short) - (slotNum + 1) * sizeof(Slot), sizeof(Slot));
        return 0;
    }

    RC RecordBasedFileManager::writeSlot(FileHandle &fileHandle, Slot &slotBuffer, unsigned short slotNum) {
        if (fileHandle.curPageNum == -1) return ERR_PAGE_BUFFER_INVALID;
        if (slotNum >= getNumberOfSlot(fileHandle)) return ERR_SLOT_READ_EXCEED;
        std::memcpy((char *) fileHandle.pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short) - (slotNum + 1) * sizeof(Slot), &slotBuffer, sizeof(Slot));
        return 0;
    }

    unsigned short RecordBasedFileManager::getFreeSlotNum(FileHandle &fileHandle) {
        unsigned short numberOfSlot = getNumberOfSlot(fileHandle);
        Slot slot;
        for (unsigned short i = 0; i < numberOfSlot; i++) {
            getSlot(fileHandle, slot, i);
            if (slot.offset == -1) return i;
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
        unsigned short numberOfSlot = getNumberOfSlot(fileHandle);
        unsigned short freeSpace = PAGE_SIZE - startOfFreeSpace - 2 * sizeof(unsigned short) - numberOfSlot * sizeof(Slot);
        return freeSpace;
    }

    bool RecordBasedFileManager::isCurrentPageFree(FileHandle &fileHandle) {
        if (fileHandle.curPageNum == -1) return false;
        unsigned short freeSpace = getFreeSpace(fileHandle);
        if (fileHandle.recordLength > freeSpace) return false;
        bool hasFreeSlot = getFreeSlotNum(fileHandle) != getNumberOfSlot(fileHandle);
        if (hasFreeSlot || fileHandle.recordLength + sizeof(Slot) <= freeSpace) return true;
        return false;
    }

    unsigned RecordBasedFileManager::findFreePage(FileHandle &fileHandle) {
        if (isCurrentPageFree(fileHandle)) return fileHandle.curPageNum;
        unsigned numberOfPages = fileHandle.getNumberOfPages();
        /*
        for (unsigned pageNum = 0; pageNum < numberOfPages; pageNum++) {
            getPageBuffer(fileHandle, pageNum);
            unsigned short freeSpace = getFreeSpace(fileHandle);
            if (fileHandle.recordLength > freeSpace) continue;
            bool hasFreeSlot = getFreeSlotNum(fileHandle) != getNumberOfSlot(fileHandle);
            if (hasFreeSlot || fileHandle.recordLength + sizeof(Slot) <= freeSpace) return pageNum;
        }
         */
        appendEmptyPage(fileHandle);
        return numberOfPages;
    }

    RC RecordBasedFileManager::toRecordBuffer(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, const void *data) {
        if (recordDescriptor.empty()) return ERR_RECORD_DESCRIPTOR_EMPTY;

        std::memset(fileHandle.recordBuffer, 0, 1);

        unsigned short numberOfAttributes = recordDescriptor.size();
        unsigned numberOfNullBytes = numberOfAttributes % 8 == 0 ? numberOfAttributes / 8 : numberOfAttributes / 8 + 1;
        char bitmaps[numberOfNullBytes];
        std::memcpy(bitmaps, (char *) data, numberOfNullBytes);

        fileHandle.recordLength = 1 + sizeof(unsigned) + (numberOfAttributes + 1) * sizeof(unsigned short);

        std::memcpy((char *) fileHandle.recordBuffer + 1, &fileHandle.version, sizeof(unsigned));
        std::memcpy((char *) fileHandle.recordBuffer + 1 + sizeof(unsigned), &numberOfAttributes, sizeof(unsigned short));

        int length;
        unsigned pData = numberOfNullBytes;
        unsigned short pRecord = 1 + sizeof(unsigned) + (numberOfAttributes + 1) * sizeof(unsigned short);
        for (unsigned indexOfAttribute = 0; indexOfAttribute < numberOfAttributes; indexOfAttribute++) {
            const Attribute &attr = recordDescriptor[indexOfAttribute];
            unsigned indexOfBitmap = indexOfAttribute / 8;
            unsigned offsetOfBitmap = indexOfAttribute % 8;
            if (bitmaps[indexOfBitmap] >> (7 - offsetOfBitmap) & (unsigned) 1) {
                std::memset((char *) fileHandle.recordBuffer + 1 + sizeof(unsigned) + (indexOfAttribute + 1) * sizeof(unsigned short), 0, sizeof(unsigned short));
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
            std::memcpy((char *) fileHandle.recordBuffer + 1 + sizeof(unsigned) + (indexOfAttribute + 1) * sizeof(unsigned short), &pRecord, sizeof(unsigned short));
        }

        fileHandle.recordLength = fileHandle.recordLength < 7 ? 7 : fileHandle.recordLength;

        return 0;
    }

    RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, RID &rid) {
        if (toRecordBuffer(fileHandle, recordDescriptor, data) != 0) return ERR_RECORD_FROM_DATA_FAILED;

        rid.pageNum = findFreePage(fileHandle);
        unsigned short startOfFreeSpace = getStartOfFreeSpace(fileHandle);
        rid.slotNum = getFreeSlotNum(fileHandle);

        Slot slot;
        slot.offset = (short) startOfFreeSpace;
        slot.length = fileHandle.recordLength;

        std::memcpy((char *) fileHandle.pageBuffer + startOfFreeSpace, fileHandle.recordBuffer, fileHandle.recordLength);
        std::memcpy((char *) fileHandle.pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short) - (rid.slotNum + 1) * sizeof(Slot), &slot, sizeof(Slot));

        if (rid.slotNum == getNumberOfSlot(fileHandle)) {
            unsigned short newSlotNum = rid.slotNum + 1;
            std::memcpy((char *) fileHandle.pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short), &newSlotNum, sizeof(unsigned short));
        }
        startOfFreeSpace = startOfFreeSpace + fileHandle.recordLength;
        std::memcpy((char *) fileHandle.pageBuffer + PAGE_SIZE - sizeof(unsigned short), &startOfFreeSpace, sizeof(unsigned short));

        return fileHandle.writePage(rid.pageNum, fileHandle.pageBuffer);
    }

    RC RecordBasedFileManager::getRecordBuffer(FileHandle &fileHandle, const RID &rid, bool recursive) {
        getPageBuffer(fileHandle, rid.pageNum);
        Slot slot;
        RC rc = getSlot(fileHandle, slot, rid.slotNum);
        if (rc != 0) return rc;
        if (slot.offset == -1) return ERR_RECORD_READ_DELETED;
        fileHandle.recordLength = slot.length;
        std::memcpy(fileHandle.recordBuffer, (char *) fileHandle.pageBuffer + slot.offset, fileHandle.recordLength);
        if (!recursive) return 0;
        unsigned char c = 0;
        if (std::memcmp(fileHandle.recordBuffer, &c, 1) == 0) return 0;
        RID recordID;
        std::memcpy(&recordID.pageNum, (char *) fileHandle.recordBuffer + 1, sizeof(unsigned));
        std::memcpy(&recordID.slotNum, (char *) fileHandle.recordBuffer + 1 + sizeof(unsigned), sizeof(unsigned short));
        return getRecordBuffer(fileHandle, recordID, true);
    }

    RC RecordBasedFileManager::toData(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor, void *data) {
        unsigned numberOfAttributes = recordDescriptor.size();
        unsigned numberOfNullBytes = numberOfAttributes % 8 == 0 ? numberOfAttributes / 8 : numberOfAttributes / 8 + 1;
        char bitmaps[numberOfNullBytes];
        std::memset(bitmaps, 0, numberOfNullBytes);

        unsigned short offset;
        unsigned short pData = numberOfNullBytes;
        unsigned short pRecord = 1 + sizeof(unsigned) + (numberOfAttributes + 1) * sizeof(unsigned short);
        unsigned short prev = pRecord;
        int length;
        for (unsigned indexOfAttribute = 0; indexOfAttribute < numberOfAttributes; indexOfAttribute++) {
            const Attribute &attr = recordDescriptor[indexOfAttribute];
            unsigned indexOfBitmap = indexOfAttribute / 8;
            unsigned offsetOfBitmap = indexOfAttribute % 8;
            std::memcpy(&offset, (char *) fileHandle.recordBuffer + 1 + sizeof(unsigned) + (1 + indexOfAttribute) * sizeof(unsigned short), sizeof(unsigned short));
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

    void RecordBasedFileManager::increaseVersion(FileHandle &fileHandle) {
        fileHandle.version = fileHandle.version + 1;
    }

    int RecordBasedFileManager::getRecordVersion(FileHandle &fileHandle, const RID &rid) {
        getRecordBuffer(fileHandle, rid, true);
        unsigned version = 0;
        std::memcpy(&version, (char *) fileHandle.recordBuffer + 1, sizeof(unsigned));
        return version;
    }

    RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                          const RID &rid, void *data) {
        RC rc = getRecordBuffer(fileHandle, rid, true);
        if (rc == 0) toData(fileHandle, recordDescriptor, data);

        return rc;
    }

    RC RecordBasedFileManager::printRecord(const std::vector<Attribute> &recordDescriptor, const void *data,
                                           std::ostream &out) {
        if (recordDescriptor.empty()) return ERR_RECORD_DESCRIPTOR_EMPTY;
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
            } else return ERR_ATTRIBUTE_TYPE_UNDEFINED;
        }
        return 0;
    }

    RC RecordBasedFileManager::shiftLeft(FileHandle &fileHandle, unsigned short offset, unsigned short length) {

        if (length == 0) return 0;

        unsigned short startOfFreeSpace = getStartOfFreeSpace(fileHandle);
        std::memmove((char *) fileHandle.pageBuffer + offset, (char *) fileHandle.pageBuffer + offset + length, startOfFreeSpace - offset - length);
        startOfFreeSpace = startOfFreeSpace - length;
        std::memcpy((char *) fileHandle.pageBuffer + PAGE_SIZE - sizeof(unsigned short), &startOfFreeSpace, sizeof(unsigned short));

        unsigned short numberOfSlot = getNumberOfSlot(fileHandle);
        Slot slotBuffer;
        for (unsigned short i = 0; i < numberOfSlot; i++) {
            getSlot(fileHandle, slotBuffer, i);
            if (slotBuffer.offset > offset) {
                slotBuffer.offset = slotBuffer.offset - length;
                writeSlot(fileHandle, slotBuffer, i);
            }
        }

        return 0;
    }

    RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const RID &rid) {
        RC rc = getPageBuffer(fileHandle, rid.pageNum);
        if (rc != 0) return rc;
        Slot slot;
        rc = getSlot(fileHandle, slot, rid.slotNum);
        if (rc != 0) return rc;
        if (slot.offset == -1) return ERR_RECORD_DUPLICATE_DELETE;
        fileHandle.recordLength = slot.length;
        std::memcpy(fileHandle.recordBuffer, (char *) fileHandle.pageBuffer + slot.offset, fileHandle.recordLength);
        shiftLeft(fileHandle, slot.offset, slot.length);
        slot.offset = -1;
        std::memcpy((char *) fileHandle.pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short) - (rid.slotNum + 1) * sizeof(Slot), &slot, sizeof(Slot));
        if (rid.slotNum == getNumberOfSlot(fileHandle) - 1) std::memcpy((char *) fileHandle.pageBuffer + PAGE_SIZE - 2 * sizeof(unsigned short), &rid.slotNum, sizeof(unsigned short));
        rc = fileHandle.writePage(rid.pageNum, fileHandle.pageBuffer);
        if (rc != 0) return rc;
        unsigned char c = 0;
        if (std::memcmp(fileHandle.recordBuffer, &c, 1) == 0) return 0;
        RID recordID;
        std::memcpy(&recordID.pageNum, (char *) fileHandle.recordBuffer + 1, sizeof(unsigned));
        std::memcpy(&recordID.slotNum, (char *) fileHandle.recordBuffer + 1 + sizeof(unsigned), sizeof(unsigned short));
        return deleteRecord(fileHandle, recordDescriptor, recordID);
    }

    RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                            const void *data, const RID &rid) {
        RC rc = toRecordBuffer(fileHandle, recordDescriptor, data);
        if (rc != 0) return rc;
        rc = getPageBuffer(fileHandle, rid.pageNum);
        if (rc != 0) return rc;
        Slot slot;
        rc = getSlot(fileHandle, slot, rid.slotNum);
        if (rc != 0) return rc;
        unsigned freeSpace = getFreeSpace(fileHandle);

        if (fileHandle.recordLength <= slot.length) {
            std::memcpy((char *) fileHandle.pageBuffer + slot.offset, fileHandle.recordBuffer, fileHandle.recordLength);
            shiftLeft(fileHandle, slot.offset + fileHandle.recordLength, slot.length - fileHandle.recordLength);
            slot.length = fileHandle.recordLength;
            writeSlot(fileHandle, slot, rid.slotNum);
            return fileHandle.writePage(rid.pageNum, fileHandle.pageBuffer);
        } else if (fileHandle.recordLength - slot.length <= freeSpace) {
            shiftLeft(fileHandle, slot.offset, slot.length);
            unsigned short startOfFreeSpace = getStartOfFreeSpace(fileHandle);
            std::memcpy((char *) fileHandle.pageBuffer + startOfFreeSpace, fileHandle.recordBuffer, fileHandle.recordLength);
            slot.offset = startOfFreeSpace;
            slot.length = fileHandle.recordLength;
            writeSlot(fileHandle, slot, rid.slotNum);
            startOfFreeSpace = startOfFreeSpace + fileHandle.recordLength;
            std::memcpy((char *) fileHandle.pageBuffer + PAGE_SIZE - sizeof(unsigned short), &startOfFreeSpace, sizeof(unsigned short));
            return fileHandle.writePage(rid.pageNum, fileHandle.pageBuffer);
        } else {
            RID recordID;
            rc = insertRecord(fileHandle, recordDescriptor, data, recordID);
            if (rc != 0) return rc;
            rc = getPageBuffer(fileHandle, rid.pageNum);
            if (rc != 0) return rc;
            unsigned char c = 1;
            std::memcpy((char *) fileHandle.pageBuffer + slot.offset, &c, sizeof(unsigned char));
            std::memcpy((char *) fileHandle.pageBuffer + slot.offset + sizeof(unsigned char), &recordID.pageNum, sizeof(unsigned));
            std::memcpy((char *) fileHandle.pageBuffer + slot.offset + sizeof(unsigned char) + sizeof(unsigned), &recordID.slotNum, sizeof(unsigned short));
            shiftLeft(fileHandle, slot.offset + 7, slot.length - 7);
            slot.length = 7;
            writeSlot(fileHandle, slot, rid.slotNum);
            return fileHandle.writePage(rid.pageNum, fileHandle.pageBuffer);
        }
    }

    RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                             const RID &rid, const std::string &attributeName, void *data) {
        RC rc = getRecordBuffer(fileHandle, rid, true);
        if (rc != 0) return rc;

        int index = 0;
        while (index < recordDescriptor.size()) {
            if (recordDescriptor[index].name == attributeName) break;
            index = index + 1;
        }
        if (index >= recordDescriptor.size()) return ERR_ATTRIBUTE_NOT_EXISTS;
        const Attribute &attr = recordDescriptor[index];

        unsigned short endPtr = 0;
        std::memcpy(&endPtr, (char *) fileHandle.recordBuffer + 1 + sizeof(unsigned) + (1 + index) * sizeof(unsigned short), sizeof(unsigned short));
        if (endPtr == 0) {
            unsigned char c = (unsigned char) 1 << 7;
            std::memcpy(data, &c, 1);
        } else {
            std::memset(data, 0 , 1);
            unsigned short startPtr = 0;
            while (--index >= 0) {
                std::memcpy(&startPtr, (char *) fileHandle.recordBuffer + 1 + sizeof(unsigned) + (1 + index) * sizeof(unsigned short), sizeof(unsigned short));
                if (startPtr != 0) break;
            }
            if (startPtr == 0) startPtr = 1 + sizeof(unsigned) + (1 + recordDescriptor.size()) * sizeof(unsigned short);
            if (attr.type == 2) {
                int length = endPtr - startPtr;
                std::memcpy((char *) data + 1, &length, sizeof(int));
                std::memcpy((char *) data + 1 + sizeof(int), (char *) fileHandle.recordBuffer + startPtr, endPtr - startPtr);
            } else std::memcpy((char *) data + 1, (char *) fileHandle.recordBuffer + startPtr, endPtr - startPtr);
        }

        return 0;
    }

    RC RecordBasedFileManager::scan(FileHandle &fileHandle, const std::vector<Attribute> &recordDescriptor,
                                    const std::string &conditionAttribute, const CompOp compOp, const void *value,
                                    const std::vector<std::string> &attributeNames,
                                    RBFM_ScanIterator &rbfm_ScanIterator) {
        rbfm_ScanIterator.fileHandle = fileHandle;
        rbfm_ScanIterator.rids = std::vector<RID>();
        rbfm_ScanIterator.pos = 0;
        rbfm_ScanIterator.recordDescriptor = recordDescriptor;
        rbfm_ScanIterator.attributeNames = attributeNames;

        Attribute attr;
        for (const auto &attrDescriptor: recordDescriptor) {
            if (attrDescriptor.name == conditionAttribute) {
                attr = attrDescriptor;
                break;
            }
        }

        int intBuffer, intValue, lengthBuffer, length;
        float floatBuffer, floatValue;
        char *varCharBuffer = nullptr, *varChar = nullptr;
        unsigned char c = 0;
        bool mark;
        unsigned numberOfPages = fileHandle.getNumberOfPages();
        for (unsigned pageNum = 0; pageNum < numberOfPages; pageNum = pageNum + 1) {
            getPageBuffer(fileHandle, pageNum);
            unsigned short numberOfSlots = getNumberOfSlot(fileHandle);
            for (unsigned short slotNum = 0; slotNum < numberOfSlots; slotNum = slotNum + 1) {
                Slot slot;
                getSlot(fileHandle, slot, slotNum);
                if (slot.offset == -1) continue;
                RID rid(pageNum, slotNum);
                if (conditionAttribute.empty()) {
                    rbfm_ScanIterator.rids.push_back(rid);
                    continue;
                }
                getRecordBuffer(fileHandle, rid, false);
                if (std::memcmp(fileHandle.recordBuffer, &c, 1) != 0) continue;
                char data[1 + sizeof(int) + attr.length];
                std::memset(data, 0, 1 + sizeof(int) + attr.length);
                readAttribute(fileHandle, recordDescriptor, rid, conditionAttribute, data);
                if (std::memcmp(data, &c, 1) != 0) continue;
                if (compOp == NO_OP) {
                    rbfm_ScanIterator.rids.push_back(rid);
                    continue;
                }
                switch (attr.type) {
                    case 0:
                        std::memcpy(&intBuffer, data + 1, sizeof(int));
                        std::memcpy(&intValue, value, sizeof(int));
                        break;
                    case 1:
                        std::memcpy(&floatBuffer, data + 1, sizeof(float));
                        std::memcpy(&floatValue, value, sizeof(float));
                        break;
                    default:
                        std::memcpy(&lengthBuffer, data + 1, sizeof(int));
                        std::memcpy(&length, value, sizeof(int));
                        delete[] varCharBuffer;
                        varCharBuffer = nullptr;
                        varCharBuffer = new char[lengthBuffer + 1];
                        std::memset(varCharBuffer, 0, lengthBuffer + 1);
                        std::memcpy(varCharBuffer, data + 1 + sizeof(int), lengthBuffer);
                        delete[] varChar;
                        varChar = nullptr;
                        varChar = new char[length + 1];
                        std::memset(varChar, 0, length + 1);
                        std::memcpy(varChar, (char *) value + sizeof(int), length);
                }
                switch (compOp) {
                    case 0:
                        if (attr.type == 0) mark = intBuffer == intValue;
                        else if (attr.type == 1) mark = floatBuffer == floatValue;
                        else mark = std::strcmp(varCharBuffer, varChar) == 0;
                        break;
                    case 1:
                        if (attr.type == 0) mark = intBuffer < intValue;
                        else if (attr.type == 1) mark = floatBuffer < floatValue;
                        else mark = std::strcmp(varCharBuffer, varChar) < 0;
                        break;
                    case 2:
                        if (attr.type == 0) mark = intBuffer <= intValue;
                        else if (attr.type == 1) mark = floatBuffer <= floatValue;
                        else mark = std::strcmp(varCharBuffer, varChar) <= 0;
                        break;
                    case 3:
                        if (attr.type == 0) mark = intBuffer > intValue;
                        else if (attr.type == 1) mark = floatBuffer > floatValue;
                        else mark = std::strcmp(varCharBuffer, varChar) > 0;
                        break;
                    case 4:
                        if (attr.type == 0) mark = intBuffer >= intValue;
                        else if (attr.type == 1) mark = floatBuffer >= floatValue;
                        else mark = std::strcmp(varCharBuffer, varChar) >= 0;
                        break;
                    case 5:
                        if (attr.type == 0) mark = intBuffer != intValue;
                        else if (attr.type == 1) mark = floatBuffer != floatValue;
                        else mark = std::strcmp(varCharBuffer, varChar) != 0;
                        break;
                    default:
                        mark = true;
                }
                if (mark) rbfm_ScanIterator.rids.push_back(rid);
            }
        }
        delete[] varCharBuffer;
        delete[] varChar;
        return 0;
    }

    RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data) {
        if (pos >= rids.size()) return RBFM_EOF;
        rid = rids[pos];
        int attrsLength = attributeNames.size();
        int bitmapBytes = attrsLength % 8 ? attrsLength / 8 + 1 : attrsLength / 8;
        char bitmaps[bitmapBytes];
        std::memset(bitmaps, 0, bitmapBytes);
        int offset = bitmapBytes;
        for (int index = 0; index < attrsLength; index = index + 1) {
            const std::string &attributeName = attributeNames[index];
            for (const Attribute &attr : recordDescriptor) {
                if (attr.name == attributeName) {
                    char dataBuffer[1 + sizeof(int) + attr.length];
                    RecordBasedFileManager::instance().readAttribute(fileHandle, recordDescriptor, rid, attr.name, dataBuffer);
                    if (dataBuffer[0] >> 7 & (unsigned) 1) bitmaps[index / 8] |= (unsigned) 1 << (7 - index % 8);
                    else if (attr.type == 2) {
                        int length;
                        std::memcpy(&length, dataBuffer + 1, sizeof(int));
                        std::memcpy((char *) data + offset, dataBuffer + 1, sizeof(int) + length);
                        offset = offset + sizeof(int) + length;
                    } else {
                        std::memcpy((char *) data + offset, dataBuffer + 1, sizeof(int));
                        offset = offset + sizeof(int);
                    }
                    break;
                }
            }
        }
        std::memcpy(data, bitmaps, bitmapBytes);
        pos = pos + 1;
        return 0;
    }

    RC RBFM_ScanIterator::close() {
        fileHandle.closeFile();
        rids.clear();
        return 0;
    }

} // namespace PeterDB
