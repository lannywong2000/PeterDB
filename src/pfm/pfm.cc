#include "src/include/pfm.h"

namespace PeterDB {
    PagedFileManager &PagedFileManager::instance() {
        static PagedFileManager _pf_manager = PagedFileManager();
        return _pf_manager;
    }

    PagedFileManager::PagedFileManager() = default;

    PagedFileManager::~PagedFileManager() = default;

    PagedFileManager::PagedFileManager(const PagedFileManager &) = default;

    PagedFileManager &PagedFileManager::operator=(const PagedFileManager &) = default;

    bool exists(const std::string &fileName) {
        FILE *pFile = fopen(fileName.c_str(), "rb");
        if (pFile == nullptr) return false;
        fclose(pFile);
        return true;
    }

    RC PagedFileManager::createFile(const std::string &fileName) {
        if (exists(fileName)) return ERR_FILE_NAME_EXISTS;
        FILE *pFile = fopen(fileName.c_str(), "w+b");
        if (pFile == nullptr) return ERR_FILE_CREATE_FAILED;
        void *pageBuffer = malloc(PAGE_SIZE);
        memset(pageBuffer, 0, PAGE_SIZE);
        unsigned buffer[5] = {0};
        std::memcpy(pageBuffer, buffer, sizeof(unsigned) * 5);
        fwrite(pageBuffer, sizeof(char), PAGE_SIZE, pFile);
        free(pageBuffer);
        fclose(pFile);
        return 0;
    }

    RC PagedFileManager::destroyFile(const std::string &fileName) {
        if (!exists(fileName)) return ERR_FILE_NOT_EXISTS;
        if (remove(fileName.c_str()) != 0) return ERR_FILE_DELETE_FAILED;
        else return 0;
    }

    RC PagedFileManager::openFile(const std::string &fileName, FileHandle &fileHandle) {
        if (fileHandle.pFile != nullptr) return ERR_FILE_HANDLE_REUSE;
        FILE *pFile = fopen(fileName.c_str(), "r+b");
        if (pFile == nullptr) return ERR_FILE_OPEN_FAILED;
        fseek(pFile, 0, SEEK_SET);
        unsigned buffer[5];
        if (fread(buffer, sizeof(unsigned), 5, pFile) == 5) {
            fileHandle.pFile = pFile;
            fileHandle.numberOfPages = buffer[0];
            fileHandle.readPageCounter = buffer[1];
            fileHandle.writePageCounter = buffer[2];
            fileHandle.appendPageCounter = buffer[3];
            fileHandle.version = buffer[4];
            fileHandle.pageBuffer = malloc(PAGE_SIZE);
            fileHandle.recordBuffer = malloc(RECORD_SIZE);
            memset(fileHandle.pageBuffer, 0, PAGE_SIZE);
            memset(fileHandle.recordBuffer, 0, RECORD_SIZE);
            fileHandle.curPageNum = -1;
            return 0;
        } else return ERR_FILE_WRONG_FORMAT;
    }

    RC PagedFileManager::closeFile(FileHandle &fileHandle) {
        return fileHandle.closeFile();
    }

    FileHandle::FileHandle() {
        numberOfPages = 0;
        readPageCounter = 0;
        writePageCounter = 0;
        appendPageCounter = 0;
    };

    FileHandle::~FileHandle() = default;

    RC FileHandle::closeFile() {
        if (pFile == nullptr) return 0;
        fseek(pFile, 0, SEEK_SET);
        unsigned buffer[5] = {numberOfPages, readPageCounter, writePageCounter, appendPageCounter, version};
        fwrite(buffer, sizeof(unsigned), 5, pFile);

        free(pageBuffer);
        free(recordBuffer);

        if (fclose(pFile) != 0) return ERR_FILE_CLOSE_FAILED;
        pFile = nullptr;
        return 0;
    }

    RC FileHandle::readPage(PageNum pageNum, void *data) {
        if (pageNum >= numberOfPages) return ERR_PAGE_READ_EXCEED;
        fseek(pFile, PAGE_SIZE * (pageNum + 1), SEEK_SET);
        fread(data, sizeof(char), PAGE_SIZE, pFile);
        readPageCounter = readPageCounter + 1;
        return 0;
    }

    RC FileHandle::writePage(PageNum pageNum, const void *data) {
        if (pageNum >= numberOfPages) return ERR_PAGE_WRITE_EXCEED;
        fseek(pFile, PAGE_SIZE * (pageNum + 1), SEEK_SET);
        fwrite(data, sizeof(char), PAGE_SIZE, pFile);
        writePageCounter = writePageCounter + 1;
        return 0;
    }

    RC FileHandle::appendPage(const void *data) {
        fseek(pFile, PAGE_SIZE * (numberOfPages + 1), SEEK_SET);
        fwrite(data, sizeof(char), PAGE_SIZE, pFile);
        numberOfPages = numberOfPages + 1;
        appendPageCounter = appendPageCounter + 1;
        return 0;
    }

    unsigned FileHandle::getNumberOfPages() {
        return numberOfPages;
    }

    RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount) {
        readPageCount = readPageCounter;
        writePageCount = writePageCounter;
        appendPageCount = appendPageCounter;
        return 0;
    }

} // namespace PeterDB