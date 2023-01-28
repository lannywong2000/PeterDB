#ifndef _rc_h_
#define _rc_h_

namespace PeterDB {

    typedef enum {

        ERR_UNDEFINED = -1,

        OK = 0,

        ERR_FILE_NOT_EXISTS,
        ERR_FILE_NAME_EXISTS,
        ERR_FILE_CREATE_FAILED,
        ERR_FILE_OPEN_FAILED,
        ERR_FILE_CLOSE_FAILED,
        ERR_FILE_DELETE_FAILED,
        ERR_FILE_HANDLE_REUSE,
        ERR_FILE_WRONG_FORMAT,

        ERR_PAGE_READ_EXCEED,
        ERR_PAGE_WRITE_EXCEED,
        ERR_PAGE_BUFFER_INVALID,

        ERR_RECORD_DESCRIPTOR_EMPTY,
        ERR_RECORD_DUPLICATE_DELETE,
        ERR_RECORD_FROM_DATA_FAILED,

        ERR_SLOT_READ_EXCEED,

        ERR_ATTRIBUTE_TYPE_UNDEFINED,

        RBFM_EOF // end of a scan operator

    } RC;

}

#endif // _rc_h_
