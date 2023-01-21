## Project 1 Report


### 1. Basic information
 - Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter23-lannywong2000/blob/main/src/include/rbfm.h
 - Student UCI NetID: 38242300
 - Student Name: Lanny Wang


### 2. Internal Record Format
- Show your record format design.

    - A record consists of three parts: the number of attributes, the offsets representing the length of bytes from the start of record to the ending position of each attribute, and the values of attributes. 
    - The number of attributes is of type unsigned short and takes up 2 bytes. 
    - Every offset is an unsigned short which contains 2 bytes. 
    - The values of attribute are stored in the end of record accordingly, with int of 4 bytes, float of 4 bytes, varchar of its length bytes, and NULL of 0 byte.

- Describe how you store a null field.

    - A null value is indicated by setting its offset value to 0. 
    - This is acceptable since the number of attributes is always stores at the head of a record. 
    - No data of NULL is stored in the values of attribute section.

- Describe how you store a VarChar field.

    - The VarChar is stored as a byte array of the actual length of its value. 
    - The offset of the non-NULL field before it indicated it starting position and its own offset points to its ending position.

- Describe how your record design satisfies O(1) field access.

    - O(1) field access is achieved by the offset array. 
    - For fixed length field, read the fixed length of bytes to the offset. 
    - For VarChar, read from the offset of the non-NULL field before it and to its own offset.

### 3. Page Format
- Show your page format design.

    - The records are stored from the head of a page.
    - The slot directory rests in the end of a page, and the slots grow from the back.
    - All the free space resides in the middle of a page. 

- Explain your slot directory design if applicable.

    - The last 2 bytes of a page store the start of the free space from the start of page in bytes as an usigned short.
    - The second last 2 bytes stores the number of slots in the slot directory as an unsigned short.
    - Slots grow from the last 4 bytes of a page. Each slot is a 4-bytes struct which contains a short as the offset of the record and an unsigned short as the record's length in bytes.

### 4. Page Management
- Show your algorithm of finding next available-space page when inserting a record.

    - A current page number is associated with each fileHandle.
    - The free space in the current page is calculated and compared with the required space. If the record fits, insert the record in the current page.
    - Otherwise, scan each page in the file from the start and calculate the free space. If the record fits, store the record in that page and set the curretn page number accordingly.
    - If no page is available in the file, append a new page to the end and insert the record, set the curretn page number accordingly.

- How many hidden pages are utilized in your design?

    - One hidden page as the first page of the file.

- Show your hidden page(s) format design if applicable

    - The hidden page stores 4 unsigned from the start, including numberOfPages, readPageCounter, writePageCounter and appendPageCounter. 
    - The rest of the page is not used for now.

### 5. Implementation Detail
- Other implementation details goes here.

  - As mentioned above, maintaining a current page number for each fileHandle is beneficial since it avoids finding free space from the first page of file every time a record is inserted. Thus speeding up the insertion. 

### 6. Other (optional)
- Freely use this section to tell us about things that are related to the project 1, but not related to the other sections (optional)



- Feedback on the project to help improve the project. (optional)