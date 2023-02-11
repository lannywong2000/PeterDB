## Project 2 Report


### 1. Basic information
- Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter23-lannywong2000
- Student UCI ID: 38242300
- Student UCI NetID: ruoyuw15
- Student Name: Lanny Wang


### 2. Meta-data
- Show your meta-data design (Tables and Columns table) and information about each column.
    - Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
    - Columns (table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int, table-version:int)


### 3. Internal Record Format (in case you have changed from P1, please re-enter here)
- Show your record format design.

    - There are two types of records, the actual record and the tombstone that points to another record.
    - The first byte of a record is a 1-byte tombstone indicator, where the left most bit representing whether it is a tombstone (1 is and 0 is not).
    - An actual record consists of five parts: the tombstone indicator, the table version number, the number of attributes, the offsets representing the length of bytes from the start of record to the ending position of each attribute, and the values of attributes.
      - The table version number is a 4-byte unsigned int that shows which table version this record belongs.
      - The number of attributes is of type unsigned short and takes up 2 bytes.
      - Every offset is an unsigned short which contains 2 bytes.
      - The values of attribute are stored in the end of record accordingly, with int of 4 bytes, float of 4 bytes, varchar of its length bytes, and NULL of 0 byte.
    - A tombstone record consists of three parts: the tombstone indicator, a 4-byte unsigned int page number, and a 2-byte unsigned short slot number.

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

### 4. Page Format (in case you have changed from P1, please re-enter here)
- Show your page format design.

    - The records are stored from the head of a page.
    - The slot directory rests in the end of a page, and the slots grow from the back.
    - All the free space resides in the middle of a page.

- Explain your slot directory design if applicable.

    - The last 2 bytes of a page store the start of the free space from the start of page in bytes as an usigned short.
    - The second last 2 bytes stores the number of slots in the slot directory as an unsigned short.
    - Slots grow from the last 4 bytes of a page. Each slot is a 4-byte struct which contains a short as the offset of the record and an unsigned short as the record's length in bytes.

### 5. Page Management (in case you have changed from P1, please re-enter here)
- How many hidden pages are utilized in your design?

    - One hidden page as the first page of the file.

- Show your hidden page(s) format design if applicable

    - The hidden page stores 5 unsigned from the start, including numberOfPages, readPageCounter, writePageCounter, appendPageCounter, and the current table version.
    - The rest of the page is not used for now.

### 6. Describe the following operation logic.
- Delete a record

  1. Load the page of rid.pageNum into a page buffer.
  2. Find the slot of the record using rid.slotNum.
  3. Load the record corresponding to the slot into a record buffer.
  4. Shift the records behind the record in the same page to the left slot.length bytes in the page buffer.
  5. Set the slot.offset to -1 in the page buffer.
  6. If this slot is the leftmost slot of the slot directory, decrement the number of slot by 1 in the page buffer.
  7. Flush the page buffer to disk.
  8. Check the tombstone indicator in the record buffer, if it is a tombstone, call the delete record function recursively on the record it is pointing to.

- Update a record

  1. In the following description, the offset of records in the slots are correspondingly updated when records are being shifted left.
  2. If the length of the new record is shorter than the original one. Simply overwrite the record and shift the records behind it within a page to the left. Update the length in the slot of the current record.
  3. If the length of the record is longer than the original one, but the current page still have enough space for it. First shift all the records behind it within a page left to the offset of the record that is being updated. Then insert the updated record to the start of the free space and update its slot.
  4. If the record is too large to reside in its original page, call the insert record function and create a tombstone record with the returned rid. Update the record with the tombstone record as described in step ii.

- Scan on normal records

  - Scanning records is done by traversing all the slots page by page.
  - If the slot has non-minus-one offset and the tombstone indicator bit of the record is 0, cache the rid of the record for future retreving.

- Scan on deleted records

  - If a slot has offset value of -1, it means that the record it is pointing to has been deleted. Then this slot is disregarded and the scanning continues.

- Scan on updated records

  - If the slot offset points to a record with the tombstone indicator bit of 1, this means this record is pointing another record that may also be a tombstone. In this case, this record is also neglected. The actual record it is pointing to will eventually be reached and it is scanned as a normal record.

### 7. Schema Versioning

- What does add attributes do?

  1. Increment the current table version by 1.
  2. Query all the records in Columns table with corresponding table id and the last table version.
  3. Insert them into the Columns table again with the new table version.
  4. Assign a unique column position to the new attribute as the largest colum position of this table plus one.
  5. Insert the new attribute to the Columns table with the new table version and its column position.

- What does drop attributes do?

  1. Increment the current table version by 1.
  2. Query all the records in Columns table with corresponding table id and the last table version.
  3. Insert them, except for the dropped attribute, into the Columns table again with the new table version.

- How to insert/update a tuple with schema versioning?

  - Store the current table version in the record when inserting and updating tuples.

- How to read a tuple with schema versioning?

  - Acquire the table version of the desired record and fetch the record descriptors of that version from Columns table.
  - Call the read attributes function with the record descriptors in i and receive the record data formatted correspondingly.
  - Compare the record descriptors in i and the latest record descriptors and convert the record data in relation manager. Attributes can be distinguished solely using column position even with the same column name. 

- How to read an attribute with schema versioning?

  - The same mentality as read a tuple.
  - Retrieve the attribute with its corresponding version of record descriptors.
  - Do the version conversion in relation manager.
