## Project 3 Report


### 1. Basic information
- Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter23-lannywong2000
- Student UCI ID: 38242300
- Student UCI NetID: ruoyuw15
- Student Name: Lanny Wang


### 2. Meta-data page in an index file
- Show your meta-data page of an index design if you have any. 

  - The first page of an index file (i.e. the second page including the hidden page) is reserved for index meta-data.
  - The first 4 bytes contain an integer for the index root page number.
  - The following 4 bytes contain an unsigned integer for the index's attribute type.


### 3. Index Entry Format
- Show your index entry design (structure).

  - entries on internal nodes:
    - int / float: 4 bytes of actual data + RID
    - varChar: 4 bytes integer for length + length bytes of actual data + RID

  - entries on leaf nodes:
    - int / float: 4 bytes of actual data + RID
    - varChar: 4 bytes integer for length + length bytes of actual data + RID


### 4. Page Format
- Show your internal-page (non-leaf node) design.
  - Indicator: 1 byte, with first bit set to 0
  - EntryListSize: 2 byte unsigned short
  - Pointers: (EntryListSize + 1) * sizeof(PageNum)
  - Entries: EntryListSize entries on internal nodes

- Show your leaf-page (leaf node) design.
  - Indicator: 1 byte, with first bit set to 1
  - EntryListSize: 2 byte unsigned short
  - Entries: EntryListSize entries on leaf nodes


### 5. Describe the following operation logic.
- Split

  - Split leaf L:
    - First half entries stay, move the rest to a brand new node L2
    - Set newChildEntry = ((smallest key value on L2, pointer to L2))
    - Set sibling pointers in L and L2
    - If L is the root:
      - Create a new node with (pointer to L, newChildEntry)
      - Make the root node pointer point to the new node

  - Split internal node N (with EntryListSize of 2d + 1)
    - First d key values and d + 1 node pointers stay,
    - Last d keys and d + 1 pointers move to new node, N2;
    - Set newChildEntry = (((d + 1)th key value, pointer to N2))
    - if N is the root:
      - Create a new node with (pointer to N, newChildEntry)
      - Make the root node pointer point to the new node

- Rotation (if applicable)

- Merge/non-lazy deletion (if applicable)

- Duplicate key span in a page / multiple pages
  - Composite keys (key, RID) are stored in the entries of leaves and internal nodes.
  - Therefore, all the keys are basically unique and entries with duplicate key name can span across arbitrary number of pages without keeping a list of RIDs in the leaf nodes.

### 6. Implementation Detail
- Have you added your own module or source file (.cc or .h)? 
  Clearly list the changes on files and CMakeLists.txt, if any.
  - rc.h: self-defined return codes

