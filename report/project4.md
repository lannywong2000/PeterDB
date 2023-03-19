## Project 4 Report


### 1. Basic information
- Github Repo Link: https://github.com/UCI-Chenli-teaching/cs222p-winter23-lannywong2000
- Student UCI ID: 38242300
- Student UCI NetID: ruoyuw15
- Student Name: Lanny Wang


### 2. Catalog information about Index
- Show your catalog information about an index (tables, columns).

  - A separate catalog file (table) called Indexes is created for storing index information.
  - Indexes (table-id:int, column-position:int).
  - Indexes table has two attributes, the table id and the position of the column that the respective index is built on.


### 3. Filter
- Describe how your filter works (especially, how you check the condition.)

  - Filtering is a stateless operations. Therefore, condition can be checked on each getNextTuple call on the fly.
  - If the condition is not satisfied, call getNextTuple recursively until no tuple can be produced from the input.


### 4. Project
- Describe how your project works.

  - Projecting is a stateless operations. Therefore, desired attributes can be selected on each getNextTuple call on the fly.
  - In getNextTuple, reorder the null bitmap and the attributes of the produced tuple from the input.

### 5. Block Nested Loop Join
- Describe how your block nested loop join works (especially, how you manage the given buffers.)

  - Allocate numPages pages for the outer table buffer, one page for the inner table buffer.
  - If the inner iterator has reached the end, fill up the outer table buffer and reset the inner iterator.
  - If the outer iterator has reached the end, return QE_EOF.
  - Keep twp pointers for the outer and inner table buffers respectively.
  - For every tuple pointed by the inner pointer, try joining it with the tuple pointed by the outer pointer Increment the outer pointer.
  - If the inner pointer has reached the end of inner table buffer, fill up the inner table buffer from inner iterator.


### 6. Index Nested Loop Join
- Describe how your index nested loop join works.

  - For every tuple in the outer table, reset the inner iterator with its join attribute value.
  - Produce a tuple from the inner iterator when getNextTuple is called.


### 7. Grace Hash Join (If you have implemented this feature)
- Describe how your grace hash join works (especially, in-memory structure).



### 8. Aggregation
- Describe how your basic aggregation works.

  - Scan the tuples from input iterator in initialization.
  - Keep two variables during the scan, a sum and a counter.
  - Calculate the output corresponding to the aggregation operator.


- Describe how your group-based aggregation works. (If you have implemented this feature)

  - Scan the tuples from input iterator in initialization.
  - Keep two variables during the scan, a sum and a counter for each group attribute value, store them in a hashmap.
  - Calculate the output corresponding to the aggregation operator.

### 9. Implementation Detail
- Have you added your own module or source file (.cc or .h)?
  Clearly list the changes on files and CMakeLists.txt, if any.
  - rc.h: self-defined return codes
