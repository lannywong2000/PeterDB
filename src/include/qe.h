#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <string>
#include <limits>
#include <unordered_map>
#include <cassert>

#include "rm.h"
#include "ix.h"

namespace PeterDB {

#define QE_EOF (-1)  // end of the index scan
    typedef enum AggregateOp {
        MIN = 0, MAX, COUNT, SUM, AVG
    } AggregateOp;

    // The following functions use the following
    // format for the passed data.
    //    For INT and REAL: use 4 bytes
    //    For VARCHAR: use 4 bytes for the length followed by the characters

    typedef struct Value {
        AttrType type;          // type of value
        void *data;             // value
    } Value;

    typedef struct Condition {
        std::string lhsAttr;        // left-hand side attribute
        CompOp op;                  // comparison operator
        bool bRhsIsAttr;            // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
        std::string rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
        Value rhsValue;             // right-hand side value if bRhsIsAttr = FALSE
    } Condition;

    class Iterator {
        // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;

        virtual RC getAttributes(std::vector<Attribute> &attrs) const = 0;

        virtual ~Iterator() = default;

        bool getAttr(char *tupleBuffer, const std::vector<Attribute> &attrs, int attrPos, char *bitmap,
                     int bitmapBytes, int &intBuffer, float &floatBuffer, std::string &varCharBuffer) {
            std::memcpy(bitmap, tupleBuffer, bitmapBytes);
            if (bitmap[attrPos / 8] >> (7 - attrPos % 8) & (unsigned) 1) return false;
            int offset = bitmapBytes, length;
            for (int i = 0; i < attrPos; i++) {
                if (bitmap[i / 8] >> (7 - i % 8) & (unsigned) 1) continue;
                length = 0;
                if (attrs[i].type == 2) std::memcpy(&length, tupleBuffer + offset, sizeof(int));
                offset = offset + sizeof(int) + length;
            }
            switch (attrs[attrPos].type) {
                case 0:
                    std::memcpy(&intBuffer, tupleBuffer + offset, sizeof(int));
                    break;
                case 1:
                    std::memcpy(&floatBuffer, tupleBuffer + offset, sizeof(float));
                    break;
                default:
                    std::memcpy(&length, tupleBuffer + offset, sizeof(int));
                    char varChar[length + 1];
                    std::memset(varChar, 0, length + 1);
                    std::memcpy(varChar, tupleBuffer + offset + sizeof(int), length);
                    varCharBuffer = std::string(varChar);
            }
            return true;
        }
    };

    class TableScan : public Iterator {
        // A wrapper inheriting Iterator over RM_ScanIterator
    private:
        RelationManager &rm;
        RM_ScanIterator iter;
        std::string tableName;
        std::vector<Attribute> attrs;
        std::vector<std::string> attrNames;
        RID rid;
    public:
        TableScan(RelationManager &rm, const std::string &tableName, const char *alias = nullptr) : rm(rm) {
            //Set members
            this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            for (const Attribute &attr : attrs) {
                // convert to char *
                attrNames.push_back(attr.name);
            }

            // Call RM scan to get an iterator
            rm.scan(tableName, "", NO_OP, nullptr, attrNames, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator() {
            iter.close();
            rm.scan(tableName, "", NO_OP, nullptr, attrNames, iter);
        };

        RC getNextTuple(void *data) override {
            return iter.getNextTuple(rid, data);
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }
            return 0;
        };

        ~TableScan() override {
            iter.close();
        };
    };

    class IndexScan : public Iterator {
        // A wrapper inheriting Iterator over IX_IndexScan
    private:
        RelationManager &rm;
        RM_IndexScanIterator iter;
        std::string tableName;
        std::string attrName;
        std::vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;
    public:
        IndexScan(RelationManager &rm, const std::string &tableName, const std::string &attrName,
                  const char *alias = nullptr) : rm(rm) {
            // Set members
            this->tableName = tableName;
            this->attrName = attrName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator
            rm.indexScan(tableName, attrName, nullptr, nullptr, true, true, iter);

            // Set alias
            if (alias) this->tableName = alias;
        };

        // Start a new iterator given the new key range
        void setIterator(void *lowKey, void *highKey, bool lowKeyInclusive, bool highKeyInclusive) {
            iter.close();
            rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive, highKeyInclusive, iter);
        };

        RC getNextTuple(void *data) override {
            RC rc = iter.getNextEntry(rid, key);
            if (rc == 0) {
                rc = rm.readTuple(tableName, rid, data);
            }
            return rc;
        };

        RC getAttributes(std::vector<Attribute> &attributes) const override {
            attributes.clear();
            attributes = this->attrs;

            // For attribute in std::vector<Attribute>, name it as rel.attr
            for (Attribute &attribute : attributes) {
                attribute.name = tableName + "." + attribute.name;
            }

            return 0;
        };

        ~IndexScan() override {
            iter.close();
        };
    };

    class Filter : public Iterator {
        Iterator *iter;
        std::vector<Attribute> attrs;
        const Condition &cond;
        int condAttrPos = -1, attrsSize, bitmapBytes, condIntBuffer;
        float condFloatBuffer;
        void *bitmap = nullptr;
        std::string condVarCharBuffer;

        // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );

        ~Filter() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Project : public Iterator {
        Iterator *iter;
        int attrsBufferSize, bitmapBufferBytes, attrsSize, bitmapBytes;
        std::vector<Attribute> attrsBuffer, attrs;
        void *dataBuffer = nullptr, *bitmapBuffer = nullptr, *bitmap = nullptr;

        // Projection operator
    public:
        Project(Iterator *input,                                // Iterator of input R
                const std::vector<std::string> &attrNames);     // std::vector containing attribute names
        ~Project() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class BNLJoin : public Iterator {
        Iterator *outer;
        TableScan *inner;
        const Condition &cond;
        const unsigned int numPages;
        std::vector<Attribute> attrs, leftAttrs, rightAttrs;
        int leftAttrsSize, rightAttrsSize, attrsSize, leftBitmapBytes, rightBitmapBytes, bitmapBytes, innerAttrPos = -1, outerAttrPos = -1;
        void *leftBitmap = nullptr, *rightBitmap = nullptr, *bitmap = nullptr, *innerBuffer = nullptr, *outerBuffer = nullptr, *innerTupleBuffer = nullptr, *outerTupleBuffer = nullptr;
        std::unordered_map<int, std::vector<std::pair<int, int>>> intHm;
        std::unordered_map<float, std::vector<std::pair<int, int>>> floatHm;
        std::unordered_map<std::string, std::vector<std::pair<int, int>>> varCharHm;
        bool innerHasNext = false, outerHasNext = true, outerFillingStarted = false;
        int innerBufferIndex = 0, outerBufferIndex = 0;
        std::vector<std::pair<int, int>> innerBufferDirectory;

        int getTupleLength(char *tupleBuffer, const std::vector<Attribute> &attrs, int attrsSize, char *bitmap, int bitmapBytes);

        void fillInnerBuffer();

        RC fillOuterBuffer();

        void joinTuples(int leftOffset, int leftLength, int rightOffset, int rightLength, void *data);

        // Block nested-loop join operator
    public:
        BNLJoin(Iterator *leftIn,            // Iterator of input R
                TableScan *rightIn,           // TableScan Iterator of input S
                const Condition &condition,   // Join condition
                const unsigned numPages       // # of pages that can be loaded into memory,
                //   i.e., memory block size (decided by the optimizer)
        );

        ~BNLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class INLJoin : public Iterator {
        // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
                IndexScan *rightIn,          // IndexScan Iterator of input S
                const Condition &condition   // Join condition
        );

        ~INLJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    // 10 extra-credit points
    class GHJoin : public Iterator {
        // Grace hash join operator
    public:
        GHJoin(Iterator *leftIn,               // Iterator of input R
               Iterator *rightIn,               // Iterator of input S
               const Condition &condition,      // Join condition (CompOp is always EQ)
               const unsigned numPartitions     // # of partitions for each relation (decided by the optimizer)
        );

        ~GHJoin() override;

        RC getNextTuple(void *data) override;

        // For attribute in std::vector<Attribute>, name it as rel.attr
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };

    class Aggregate : public Iterator {
        const std::vector<std::string> ops = {"MIN", "MAX", "COUNT", "SUM", "AVG"};
        Iterator *iter;
        const Attribute &aggAttr;
        AggregateOp op;
        const Attribute *groupAttr;
        bool hasGroupBy, singleResultReturned = false;
        std::vector<Attribute> attrs, outputAttrs;
        int attrsSize, bitmapBytes, aggAttrPos = -1, groupAttrPos = -1;
        void *bitmap = nullptr, *tupleBuffer = nullptr;
        std::unordered_map<int, std::pair<float, int>> intHm;
        std::unordered_map<float, std::pair<float, int>> floatHm;
        std::unordered_map<std::string, std::pair<float, int>> varCharHm;
        std::vector<std::pair<int, float>> intResults;
        std::vector<std::pair<float, float>> floatResults;
        std::vector<std::pair<std::string, float>> varCharResults;
        int resultIndex;

        RC getResult(void *data);

        RC getGroupResult(void *data);

        // Aggregation operator
    public:
        // Mandatory
        // Basic aggregation
        Aggregate(Iterator *input,          // Iterator of input R
                  const Attribute &aggAttr,        // The attribute over which we are computing an aggregate
                  AggregateOp op            // Aggregate operation
        );

        // Optional for everyone: 5 extra-credit points
        // Group-based hash aggregation
        Aggregate(Iterator *input,             // Iterator of input R
                  const Attribute &aggAttr,           // The attribute over which we are computing an aggregate
                  const Attribute &groupAttr,         // The attribute over which we are grouping the tuples
                  AggregateOp op              // Aggregate operation
        );

        ~Aggregate() override;

        RC getNextTuple(void *data) override;

        // Please name the output attribute as aggregateOp(aggAttr)
        // E.g. Relation=rel, attribute=attr, aggregateOp=MAX
        // output attrName = "MAX(rel.attr)"
        RC getAttributes(std::vector<Attribute> &attrs) const override;
    };
} // namespace PeterDB

#endif // _qe_h_
