#include "src/include/qe.h"
#include <cassert>

namespace PeterDB {
    Filter::Filter(Iterator *input, const Condition &condition) : iter(input), cond(condition) {
        assert(cond.bRhsIsAttr == false);
        assert(cond.op != NO_OP);
        iter->getAttributes(attrs);
        attrsSize = attrs.size();
        for (int i = 0; i < attrsSize; i++) {
            if (cond.lhsAttr == attrs[i].name) {
                condAttrPos = i;
                break;
            }
        }
        assert(condAttrPos != -1);
        assert(attrs[condAttrPos].type == cond.rhsValue.type);
        bitmapBytes = attrsSize % 8 ? attrsSize / 8 + 1 : attrsSize / 8;
        bitmap = malloc(bitmapBytes);
        switch (cond.rhsValue.type) {
            case 0:
                std::memcpy(&condIntBuffer, cond.rhsValue.data, sizeof(int));
                break;
            case 1:
                std::memcpy(&condFloatBuffer, cond.rhsValue.data, sizeof(float));
                break;
            default:
                int length;
                std::memcpy(&length, cond.rhsValue.data, sizeof(int));
                char varCharBuffer[length + 1];
                std::memset(varCharBuffer, 0, length + 1);
                std::memcpy(varCharBuffer, (char *) cond.rhsValue.data + sizeof(int), length);
                condVarCharBuffer = std::string(varCharBuffer);
        }
    }

    Filter::~Filter() {
        attrs.clear();
        free(bitmap);
    }

    RC Filter::getNextTuple(void *data) {
        RC rc = iter->getNextTuple(data);
        if (rc != 0) return rc;
        std::memcpy(bitmap, data, bitmapBytes);
        if (((char *) bitmap)[condAttrPos / 8] >> (7 - condAttrPos % 8) & (unsigned) 1) return getNextTuple(data);
        int offset = bitmapBytes, length;
        for (int i = 0; i < condAttrPos; i++) {
            if (((char *) bitmap)[i / 8] >> (7 - i % 8) & (unsigned) 1) continue;
            length = 0;
            if (attrs[i].type == 2) std::memcpy(&length, (char *) data + offset, sizeof(int));
            offset = offset + length + sizeof(int);
        }
        switch (cond.rhsValue.type) {
            case 0:
                int intBuffer;
                std::memcpy(&intBuffer, (char *) data + offset, sizeof(int));
                switch (cond.op) {
                    case EQ_OP:
                        if (intBuffer == condIntBuffer) return 0;
                        break;
                    case LT_OP:
                        if (intBuffer < condIntBuffer) return 0;
                        break;
                    case LE_OP:
                        if (intBuffer <= condIntBuffer) return 0;
                        break;
                    case GT_OP:
                        if (intBuffer > condIntBuffer) return 0;
                        break;
                    case GE_OP:
                        if (intBuffer >= condIntBuffer) return 0;
                        break;
                    default:
                        if (intBuffer != condIntBuffer) return 0;
                }
                break;
            case 1:
                float floatBuffer;
                std::memcpy(&floatBuffer, (char *) data + offset, sizeof(float));
                switch (cond.op) {
                    case EQ_OP:
                        if (floatBuffer == condFloatBuffer) return 0;
                        break;
                    case LT_OP:
                        if (floatBuffer < condFloatBuffer) return 0;
                        break;
                    case LE_OP:
                        if (floatBuffer <= condFloatBuffer) return 0;
                        break;
                    case GT_OP:
                        if (floatBuffer > condFloatBuffer) return 0;
                        break;
                    case GE_OP:
                        if (floatBuffer >= condFloatBuffer) return 0;
                        break;
                    default:
                        if (floatBuffer != condFloatBuffer) return 0;
                }
                break;
            default:
                std::memcpy(&length, (char *) data + offset, sizeof(int));
                char varChar[length + 1];
                std::memset(varChar, 0, length + 1);
                std::memcpy(varChar, (char *) data + offset + sizeof(int), length);
                std::string varCharBuffer(varChar);
                switch (cond.op) {
                    case EQ_OP:
                        if (varCharBuffer == condVarCharBuffer) return 0;
                        break;
                    case LT_OP:
                        if (varCharBuffer < condVarCharBuffer) return 0;
                        break;
                    case LE_OP:
                        if (varCharBuffer <= condVarCharBuffer) return 0;
                        break;
                    case GT_OP:
                        if (varCharBuffer > condVarCharBuffer) return 0;
                        break;
                    case GE_OP:
                        if (varCharBuffer >= condVarCharBuffer) return 0;
                        break;
                    default:
                        if (varCharBuffer != condVarCharBuffer) return 0;
                }
        }
        return getNextTuple(data);
    }

    RC Filter::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attrs;
        return 0;
    }

    Project::Project(Iterator *input, const std::vector<std::string> &attrNames) : iter(input) {
        iter->getAttributes(attrsBuffer);
        attrsBufferSize = attrsBuffer.size();
        for (const auto &attrName : attrNames) {
            for (int i = 0; i < attrsBufferSize; i++) {
                if (attrsBuffer[i].name == attrName) {
                    attrs.push_back(attrsBuffer[i]);
                    break;
                }
            }
        }
        bitmapBufferBytes = attrsBufferSize % 8 ? attrsBufferSize / 8 + 1 : attrsBufferSize / 8;
        bitmapBuffer = malloc(bitmapBytes);
        attrsSize = attrs.size();
        bitmapBytes = attrsSize % 8 ? attrsSize / 8 + 1 : attrsSize / 8;
        bitmap = malloc(bitmapBytes);
        dataBuffer = malloc(PAGE_SIZE);
    }

    Project::~Project() {
        attrs.clear();
        free(bitmapBuffer);
        free(bitmap);
        free(dataBuffer);
    }

    RC Project::getNextTuple(void *data) {
        RC rc = iter->getNextTuple(dataBuffer);
        if (rc != 0) return rc;
        std::memcpy(bitmapBuffer, dataBuffer, bitmapBufferBytes);
        std::vector<std::pair<bool, std::pair<int, int>>> offsets;
        int offset = bitmapBufferBytes, length;
        bool isNull;
        for (int i = 0; i < attrsBufferSize; i++) {
            isNull = ((char *) bitmapBuffer)[i / 8] >> (7 - i % 8) & (unsigned) 1;
            if (isNull) {
                offsets.push_back({isNull, {0, 0}});
                continue;
            }
            if (attrsBuffer[i].type != 2) length = 4;
            else {
                std::memcpy(&length, (char *) dataBuffer + offset, sizeof(int));
                length = length + 4;
            }
            offsets.push_back({isNull, {offset, length}});
            offset = offset + length;
        }
        std::memset(bitmap, 0, bitmapBytes);
        offset = bitmapBytes;
        for (int i = 0; i < attrsSize; i++) {
            for (int j = 0; j < attrsBufferSize; j++) {
                if (attrsBuffer[j].name == attrs[i].name){
                    if (offsets[j].first) ((char *) bitmap)[i / 8] |= (unsigned) 1 << (7 - i % 8);
                    else {
                        std::memcpy((char *) data + offset, (char *) dataBuffer + offsets[j].second.first, offsets[j].second.second);
                        offset = offset + offsets[j].second.second;
                    }
                }
            }
        }
        std::memcpy(dataBuffer, bitmap, bitmapBytes);
        return 0;
    }

    RC Project::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attrs;
        return 0;
    }

    BNLJoin::BNLJoin(Iterator *leftIn, TableScan *rightIn, const Condition &condition, const unsigned int numPages) : outer(leftIn), inner(rightIn), cond(condition), numPages(numPages) {
        assert(cond.bRhsIsAttr == true && cond.op == EQ_OP);
        leftIn->getAttributes(leftAttrs);
        rightIn->getAttributes(rightAttrs);
        for (const auto &attr : leftAttrs) attrs.push_back(attr);
        for (const auto &attr : rightAttrs) attrs.push_back(attr);

        leftAttrsSize = leftAttrs.size();
        rightAttrsSize = rightAttrs.size();
        attrsSize = leftAttrsSize + rightAttrsSize;

        for (int i = 0; i < leftAttrsSize; i++) {
            if (leftAttrs[i].name == cond.lhsAttr) {
                outerAttrPos = i;
                break;
            }
        }
        assert(outerAttrPos != -1);

        for (int i = 0; i < rightAttrsSize; i++) {
            if (rightAttrs[i].name == cond.rhsAttr) {
                innerAttrPos = i;
                break;
            }
        }
        assert(innerAttrPos != -1);
        assert(leftAttrs[outerAttrPos].type == rightAttrs[innerAttrPos].type);

        leftBitmapBytes = leftAttrsSize % 8 ? leftAttrsSize / 8 + 1 : leftAttrsSize / 8;
        rightBitmapBytes = rightAttrsSize % 8 ? rightAttrsSize / 8 + 1 : rightAttrsSize / 8;
        bitmapBytes = attrsSize % 8 ? attrsSize / 8 + 1 : attrsSize / 8;
        leftBitmap = malloc(leftBitmapBytes);
        rightBitmap = malloc(rightBitmapBytes);
        bitmap = malloc(bitmapBytes);

        innerBuffer = malloc(PAGE_SIZE);
        outerBuffer = malloc(numPages * PAGE_SIZE);
        innerTupleBuffer = malloc(PAGE_SIZE);
        outerTupleBuffer = malloc(PAGE_SIZE);
    }

    BNLJoin::~BNLJoin() {
        attrs.clear();
        leftAttrs.clear();
        rightAttrs.clear();
        intHm.clear();
        floatHm.clear();
        varCharHm.clear();
        innerBufferDirectory.clear();
        free(leftBitmap);
        free(rightBitmap);
        free(bitmap);
        free(innerBuffer);
        free(outerBuffer);
        free(innerTupleBuffer);
        free(outerTupleBuffer);
    }

    int BNLJoin::getTupleLength(char *tupleBuffer, const std::vector<Attribute> &attrs, int attrsSize, char *bitmap, int bitmapBytes) {
        int tupleLength = bitmapBytes, length;
        std::memcpy(bitmap, tupleBuffer, bitmapBytes);
        for (int i = 0; i < attrsSize; i++) {
            if (bitmap[i / 8] >> (7 - i % 8) & (unsigned) 1) continue;
            length = 0;
            if (attrs[i].type == 2) std::memcpy(&length, tupleBuffer + tupleLength, sizeof(int));
            tupleLength = tupleLength + length + sizeof(int);
        }
        return tupleLength;
    }

    bool
    BNLJoin::getAttr(char *tupleBuffer, const std::vector<Attribute> &attrs, int attrPos, char *bitmap,
                     int bitmapBytes, int &intBuffer, float &floatBuffer, std::string &varCharBuffer) {
        std::memcpy(bitmap, tupleBuffer, bitmapBytes);
        if (bitmap[attrPos / 8] >> (7 - attrPos % 8) & (unsigned) 1) return false;
        int offset = 0, length;
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

    void BNLJoin::fillInnerBuffer() {
        RC rc;
        if (!innerHasNext) {
            inner->setIterator();
            rc = inner->getNextTuple(innerTupleBuffer);
            if (rc != 0) return;
        }
        innerBufferDirectory.clear();
        innerBufferIndex = 0;
        int tupleLength = getTupleLength((char *) innerTupleBuffer, rightAttrs, rightAttrsSize, (char *) rightBitmap, rightBitmapBytes);
        int offset = 0;
        while (offset + tupleLength <= PAGE_SIZE) {
            if ((((char *) rightBitmap)[innerAttrPos / 8] >> (7 - innerAttrPos % 8) & (unsigned) 1) == 0) {
                std::memcpy((char *) innerBuffer + offset, innerTupleBuffer, tupleLength);
                innerBufferDirectory.emplace_back(offset, tupleLength);
                offset = offset + tupleLength;
            }
            rc = inner->getNextTuple(innerTupleBuffer);
            if (rc != 0) {
                innerHasNext = false;
                break;
            }
            tupleLength = getTupleLength((char *) innerTupleBuffer, rightAttrs, rightAttrsSize, (char *) rightBitmap, rightBitmapBytes);
        }
    }

    RC BNLJoin::fillOuterBuffer() {
        if (!outerHasNext) return QE_EOF;
        RC rc;
        if (!outerFillingStarted) {
            rc = outer->getNextTuple(outerTupleBuffer);
            if (rc != 0) return rc;
            outerFillingStarted = true;
        }
        intHm.clear();
        floatHm.clear();
        varCharHm.clear();
        int tupleLength = getTupleLength((char *)outerTupleBuffer, leftAttrs, leftAttrsSize, (char *) leftBitmap, leftBitmapBytes);
        int offset = 0;
        int intBuffer; float floatBuffer; std::string varCharBuffer;
        while (offset + tupleLength <= numPages * PAGE_SIZE) {
            if (getAttr((char *) outerTupleBuffer, leftAttrs, outerAttrPos, (char *) leftBitmap, leftBitmapBytes, intBuffer, floatBuffer, varCharBuffer)) {
                switch (leftAttrs[outerAttrPos].type) {
                    case 0:
                        intHm[intBuffer].emplace_back(offset, tupleLength);
                        break;
                    case 1:
                        floatHm[floatBuffer].emplace_back(offset, tupleLength);
                        break;
                    default:
                        varCharHm[varCharBuffer].emplace_back(offset, tupleLength);
                }
                std::memcpy((char *) outerBuffer + offset,outerTupleBuffer, tupleLength);
                offset = offset + tupleLength;
            }
            rc = outer->getNextTuple(outerTupleBuffer);
            if (rc != 0) {
                outerHasNext = false;
                break;
            }
            tupleLength = getTupleLength((char *)outerTupleBuffer, leftAttrs, leftAttrsSize, (char *) leftBitmap, leftBitmapBytes);
        }
        return 0;
    }

    void BNLJoin::joinTuples(int leftOffset, int leftLength, int rightOffset, int rightLength, void *data) {
        std::memcpy(leftBitmap, (char *) outerBuffer + leftOffset, leftBitmapBytes);
        std::memcpy(rightBitmap, (char *) innerBuffer + rightOffset, rightBitmapBytes);
        std::memset(bitmap, 0 , bitmapBytes);
        for (int i = 0; i < leftAttrsSize; i++) {
            if (((char *) leftBitmap)[i / 8] >> (7 - i % 8) & (unsigned) 1) {
                ((char *) bitmap)[i / 8] |= (unsigned) 1 << (7 - i % 8);
            }
        }
        for (int i = 0; i < rightAttrsSize; i++) {
            if (((char *) rightBitmap)[i / 8] >> (7 - i % 8) & (unsigned) 1) {
                ((char *) bitmap)[(i + leftAttrsSize) / 8] |= (unsigned) 1 << (7 - (i + leftAttrsSize) % 8);
            }
        }
        std::memcpy(data, bitmap, bitmapBytes);
        std::memcpy((char *) data + bitmapBytes, (char *) outerBuffer + leftOffset + leftBitmapBytes, leftLength - leftBitmapBytes);
        std::memcpy((char *) data + bitmapBytes + leftLength - leftBitmapBytes, (char *) innerBuffer + rightOffset + rightBitmapBytes, rightLength - rightBitmapBytes);
    }

    RC BNLJoin::getNextTuple(void *data) {
        if (innerBufferIndex >= innerBufferDirectory.size()) {
            if (!innerHasNext) {
                RC rc = fillOuterBuffer();
                if (rc != 0) return rc;
            }
            fillInnerBuffer();
            if (innerBufferDirectory.empty()) return QE_EOF;
        }
        int intBuffer; float floatBuffer; std::string varCharBuffer;
        getAttr((char *) innerBuffer + innerBufferDirectory[innerBufferIndex].first, rightAttrs, innerAttrPos, (char *) rightBitmap, rightBitmapBytes, intBuffer, floatBuffer, varCharBuffer);
        switch (rightAttrs[innerAttrPos].type){
            case 0:
                if (outerBufferIndex >= intHm[intBuffer].size()) {
                    outerBufferIndex = 0;
                    innerBufferIndex = innerBufferIndex + 1;
                    return getNextTuple(data);
                }
                break;
            case 1:
                if (outerBufferIndex >= floatHm[floatBuffer].size()) {
                    outerBufferIndex = 0;
                    innerBufferIndex = innerBufferIndex + 1;
                    return getNextTuple(data);
                }
                break;
            default:
                if (outerBufferIndex >= varCharHm[varCharBuffer].size()) {
                    outerBufferIndex = 0;
                    innerBufferIndex = innerBufferIndex + 1;
                    return getNextTuple(data);
                }
        }
        joinTuples(intHm[intBuffer][outerBufferIndex].first, intHm[intBuffer][outerBufferIndex].second, innerBufferDirectory[innerBufferIndex].first, innerBufferDirectory[innerBufferIndex].second, data);
        outerBufferIndex = outerBufferIndex + 1;
        return 0;
    }

    RC BNLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        attrs.clear();
        attrs = this->attrs;
        return 0;
    }

    INLJoin::INLJoin(Iterator *leftIn, IndexScan *rightIn, const Condition &condition) {

    }

    INLJoin::~INLJoin() {

    }

    RC INLJoin::getNextTuple(void *data) {
        return -1;
    }

    RC INLJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    GHJoin::GHJoin(Iterator *leftIn, Iterator *rightIn, const Condition &condition, const unsigned int numPartitions) {

    }

    GHJoin::~GHJoin() {

    }

    RC GHJoin::getNextTuple(void *data) {
        return -1;
    }

    RC GHJoin::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, AggregateOp op) {

    }

    Aggregate::Aggregate(Iterator *input, const Attribute &aggAttr, const Attribute &groupAttr, AggregateOp op) {

    }

    Aggregate::~Aggregate() {

    }

    RC Aggregate::getNextTuple(void *data) {
        return -1;
    }

    RC Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        return -1;
    }
} // namespace PeterDB
