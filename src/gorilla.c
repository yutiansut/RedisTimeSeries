/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/

#include <assert.h>

#include "gorilla.h"


#define _64BIT 64
#define DOUBLE_LEADING 5
#define DOUBLE_BLOCK_SIZE 6
#define DOUBLE_BLOCK_ADJUST 1

// Ensures enough space is available otherwise, returns false 
#define CHECKSPACE(chunk, x)    if (!isSpaceAvailable(chunk, x)) { \
                                    return CR_ERR; }

#define  LeadingZeros64(x)  __builtin_clzll(x)
#define TrailingZeros64(x)  __builtin_ctzll(x)

// Clear all bits from position `bits`
static u_int64_t clearBits(u_int64_t data, u_int64_t bits) {
    return data & ((1LL << bits) - 1);
}

// Converts `x`, an int64, to binary representation with length `l`
static u_int64_t int2bin(int64_t x, u_int8_t l) {
    u_int64_t negative = x < 0 ? 1 : 0;
    u_int64_t rv = clearBits(x, l - 1);
    rv |= (negative << (l - 1));
    return rv;
}

// Converts `x`, a binary of length `l`, into an int64
static int64_t bin2int(u_int64_t x, u_int8_t l) {
    bool negative = x & (1LL << (l - 1));
    int64_t rv = x;
    if (negative) {
        rv -= 1LL << l;
    }
    return rv;
}

// Return `true` if bit is off
static bool isBitOff(u_int64_t *x, u_int64_t idx) {
    return !(x[idx / 64LL] & (1LL << (idx % 64)));
}

// Append `dataLen` bits from `data` into `arr` at position `idx`
static void appendBits(u_int64_t *arr, u_int64_t *idx, u_int64_t data, u_int8_t dataLen) {
    u_int8_t idx64 = *idx % _64BIT;
    u_int8_t available = _64BIT - idx64;
    u_int64_t *runner = &arr[*idx / _64BIT];

    if(available >= dataLen) {
        *runner |= clearBits(data, dataLen) << idx64;
    } else {
        *runner |= data << idx64;
        *(++runner) |= clearBits(data, dataLen) >> available;
    }
    *idx += dataLen;
}

// Read `dataLen` bits from `arr` at position `idx`
static u_int64_t readBits(u_int64_t *arr, u_int64_t *idx, u_int8_t dataLen) {
    u_int64_t rv = 0;
    u_int8_t idx64 = *idx % _64BIT;
    u_int8_t available = _64BIT - idx64;
    u_int64_t *runner = &arr[*idx / _64BIT];

    if(available >= dataLen) {
        rv = clearBits(*runner >> idx64, dataLen);
    } else {
        u_int8_t left = dataLen - available;
        rv = clearBits(*runner >> idx64, available);
        rv |= clearBits(*++runner, left) << available;
    }
    *idx += dataLen;
    return rv;
}

// returns `true` if `x` is <2^(nbits-1)-1 or >-2^(nbits-1)
static bool bitRange(int64_t x, u_int8_t nbits) {
    // x+1 since int2bin and bin2int are [-32, 31]
	return -((1LL << (nbits - 1)) - 1) <= (x + 1) && 
           (1LL << (nbits - 1))      >= (x + 1);
}

static bool isSpaceAvailable(CompressedChunk *chunk, u_int8_t size) {
  u_int64_t available = (chunk->size * 8) - chunk->idx;
  return (size <= available) ? true : false;
}

/***************************** APPEND ********************************/
static ChunkResult appendTS(CompressedChunk *chunk, u_int64_t timestamp) {
  union64bits doubleDelta;

  int64_t curDelta = timestamp - chunk->prevTimestamp;
  assert(curDelta >= 0);
  doubleDelta.i = curDelta - chunk->prevTimestampDelta;

  if (doubleDelta.i == 0) {
    CHECKSPACE(chunk, 1 + 1); // CHECKSPACE adds 1 as minimum for double space
    appendBits(chunk->data, &chunk->idx, 0x00, 1);
  } else if (bitRange(doubleDelta.i, 7)) {
    CHECKSPACE(chunk, 2 + 7 + 1);
    appendBits(chunk->data, &chunk->idx, 0x01, 2);
    appendBits(chunk->data, &chunk->idx, int2bin(doubleDelta.i, 7), 7);
  } else if (bitRange(doubleDelta.i, 10)) {
    CHECKSPACE(chunk, 3 + 10 + 1);
    appendBits(chunk->data, &chunk->idx, 0x03, 3);
    appendBits(chunk->data, &chunk->idx, int2bin(doubleDelta.i, 10), 10);
  } else if (bitRange(doubleDelta.i, 13)) {
    CHECKSPACE(chunk, 4 + 13 + 1);
    appendBits(chunk->data, &chunk->idx, 0x07, 4);
    appendBits(chunk->data, &chunk->idx, int2bin(doubleDelta.i, 13),13);
  } else if (bitRange(doubleDelta.i, 16)) {
    CHECKSPACE(chunk, 5 + 16 + 1);
    appendBits(chunk->data, &chunk->idx, 0x0f, 5);
    appendBits(chunk->data, &chunk->idx, int2bin(doubleDelta.i, 16),16);
  } else {
    CHECKSPACE(chunk, 5 + 64 + 1);
    appendBits(chunk->data, &chunk->idx, 0x1f, 5);
    appendBits(chunk->data, &chunk->idx, doubleDelta.u, 64);
  }
  chunk->prevTimestampDelta = curDelta;
  chunk->prevTimestamp = timestamp;
  return CR_OK;
}

static ChunkResult appendV(CompressedChunk *chunk, double value) {
  union64bits val;
  val.d = value;
  u_int64_t xorWithPrevious = val.u ^ chunk->prevValue.u;

  // CHECKSPACE already checked for 1 extra bit availability in appendTS
  if (xorWithPrevious == 0) {
    appendBits(chunk->data, &chunk->idx, 0, 1);
    return CR_OK;   
  }
  appendBits(chunk->data, &chunk->idx, 1, 1);

  u_int64_t leading  = LeadingZeros64(xorWithPrevious);
  u_int64_t trailing = TrailingZeros64(xorWithPrevious);
  
  // Prevent over flow of DOUBLE_LEADING
  if (leading > 31) { 
    leading = 31; 
  }

  int8_t prevLeading = chunk->prevLeading;
  int8_t prevTrailing = chunk->prevTrailing;    
  int8_t blockSize = _64BIT - leading - trailing;
  u_int32_t expectedSize = DOUBLE_LEADING + DOUBLE_BLOCK_SIZE + blockSize;
  u_int32_t prevBlockInfoSize = _64BIT - prevLeading - prevTrailing;

  if (leading >= chunk->prevLeading && 
      trailing >= chunk->prevTrailing &&
      expectedSize > prevBlockInfoSize
    ) {
    CHECKSPACE(chunk, prevBlockInfoSize + 1);
    appendBits(chunk->data, &chunk->idx, 0, 1);    
    appendBits(chunk->data, &chunk->idx, xorWithPrevious >> prevTrailing, prevBlockInfoSize);
  } else {
    CHECKSPACE(chunk, expectedSize + 1);
    appendBits(chunk->data, &chunk->idx, 1, 1);
    appendBits(chunk->data, &chunk->idx, leading, DOUBLE_LEADING);
    appendBits(chunk->data, &chunk->idx, blockSize - DOUBLE_BLOCK_ADJUST, DOUBLE_BLOCK_SIZE);            
    appendBits(chunk->data, &chunk->idx, xorWithPrevious >> trailing, blockSize);  
    chunk->prevLeading = leading;
    chunk->prevTrailing = trailing;
  }
  chunk->prevValue.d = value;
  return CR_OK;
}

ChunkResult CChunk_Append(CompressedChunk *chunk, u_int64_t timestamp, double value) {
  if (chunk->count == 0) {
    chunk->baseValue.d   = chunk->prevValue.d = value;
    chunk->baseTimestamp = chunk->prevTimestamp = timestamp;
    chunk->prevTimestampDelta = 0;
  } else {
    if (appendTS(chunk, timestamp) != CR_OK) return CR_END;
    if (appendV (chunk, value) != CR_OK) return CR_END;
  }
  chunk->count++;
  return CR_OK;
}

/********************************** READ *********************************/
static u_int64_t readTS(CChunk_Iterator *iter) {
  int64_t dd = 0;
  u_int64_t *idx = &iter->idx;
  u_int64_t *runner = iter->chunk->data;

  // Read stored double delta value
  if (isBitOff(runner, (*idx)++)) {
      dd = 0;
  } else if (isBitOff(runner, (*idx)++)) {
      dd = bin2int(readBits(runner, idx, 7), 7);
  } else if (isBitOff(runner, (*idx)++)) {
      dd = bin2int(readBits(runner, idx, 10), 10);
  } else if (isBitOff(runner, (*idx)++)) {
      dd = bin2int(readBits(runner, idx, 13), 13);
  } else if (isBitOff(runner, (*idx)++)) {
      dd = bin2int(readBits(runner, idx, 16), 16);
  } else {
      dd = bin2int(readBits(runner, idx, 64), 64);
  }

  // Update iterator
  iter->prevDelta += dd;
  return iter->prevTS = iter->prevTS + iter->prevDelta;  
}

static double readV(CChunk_Iterator *iter) {
  u_int64_t xorValue;
  union64bits rv;

  // Check if value was changed
  if (isBitOff(iter->chunk->data, iter->idx++)) {
    return iter->prevValue.d;
  }

  // Check if previous block information was used
  bool usePreviousBlockInfo = isBitOff(iter->chunk->data, iter->idx++);
  if (usePreviousBlockInfo) {
    u_int8_t prevBlockInfo = _64BIT - iter->prevLeading - iter->prevTrailing;
    xorValue = readBits(iter->chunk->data, &iter->idx, prevBlockInfo);
    xorValue <<= iter->prevTrailing;
  } else {
    u_int64_t leading   = readBits(iter->chunk->data, &iter->idx, DOUBLE_LEADING);
    u_int64_t blocksize = readBits(iter->chunk->data, &iter->idx, DOUBLE_BLOCK_SIZE) + DOUBLE_BLOCK_ADJUST;
    u_int64_t trailing  = _64BIT - leading - blocksize;
    xorValue = readBits(iter->chunk->data, &iter->idx, blocksize) << trailing;
    iter->prevLeading = leading;
    iter->prevTrailing = trailing;    
  }

  rv.u = xorValue ^ iter->prevValue.u;
  return iter->prevValue.d = rv.d;
}

ChunkResult CChunk_ReadNext(CChunk_Iterator *iter, u_int64_t *timestamp, double *value) {
  *timestamp = iter->prevTS;
  *value     = iter->prevValue.d;

  if (iter->count < iter->chunk->count) {
    iter->prevTS      = readTS(iter);
    iter->prevValue.d = readV (iter);
    iter->count++;
    return CR_OK;
  } else {
    return CR_END;
  }
}