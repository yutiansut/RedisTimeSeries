/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef BITBUFFERCHUNK_H
#define BITBUFFERCHUNK_H
#include "chunk.h"
#include "buffer.h"

union my64bits
{
    u_int64_t raw;
    double    dbl;
};

typedef struct SampleData {
    timestamp_t timestamp;
    union my64bits data;
    timestamp_t time_delta;
    int write_size_bits;
    u_int64_t xor;
    int leading;
    int trailing;
} SampleData;

typedef struct CompressedChunkData {
    BitBuffer *buffer;
    SampleData last;
    SampleData previous;
} CompressedChunkData;

//u_int64_t compressData(CompressedChunkData *compressedChunkData, Sample sample, Chunk *chunk, int *data_size);
double readXorValue(int currentIndex, CompressedChunkData *compressedChunkData, BitBuffer *buffer);
Chunk * NewChunk(size_t sampleCount);
#endif