/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#include "parse_policies.h"
#include "minunit.h"
#include "compaction.h"
#include "buffer.h"
#include "bitbuffer_chunk.h"
#include "rmutil/alloc.h"
#include "chunk.h"
#include <time.h>
#include <stdlib.h>

//srand(time(NULL));   // Initialization, should only be called once.
#define TEST_COUNT 2

MU_TEST(test_compress_decompress) {
    printf("start\n");
    Chunk *chunk = NewChunk(1024 * 4);
    Sample sample;
    CompressedChunkData* compressedChunkData = (CompressedChunkData*)chunk->samples;
    compressedChunkData->buffer->trace = 1;
    int data_size;
    double numbers[TEST_COUNT] = {0};
    for (int i=0; i<TEST_COUNT; i++) {
        numbers[i] = (float)rand()/(float)(RAND_MAX/ 125.2); //(rand() % 64) / ((rand() % 32) || 1.2);
        sample.data = numbers[i];
        sample.timestamp = 1556442295 + i;
        printf("sample %d\n", i);
        data_size = ChunkAddSample(chunk, sample);
        mu_assert_double_eq(1, data_size);
    }
    printf("===\n");
    ChunkIterator iter = NewChunkIterator(chunk);
    int i = 0;
    while (ChunkIteratorGetNext(&iter, &sample)) {
        printf("sample %d\n", i);
        printf("read %lld value %lf\n", sample.timestamp, sample.data);
        mu_assert_double_eq(numbers[i], sample.data);
        mu_assert_double_eq(i + 1556442295, sample.timestamp);
        i++;
    }
}

MU_TEST(test_compress_decompress2) {
    printf("\nstart\n");
    Chunk *chunk = NewChunk(1024);
    Sample sample;
    CompressedChunkData* compressedChunkData = (CompressedChunkData*)chunk->samples;
    compressedChunkData->buffer->trace = 1;
    int data_size;
    compressedChunkData->last_data = 0;
    for (int i=0; i<4; i++) {
        printf("iter: %d\n", i);
        sample.data = 1.5 + (double)i;
        sample.timestamp = 1556442295 + i;
        u_int64_t data = compressData(compressedChunkData,
                sample,
                chunk,
                &data_size);
        BitBuffer_write_bits_be(compressedChunkData->buffer,data, data_size);
        compressedChunkData->last_data = sample.data;
        chunk->num_samples++;
    }
    printf("===\n");
    ChunkIterator iter = NewChunkIterator(chunk);
    CompressedChunkData* readCompressedChunkData = iter.data;
    readCompressedChunkData->buffer->trace = 1;
    for (int i=0; i<4; i++) {
        printf("iter: %d\n", i);
        double value = readXorValue(i, readCompressedChunkData, readCompressedChunkData->buffer);
        printf("ut: value %lf\n", value);
        mu_assert_double_eq(i + 1.5, value);
    }
}


MU_TEST(test_buffer_random) {
    srand(time(0));

    int numbers[TEST_COUNT] = {0};
    BitBuffer *buffer = BitBuffer_new(1024 * 4);
    buffer->trace = 0;
    for (int i=0; i< TEST_COUNT; i++) {
        u_int8_t num = rand() % 31;
        BitBuffer_write(buffer, num << 3, 5);
        numbers[i] = num;
    }

    buffer->byte_index = 0;
    buffer->bit_index = 0;
    for (int i=0; i< TEST_COUNT; i++) {
        int num = BitBuffer_read(buffer, 5);
        mu_assert_int_eq(numbers[i], num);
    }
}

MU_TEST(test_buffer_write_bit) {
    BitBuffer *buffer = BitBuffer_new(1024);
//    buffer->trace = 0;
//    char numbers[100] = {0};
    union my64bits data; data.dbl = 11.0;
    BitBuffer_write_bits_le(buffer, data.raw, sizeof(double) * CHAR_BIT);
    BitBuffer_writeBit(buffer, 1);
    BitBuffer_writeBit(buffer, 1);
    printf("\n-----\n");
    BitBuffer_write_bits_be(buffer, 0x18d8, 20);
//    BitBuffer_writeBit(buffer, 1);
//    BitBuffer_writeBit(buffer, 1);
//    BitBuffer_writeBit(buffer, 0);
//    BitBuffer_writeBit(buffer, 1);

    buffer->byte_index = 0;
    buffer->bit_index = 0;

    mu_assert_int_eq(data.raw, BitBuffer_read(buffer, sizeof(double) * CHAR_BIT));
    mu_assert_int_eq(1, BitBuffer_read(buffer, 1));
    mu_assert_int_eq(1, BitBuffer_read(buffer, 1));
//    mu_assert_int_eq(12, BitBuffer_read(buffer, 5));
    mu_assert_int_eq(1, BitBuffer_read(buffer, 1));
    mu_assert_int_eq(1, BitBuffer_read(buffer, 1));
    mu_assert_int_eq(12, BitBuffer_read(buffer, 5));
    mu_assert_int_eq(3, BitBuffer_read(buffer, 6));
}

MU_TEST(test_buffer_write_bits) {
    BitBuffer *buffer = BitBuffer_new(1024);
    buffer->trace = 0;
//    const char *data = "12345";
//    u_int64_t data = 0xdeadbeef;
    u_int64_t data = 0x844424930131968;

//    printf("before %x\n", *(u_int64_t *)data);
    printf("before %lx\n", data);
//    BitBuffer_write_bits(buffer, *((u_int64_t *)data), 5 * CHAR_BIT);
    BitBuffer_write_bits_be(buffer, data, 15 * CHAR_BIT);

    buffer->byte_index = 0;
    buffer->bit_index = 0;
    printf("\n+++++++\n");
    for (int i =0; i< 1; i++) {
        u_int64_t result = BitBuffer_read(buffer, 15 * CHAR_BIT);
        printf("\nres: %s\n", &result);
        printf("after %lx\n", result);
    }
}


MU_TEST_SUITE(bitbuffer_suite) {
    MU_RUN_TEST(test_compress_decompress);
//    MU_RUN_TEST(test_compress_decompress2);
//    MU_RUN_TEST(test_buffer_write_bits);
//    MU_RUN_TEST(test_buffer_random);
//    MU_RUN_TEST(test_buffer_write_bit);
}
