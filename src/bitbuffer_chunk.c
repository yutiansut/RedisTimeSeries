#include "chunk.h"
#include "buffer.h"
#include <string.h>
#include "rmutil/alloc.h"

/*
 * varint section taken from Redisearch
 * from: https://github.com/RedisLabsModules/RediSearch/blob/master/src/varint.c
 */

#define VARINT_BUF(buf, pos) ((buf) + pos)
#define VARINT_LEN(pos) (sizeof(varintBuf) - (pos))

typedef uint8_t varintBuf[24];

static inline size_t varintEncode(uint32_t value, uint8_t *vbuf) {
    unsigned pos = sizeof(varintBuf) - 1;
    vbuf[pos] = value & 127;
    while (value >>= 7) {
        vbuf[--pos] = 128 | (--value & 127);
    }
    return pos;
}

static inline uint64_t ReadVarint(BitBuffer *b) {

    unsigned char c = BitBuffer_read(b, CHAR_BIT);

    uint32_t val = c & 127;
    while (c >> 7) {
        ++val;
        c = BitBuffer_read(b, CHAR_BIT);
        val = (val << 7) | (c & 127);
    }

    return val;
}

size_t WriteVarintBuffer(varintBuf varint, size_t pos, BitBuffer *buf) {
//    varintBuf varint;
//    size_t pos = varintEncode(value, varint);
    size_t len = VARINT_LEN(pos);
    size_t written = 0;
    if (BitBuffer_hasSpaceFor(buf, len * CHAR_BIT) != BITBUFFER_OK) {
        return 0;
    }

    for (int i=0; i < len; i++) {
        if (BitBuffer_write(buf, *VARINT_BUF(varint, pos + i), CHAR_BIT) != BITBUFFER_OK) {
            break;
        }
        written++;

    }
    return written;
}

/** end **/

typedef struct CompressedChunkData {
    timestamp_t last_timestamp;
    timestamp_t last_time_delta;
    double last_data;
    int last_write_size_bits;
    BitBuffer *buffer;
} CompressedChunkData;

Chunk * NewChunk(size_t sampleCount)
{
    Chunk *newChunk = (Chunk *)malloc(sizeof(Chunk));
    newChunk->num_samples = 0;
    newChunk->max_samples = sampleCount;
    newChunk->nextChunk = NULL;
    newChunk->samples = malloc(sizeof(CompressedChunkData));
    ((CompressedChunkData*)newChunk->samples)->buffer = BitBuffer_new(sampleCount);
    return newChunk;
}

static CompressedChunkData *ChunkGetCompressedChunkData(Chunk *chunk) {
    return (CompressedChunkData*)chunk->samples;
}

void FreeChunk(Chunk *chunk) {
    BitBuffer_free(ChunkGetCompressedChunkData(chunk)->buffer);
    free(chunk);
}

int IsChunkFull(Chunk *chunk) {
    return ChunkGetCompressedChunkData(chunk)->buffer->byte_index >= ChunkGetCompressedChunkData(chunk)->buffer->size;
}

int ChunkNumOfSample(Chunk *chunk) {
    return chunk->num_samples;
}

timestamp_t ChunkGetLastTimestamp(Chunk *chunk) {
    if (chunk->num_samples == 0) {
        return -1;
    }
    return ChunkGetCompressedChunkData(chunk)->last_timestamp;
}
timestamp_t ChunkGetFirstTimestamp(Chunk *chunk) {
    if (chunk->num_samples == 0) {
        return -1;
    }
    return chunk->base_timestamp;
}

int ChunkAddSample(Chunk *chunk, Sample sample) {
    if (IsChunkFull(chunk)){
        return 0;
    }

    if (ChunkNumOfSample(chunk) == 0) {
        // initialize base_timestamp
        chunk->base_timestamp = sample.timestamp;
    }

    timestamp_t timestamp;
    if (chunk->num_samples == 0) {
        timestamp = 0;
    } else {
        timestamp = sample.timestamp - chunk->base_timestamp;
    }

    varintBuf varint;
    size_t pos = varintEncode(timestamp, varint);
    CompressedChunkData *compressedChunkData = ChunkGetCompressedChunkData(chunk);
    BitBuffer *buff = compressedChunkData->buffer;

    if (compressedChunkData->last_timestamp == sample.timestamp) {
        BitBuffer_SeekBack(buff, compressedChunkData->last_write_size_bits);
    }

    int timestamp_size = VARINT_LEN(pos) * CHAR_BIT;;
    int data_size = sizeof(double) * CHAR_BIT;
    int total_size = timestamp_size + data_size;

    if (BitBuffer_hasSpaceFor(buff, total_size) != BITBUFFER_OK) {
//        int back_seek = timestamp_size * CHAR_BIT;
//        BitBuffer_SeekBack(buff, back_seek);
        return 0;
    }

    if (WriteVarintBuffer(varint, pos, buff) == 0) {
        return 0;
    }

    char *data = &sample.data;
    for (int i=0; i < sizeof(double); i ++) {
        BitBuffer_write(buff, *(data + i), CHAR_BIT);
    }
    chunk->num_samples++;
    compressedChunkData->last_timestamp = sample.timestamp;
    compressedChunkData->last_data = sample.data;
    compressedChunkData->last_write_size_bits = total_size;

    return 1;
}

ChunkIterator NewChunkIterator(Chunk* chunk) {
    BitBuffer *writeOnly = ChunkGetCompressedChunkData(chunk)->buffer;
    BitBuffer *buffer = BitBuffer_newWithData(writeOnly->size, writeOnly->data);
    return (ChunkIterator){.chunk = chunk, .currentIndex = 0, .data = buffer};
}

int ChunkIteratorGetNext(ChunkIterator *iter, Sample* sample) {
    if (iter->currentIndex < iter->chunk->num_samples) {
        iter->currentIndex++;
        BitBuffer *buffer = iter->data;
        uint64_t timestamp = ReadVarint(buffer) + + iter->chunk->base_timestamp;
        double data = 0;
        char *byte = &data;
        for (int i=0; i < sizeof(double) / sizeof(char) ; i ++) {
            int res = BitBuffer_read(buffer, CHAR_BIT);
            memcpy(byte, &res, sizeof(char));
            byte++;
        }
        sample->timestamp = timestamp ;
        sample->data = (double)data;
        return 1;
    } else {
        return 0;
    }
}

size_t Chunk_MemorySize(Chunk *chunk) {
    CompressedChunkData *compressedChunkData = ChunkGetCompressedChunkData(chunk);
    return sizeof(Chunk) + sizeof(struct CompressedChunkData) + compressedChunkData->buffer->size;
}