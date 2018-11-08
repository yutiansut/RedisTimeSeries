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

size_t WriteVarintBuffer(uint32_t value, BitBuffer *buf) {
  varintBuf varint;
  size_t pos = varintEncode(value, varint);
  size_t len = VARINT_LEN(pos);
//   memcpy(buf, VARINT_BUF(varint, pos), VARINT_LEN(pos));
  for (int i=0; i < len; i++)
    BitBuffer_write(buf, VARINT_BUF(varint, pos + i), CHAR_BIT);
  return len;
}

/** end **/

typedef struct CompressedChunkData {
    timestamp_t last_timestamp;
    timestamp_t last_time_delta;
    double last_data;
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

inline CompressedChunkData *ChunkGetCompressedChunkData(Chunk *chunk) {
    return (CompressedChunkData*)chunk->samples;
}

void FreeChunk(Chunk *chunk) {
    BitBuffer_free(ChunkGetCompressedChunkData(chunk)->buffer);
    free(chunk->samples);
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

    if (chunk->num_samples == 0) {
        WriteVarintBuffer(sample.timestamp, ChunkGetCompressedChunkData(chunk)->buffer);
    }
    chunk->num_samples++;

    return 1;
}

ChunkIterator NewChunkIterator(Chunk* chunk) {
    return (ChunkIterator){.chunk = chunk, .currentIndex = 0};
}

int ChunkIteratorGetNext(ChunkIterator *iter, Sample* sample) {
    if (iter->currentIndex < iter->chunk->num_samples) {
        iter->currentIndex++;
        Sample *internalSample = ChunkGetSample(iter->chunk, iter->currentIndex-1);
        memcpy(sample, internalSample, sizeof(Sample));
        return 1;
    } else {
        return 0;
    }
}