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

union my64bits
{
    u_int64_t raw;
    double    dbl;
};

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

u_int8_t len8tab[256] = {
0x00, 0x01, 0x02, 0x02, 0x03, 0x03, 0x03, 0x03, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04, 0x04,
0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05, 0x05,
0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06,
0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06, 0x06,
0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07, 0x07,
0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08, 0x08,
};

const u_int64_t deBruijn64 = 0x03f79d71b4ca8b09;

char deBruijn64tab[] = {
0, 1, 56, 2, 57, 49, 28, 3, 61, 58, 42, 50, 38, 29, 17, 4,
62, 47, 59, 36, 45, 43, 51, 22, 53, 39, 33, 30, 24, 18, 12, 5,
63, 55, 48, 27, 60, 41, 37, 16, 46, 35, 44, 21, 52, 32, 23, 11,
54, 26, 40, 15, 34, 20, 31, 10, 25, 14, 19, 9, 13, 8, 7, 6,
};

static int Len64(u_int64_t x) {
    int n = 0;
    if (x >= (u_int64_t)1<<32) {
        x >>= 32;
        n = 32;
    }
    if (x >= 1<<16) {
        x >>= 16;
        n += 16;
    }
    if (x >= 1<<8) {
        x >>= 8;
        n += 8;
    }
    return n + len8tab[x];
}

int TrailingZeros64(u_int64_t x) {
    if (x == 0) {
        return 64;
    }
    // If popcount is fast, replace code below with return popcount(^x & (x - 1)).
    //
    // x & -x leaves only the right-most bit set in the word. Let k be the
    // index of that bit. Since only a single bit is set, the value is two
    // to the power of k. Multiplying by a power of two is equivalent to
    // left shifting, in this case by k bits. The de Bruijn (64 bit) constant
    // is such that all six bit, consecutive substrings are distinct.
    // Therefore, if we have a left shifted version of this constant we can
    // find by how many bits it was shifted by looking at which six bit
    // substring ended up at the top of the word.
    // (Knuth, volume 4, section 7.3.1)
    return deBruijn64tab[(x&-x)*deBruijn64>>(64-6)];
}

int LeadingZeros64(u_int64_t x) {
    return 64 - Len64(x);
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
    u_int8_t last_leading;
    u_int8_t last_trailing;
    int last_write_size_bits;
    BitBuffer *buffer;
} CompressedChunkData;

Chunk * NewChunk(size_t sampleCount)
{
    Chunk *newChunk = (Chunk *)malloc(sizeof(Chunk));
    newChunk->num_samples = 0;
    newChunk->base_timestamp = 0;
    newChunk->max_samples = sampleCount;
    newChunk->nextChunk = NULL;
    newChunk->samples = malloc(sizeof(CompressedChunkData));
    ((CompressedChunkData*)newChunk->samples)->buffer = BitBuffer_new(sampleCount);
    ((CompressedChunkData*)newChunk->samples)->last_leading = 0xFF;
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

    CompressedChunkData *compressedChunkData = ChunkGetCompressedChunkData(chunk);
    BitBuffer *buff = compressedChunkData->buffer;

    if (chunk->num_samples == 0) {
        // initialize base_timestamp
        chunk->base_timestamp = sample.timestamp;
    }

    // Handle last sample override
    if (compressedChunkData->last_timestamp == sample.timestamp) {
        BitBuffer_SeekBack(buff, compressedChunkData->last_write_size_bits);
        timestamp_t prev_timestamp = compressedChunkData->last_timestamp - compressedChunkData->last_time_delta;
        compressedChunkData->last_timestamp = prev_timestamp;
    }

    // timestamp compression
    timestamp_t timestamp;
    int delta = sample.timestamp - compressedChunkData->last_timestamp;
    int double_delta = delta - compressedChunkData->last_time_delta;
    if (chunk->num_samples == 0) {
        timestamp = sample.timestamp;
    } else if (chunk->num_samples == 1) {
        timestamp = delta;
        compressedChunkData->last_time_delta = delta;
    } else {
        timestamp = double_delta;
        compressedChunkData->last_time_delta = delta;
    }

    varintBuf varint;
    size_t pos = varintEncode(timestamp, varint);

    int timestamp_size;
    if (timestamp == 0) {
        timestamp_size = 1;
    } else {
        timestamp_size = VARINT_LEN(pos) * CHAR_BIT + 1;
    }

    // data compression
    int data_size = 0;
    u_int64_t *data = 0;
    u_int64_t xordata = 0;
    if (chunk->num_samples == 0) {
        data_size = sizeof(double) * CHAR_BIT;
        data = (u_int64_t *) &sample.data;
    } else {
        union my64bits current_data, last_data;
        current_data.dbl = sample.data;
        last_data.dbl = compressedChunkData->last_data;
        printf("pass");
        u_int64_t xor = last_data.raw ^ current_data.raw;
        BitBuffer tempBuff;
        BitBuffer_init(&tempBuff, 64, (char *) &xordata);

        if (xor == 0) {
            BitBuffer_writeBit(&tempBuff, 0);
            goto finish;
        }
        else {
            BitBuffer_writeBit(&tempBuff, 1);
        }

        u_int8_t leading = LeadingZeros64(xor);
        u_int8_t trailing = TrailingZeros64(xor);

        if (leading >= 32) {
            leading = 31;
        }

        if (compressedChunkData->last_leading != 0xFF &&
            leading >= compressedChunkData->last_leading &&
            trailing >= compressedChunkData->last_trailing)
        {
            // sameTrailingLeadingBits = 0
            BitBuffer_writeBit(&tempBuff, 0);
            int bits_len =  64 - compressedChunkData->last_trailing - compressedChunkData->last_leading;
            BitBuffer_write_bits(&tempBuff, xor >> compressedChunkData->last_trailing, bits_len);
            printf("write: xor: %llu trailing: %d sigbits: %d\n", xor >> compressedChunkData->last_trailing, compressedChunkData->last_trailing, bits_len);
        }
        else
        {
            compressedChunkData->last_leading = leading;
            compressedChunkData->last_trailing = trailing;
            // sameTrailingLeadingBits = 1
            BitBuffer_writeBit(&tempBuff, 1);
            BitBuffer_write(&tempBuff, leading, 5);
            // Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
            // Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
            // So instead we write out a 0 and adjust it back to 64 on unpacking.
            int sigbits = 64 - leading - trailing;
            BitBuffer_write(&tempBuff, sigbits, 6);
            BitBuffer_write(&tempBuff, xor >> trailing, sigbits);
            printf("write: xor: %llu trailing: %d sigbits: %d\n", xor >> trailing, trailing, sigbits);
        }
        finish:
        data_size = tempBuff.byte_index * CHAR_BIT + tempBuff.bit_index;
//        xordata >>= 64 - data_size;
        data = &xordata;
    }


    int total_size = timestamp_size + data_size;

    if (BitBuffer_hasSpaceFor(buff, total_size) != BITBUFFER_OK) {
        return 0;
    }

    if (timestamp == 0) {
        BitBuffer_writeBit(buff, 0);
    } else {
        BitBuffer_writeBit(buff, 1);
        if (WriteVarintBuffer(varint, pos, buff) == 0) {
            return 0;
        }
    }

//    printf("start\n");
//    char *data_ptr = data;
//    int left = data_size;
//    while (left > 0) {
//        int bits = 0;
//        if (left >= CHAR_BIT) {
//            bits = CHAR_BIT;
//        } else {
//            bits = left % CHAR_BIT;
//        }
////        printf("writing %d\n", data_ptr);
//        BitBuffer_write(buff, *data_ptr, bits);
//        left -= bits;
//        data_ptr++;
//    }
//    data_ptr = &data;
//    for (int i=0; i < sizeof(double); i ++) {
//        int x =1;
////        printf("writin2 %d\n", data_ptr + i);
//        BitBuffer_write(buff, *(data_ptr + i), CHAR_BIT);
//    }
    BitBuffer_write_bits(buff, *data, data_size);
    chunk->num_samples++;
    compressedChunkData->last_timestamp = sample.timestamp;
    compressedChunkData->last_data = sample.data;
    compressedChunkData->last_write_size_bits = total_size;
//    printf("end\n");

    return 1;
}

ChunkIterator NewChunkIterator(Chunk* chunk) {
    BitBuffer *writeOnly = ChunkGetCompressedChunkData(chunk)->buffer;
    BitBuffer *buffer = BitBuffer_newWithData(writeOnly->size, writeOnly->data);
    // TODO: free
    CompressedChunkData *data = malloc(sizeof(CompressedChunkData));
    data->buffer = buffer;
    data->last_data = 0;
    data->last_time_delta = 0;
    data->last_timestamp = 0;
    return (ChunkIterator){.chunk = chunk, .currentIndex = 0, .data = data};
}

double readXorValue(int currentIndex, CompressedChunkData *compressedChunkData, BitBuffer *buffer) {
    double data = 0;
    if (currentIndex == 0) {
        char *byte = (char *) &data;
        for (int i=0; i < sizeof(double) / sizeof(char) ; i ++) {
            int res = BitBuffer_read(buffer, CHAR_BIT);
            memcpy(byte, &res, sizeof(char));
            byte++;
        }
    } else {
        int sameValueBit = BitBuffer_read(buffer, 1);
        if (sameValueBit == 0) {
            return compressedChunkData->last_data;
        }

        int sameTrailingLeadingBits = BitBuffer_read(buffer, 1);
        int leading = compressedChunkData->last_leading;
        unsigned int trailing = compressedChunkData->last_trailing;
        if (sameTrailingLeadingBits != 0) {
            leading = BitBuffer_read(buffer, 5);
            int sigBits = BitBuffer_read(buffer, 6);
            if (sigBits == 0) {
                sigBits = 64;
            }
            trailing = 64 - leading - sigBits;
        }
        uint32_t sigBitsLen = 64 - leading - trailing;
        u_int64_t sigBits = BitBuffer_read(buffer, sigBitsLen);
        union my64bits last_data;
        last_data.dbl = compressedChunkData->last_data;
        data = last_data.raw ^ (sigBits << trailing);
        printf("read: xor: %llu trailing: %d sigbits: %d\n", sigBits << trailing, trailing, sigBitsLen);
        compressedChunkData->last_leading = leading;
        compressedChunkData->last_trailing = trailing;

    }
    compressedChunkData->last_data = data;
    return data;
}

int ChunkIteratorGetNext(ChunkIterator *iter, Sample* sample) {
    CompressedChunkData *compressedChunkData = iter->data;
    BitBuffer *buffer = compressedChunkData->buffer;
    int read_buffer = 0;
    uint64_t timestamp = 0;
    if (iter->currentIndex < iter->chunk->num_samples) {
        read_buffer = BitBuffer_read(buffer, 1);
        if (read_buffer == 0) {
            timestamp = compressedChunkData->last_timestamp + compressedChunkData->last_time_delta;
        } else {
            uint64_t varint = ReadVarint(buffer);
            if (iter->currentIndex == 0) {
                timestamp = varint;
            } else if (iter->currentIndex == 1) {
                timestamp = compressedChunkData->last_timestamp + varint;
                compressedChunkData->last_time_delta = varint;
            } else {
                compressedChunkData->last_time_delta = compressedChunkData->last_time_delta + varint;
                timestamp = compressedChunkData->last_timestamp + compressedChunkData->last_time_delta;
            }
            compressedChunkData->last_time_delta = varint;
        }
        double data = readXorValue(iter->currentIndex, compressedChunkData, buffer);
        compressedChunkData->last_timestamp = timestamp;
        compressedChunkData->last_data = data;
        sample->timestamp = timestamp;
        sample->data = (double)data;
        iter->currentIndex++;
        return 1;
    } else {
        return 0;
    }
}

size_t Chunk_MemorySize(Chunk *chunk) {
    CompressedChunkData *compressedChunkData = ChunkGetCompressedChunkData(chunk);
    return sizeof(Chunk) + sizeof(struct CompressedChunkData) + compressedChunkData->buffer->size;
}