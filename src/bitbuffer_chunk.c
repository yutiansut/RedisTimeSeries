#include "chunk.h"
#include "buffer.h"
#include "bitbuffer_chunk.h"
#include <string.h>
#include "rmutil/alloc.h"
#include "portable_endian.h"


// bit functions

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

static unsigned int Len64(u_int64_t x) {
    unsigned int n = 0;
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

unsigned int TrailingZeros64(u_int64_t x) {
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

unsigned int LeadingZeros64(u_int64_t x) {
    return 64 - Len64(x);
}

// End of bit functions

void CompressedChunkDataInit(CompressedChunkData *data) {
    data->last.timestamp = 0;
    data->last.write_size_bits = 0;
    data->last.time_delta = 0;
    data->last.leading = 0xFF;
    data->last.trailing = 0xFF;
}

Chunk * NewChunk(size_t sampleCount)
{
    Chunk *newChunk = (Chunk *)malloc(sizeof(Chunk));
    newChunk->num_samples = 0;
    newChunk->base_timestamp = 0;
    newChunk->max_samples = sampleCount;
    newChunk->nextChunk = NULL;
    newChunk->samples = malloc(sizeof(CompressedChunkData));
    ((CompressedChunkData*)newChunk->samples)->buffer = BitBuffer_new(sampleCount);
    CompressedChunkDataInit((CompressedChunkData*)newChunk->samples);
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
    return ChunkGetCompressedChunkData(chunk)->last.timestamp;
}
timestamp_t ChunkGetFirstTimestamp(Chunk *chunk) {
    if (chunk->num_samples == 0) {
        return -1;
    }
    return chunk->base_timestamp;
}

void compressData(SampleData *last, Sample sample, int *data_size, u_int64_t* xordata, int *oLeading, int *oTrailing, u_int64_t *oXor) {
    *data_size = 0;
    volatile union my64bits current_data;
    current_data.dbl = sample.data;
    volatile u_int64_t xor = last->data.raw ^ current_data.raw;
    BitBuffer tempBuff;
    BitBuffer_init(&tempBuff, 64, (char *) xordata);
    tempBuff.trace = 0;
//    printf("\nstart compressData tracing, xor: %llx sample data: %lf last: 0x%lf\n", xor, current_data.dbl, last->last_data.dbl);

    if (xor == 0) {
        BitBuffer_writeBit(&tempBuff, 0);
        *data_size = 1;
        return;
    }
    else {
        BitBuffer_writeBit(&tempBuff, 1);
    }

    unsigned int leading = LeadingZeros64(xor);
    unsigned int trailing = TrailingZeros64(xor);

    if (leading >= 32) {
        leading = 31;
    }

    if (last->leading != 0xFF &&
        leading >= last->leading &&
        trailing >= last->trailing)
    {
        // sameTrailingLeadingBits = 0
        BitBuffer_writeBit(&tempBuff, 0);
        u_int8_t bits_len =  (64 - last->trailing) - last->leading;
        u_int32_t last_leading = htobe64(xor << last->leading);
        //printf("problem? compressedChunkData->last_trailing=%d compressedChunkData->last_leading=%d\n", compressedChunkData->last_trailing, compressedChunkData->last_leading);
        BitBuffer_write_bits_be(&tempBuff, &last_leading, bits_len);
//            printf("write: xor: 0x%llx trailing: %d leading: %d sigbits: %d\n", xor >> trailing, trailing, leading, 64 - leading - trailing);
    }
    else
    {
//        last->leading = leading;
//        last->trailing = trailing;
        // sameTrailingLeadingBits = 1
        BitBuffer_writeBit(&tempBuff, 1);
        //printf("writing leading %d to buff\n", leading<<3);
        BitBuffer_write(&tempBuff, leading << 3, 5);
        // Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
        // Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
        // So instead we write out a 0 and adjust it back to 64 on unpacking.
        u_int8_t sigbits = 64 - leading - trailing;
        BitBuffer_write(&tempBuff, sigbits << 2, 6);
        u_int32_t current_leading = htobe64(xor << leading);
        BitBuffer_write_bits_be(&tempBuff, &current_leading, sigbits);
//        printf("xor: %lX, last_data:%x current_data: %x\n", xor >> trailing, compressedChunkData->last_data, sample.data);
//            printf("write: xor: 0x%llx trailing: %d leading: %d sigbits: %d\n", xor >> trailing, trailing, leading, sigbits);
    }

    //printf("write size: byte:%d bit:%d\n", tempBuff.byte_index, tempBuff.bit_index);
    *data_size = tempBuff.byte_index * CHAR_BIT + tempBuff.bit_index;
    *oXor = xor;
    *oLeading = leading;
    *oTrailing = trailing;
//    printf("end compressData tracing %llx\n\n", be64toh(xordata));
//    last->xor = xor;
//        last->leading = leading;
//        last->trailing = trailing;
//    last->data.dbl = sample.data;
//        printf("data_size %d\n", *data_size);
}

int ChunkAddSample(Chunk *chunk, Sample sample) {
    if (IsChunkFull(chunk)){
        return 0;
    }

    CompressedChunkData *compressedChunkData = ChunkGetCompressedChunkData(chunk);
    BitBuffer *buff = compressedChunkData->buffer;
    buff->trace = 0;
    varintBuf varint;

    // Handle last sample override
    if (compressedChunkData->last.timestamp == sample.timestamp) {
        buff->trace = 0;
        // we do not decrement chunk->num_samples here because it happens in SeriesAddSample, we do want to move it into
        // the chunk abstraction
        BitBuffer_SeekBack(buff, compressedChunkData->last.write_size_bits);
        compressedChunkData->last = compressedChunkData->previous;
    }

    if (chunk->num_samples == 0) {
        // initialize base_timestamp
        chunk->base_timestamp = sample.timestamp;

        BitBuffer_writeBit(buff, 1);
        size_t pos = varintEncode(sample.timestamp, varint);
        if (WriteVarintBuffer(varint, pos, buff) == 0) {
            return 0;
        }
        union my64bits data = {0};
        data.dbl = sample.data;
        u_int64_t raw_data = htobe64(data.raw);
        BitBuffer_write_bits_be(buff, (u_int8_t *) &raw_data, sizeof(double) * CHAR_BIT);

        int timestamp_size = VARINT_LEN(pos) * CHAR_BIT + 1;
        compressedChunkData->last.write_size_bits = sizeof(double) * CHAR_BIT + timestamp_size;
        compressedChunkData->last.timestamp = sample.timestamp;
        compressedChunkData->last.data.dbl = sample.data;
        compressedChunkData->last.time_delta = 0;
        compressedChunkData->last.leading = 0xFF;
        compressedChunkData->last.trailing = 0xFF;

        chunk->num_samples++;

        return 1;
    }

    compressedChunkData->previous = compressedChunkData->last;

    // timestamp compression
    int write_delta = 0;
    int delta = sample.timestamp - compressedChunkData->last.timestamp;
    int double_delta = delta - compressedChunkData->last.time_delta;
    if (chunk->num_samples == 1) {
        write_delta = delta;
    } else {
        write_delta = double_delta;
    }

    size_t pos = varintEncode(write_delta, varint);
    int timestamp_size;
    if (write_delta == 0) {
        timestamp_size = 1;
    } else {
        timestamp_size = VARINT_LEN(pos) * CHAR_BIT + 1;
    }

    int data_size = 0;
    u_int64_t data[2] = {0,0};
    int trailing = compressedChunkData->last.trailing;
    int leading = compressedChunkData->last.leading;
    u_int64_t xor = compressedChunkData->last.xor;
//    if (compressedChunkData->last.data.dbl == sample.data) {
//        // no changes, write 1 bit of zero
//        data_size = 1;
//    } else {
        compressData(&compressedChunkData->last, sample, &data_size, (u_int64_t *) &data, &trailing, &leading, &xor);
//    }

//    printf("\n compressed  xor: %llx, bits: %d\n", data, data_size);
    int total_size = timestamp_size + data_size;

    if (BitBuffer_hasSpaceFor(buff, total_size + 1) != BITBUFFER_OK) {
        return 0;
    }

    // TODO: fix
    if (write_delta == 0) {
        BitBuffer_writeBit(buff, 0);
    } else {
        BitBuffer_writeBit(buff, 1);
        if (WriteVarintBuffer(varint, pos, buff) == 0) {
            return 0;
        }
    }
    BitBuffer_write_bits_be(buff, (u_int8_t *) &data, data_size);
    compressedChunkData->last.time_delta = delta;
    compressedChunkData->last.write_size_bits = total_size;
    compressedChunkData->last.timestamp = sample.timestamp;
    compressedChunkData->last.data.dbl = sample.data;
    compressedChunkData->last.xor = xor;
    compressedChunkData->last.trailing = trailing;
    compressedChunkData->last.leading = leading;

    chunk->num_samples++;

    return 1;
}

ChunkIterator NewChunkIterator(Chunk* chunk) {
    BitBuffer *writeOnly = ChunkGetCompressedChunkData(chunk)->buffer;
    BitBuffer *buffer = BitBuffer_newWithData(writeOnly->size, writeOnly->data);
    CompressedChunkData *data = malloc(sizeof(CompressedChunkData));
    bzero(data, sizeof(CompressedChunkData));
    data->buffer = buffer;
    buffer->trace = 0;
    CompressedChunkDataInit(data);
    return (ChunkIterator){.chunk = chunk, .currentIndex = 0, .data = data};
}

void ChunkIteratorClose(ChunkIterator *iter) {
    CompressedChunkData *compressedChunkData = iter->data;
    // We are freeing the metadata only, since this is a readonly buffer and we do not want to free the actual data
    free(compressedChunkData->buffer);
    free(compressedChunkData);
}

double readXorValue(int currentIndex, CompressedChunkData *compressedChunkData, BitBuffer *buffer) {
    union my64bits data;
    data.raw = 0;
    if (currentIndex == 0) {
//        char *byte = (char *) &data;
//        for (int i=0; i < sizeof(double) / sizeof(char) ; i ++) {
//            int res = BitBuffer_read(buffer, CHAR_BIT);
//            memcpy(byte, &res, sizeof(char));
//            byte++;
//        }
//        data.raw = be64toh(BitBuffer_read(buffer, sizeof(double) * CHAR_BIT));
        //printf("!!! read size: %lu !!", sizeof(double) * CHAR_BIT);
        data.raw = BitBuffer_read(buffer, sizeof(double) * CHAR_BIT);
    } else {
        int sameValueBit = BitBuffer_read(buffer, 1);
        if (sameValueBit == 0) {
            //printf("sameValueBit ==0\n");
            return compressedChunkData->last.data.dbl;
        }

        int sameTrailingLeadingBits = BitBuffer_read(buffer, 1);
        int leading = compressedChunkData->last.leading;
        unsigned int trailing = compressedChunkData->last.trailing;
        if (sameTrailingLeadingBits != 0) {
            leading = BitBuffer_read(buffer, 5);
            int sigBits = BitBuffer_read(buffer, 6);
            if (sigBits == 0) {
                sigBits = 64;
            }
            //printf("leading=%d, sigbits=%d\n", leading, sigBits);
            trailing = 64 - leading - sigBits;
        }
        uint32_t sigBitsLen = 64 - leading - trailing;
        u_int64_t sigBits = BitBuffer_read(buffer, sigBitsLen);
        data.raw = compressedChunkData->last.data.raw ^ (sigBits << trailing);
//        printf("last data: %lf\n", compressedChunkData->last_data.dbl);
//        printf("read: xor: 0x%llx trailing: %d, leading: %d, sigbits: %d, data: %lf, raw: 0x%llx\n", sigBits, trailing, leading, sigBitsLen, data.dbl, data.raw);
        compressedChunkData->last.leading = leading;
        compressedChunkData->last.trailing = trailing;
    }
    compressedChunkData->last.data = data;
    return data.dbl;
}

int ChunkIteratorGetNext(ChunkIterator *iter, Sample* sample) {
    CompressedChunkData *compressedChunkData = iter->data;
    BitBuffer *buffer = compressedChunkData->buffer;
//    buffer->trace = 2;
    int read_buffer = 0;
    uint64_t timestamp = 0;
    if (iter->currentIndex < iter->chunk->num_samples) {
        read_buffer = BitBuffer_read(buffer, 1);
        if (read_buffer == 0) {
            timestamp = compressedChunkData->last.timestamp + compressedChunkData->last.time_delta;
        } else {
            uint64_t varint = ReadVarint(buffer);
            if (iter->currentIndex == 0) {
                timestamp = varint;
            } else if (iter->currentIndex == 1) {
                timestamp = compressedChunkData->last.timestamp + varint;
                compressedChunkData->last.time_delta = varint;
            } else {
                compressedChunkData->last.time_delta = compressedChunkData->last.time_delta + varint;
                timestamp = compressedChunkData->last.timestamp + compressedChunkData->last.time_delta;
            }
            compressedChunkData->last.time_delta = varint;
        }
//        printf("> start xor value; iter->currentIndex: %d, %l\n", iter->currentIndex, buffer);
        double data = readXorValue(iter->currentIndex, compressedChunkData, buffer);
        compressedChunkData->last.timestamp = timestamp;
        compressedChunkData->last.data.dbl = data;
        sample->timestamp = timestamp;
        sample->data = data;
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