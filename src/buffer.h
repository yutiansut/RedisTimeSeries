/*
* Copyright 2018-2019 Redis Labs Ltd. and Contributors
*
* This file is available under the Redis Labs Source Available License Agreement
*/
#ifndef BUFFER_H
#define BUFFER_H
#include <sys/types.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>

#define BITBUFFER_OK 0
#define BITBUFFER_ERROR 1
#define BITBUFFER_OUT_OF_SPACE 2

/*
 * varint section taken from Redisearch
 * from: https://github.com/RedisLabsModules/RediSearch/blob/master/src/varint.c
 */

#define VARINT_BUF(buf, pos) ((buf) + pos)
#define VARINT_LEN(pos) (sizeof(varintBuf) - (pos))

typedef u_int8_t varintBuf[24];

/* end section */

typedef struct BitBuffer
{
  int bit_index;
  int byte_index;
  size_t size;
  char *data;
  int trace;
} BitBuffer;

BitBuffer * BitBuffer_new(size_t size);
BitBuffer * BitBuffer_newWithData(size_t size, char *data);
void BitBuffer_init(BitBuffer *buffer, size_t size, char *data);
int BitBuffer_hasSpaceFor(struct BitBuffer *buffer, int size);
int BitBuffer_SeekBack(struct BitBuffer *buffer, int bits_len);
int BitBuffer_write(struct BitBuffer *buffer, char bits, int size);
int BitBuffer_writeBit (struct BitBuffer *buffer, char bit);
void BitBuffer_write_bits_be (struct BitBuffer *buffer, u_int8_t *bits, int length);
void BitBuffer_write_bits_le(struct BitBuffer *buffer, u_int64_t bits, int length);
u_int64_t BitBuffer_read(struct BitBuffer *buffer, int size);
void BitBuffer_free(BitBuffer *self);

size_t varintEncode(u_int64_t value, u_int8_t *vbuf);
inline u_int64_t ReadVarint(BitBuffer *b);
size_t WriteVarintBuffer(varintBuf varint, size_t pos, BitBuffer *buf);
#endif
