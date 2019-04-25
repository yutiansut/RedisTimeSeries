
#include <limits.h>
#include <stdio.h>

#define BITBUFFER_OK 0
#define BITBUFFER_ERROR 1
#define BITBUFFER_OUT_OF_SPACE 2

typedef struct BitBuffer
{
  int bit_index;
  int byte_index;
  size_t size;
  char *data;
} BitBuffer;

BitBuffer * BitBuffer_new(size_t size);
BitBuffer * BitBuffer_newWithData(size_t size, char *data);
BitBuffer * BitBuffer_init(BitBuffer *buffer, size_t size, char *data);
int BitBuffer_hasSpaceFor(struct BitBuffer *buffer, int size);
int BitBuffer_SeekBack(struct BitBuffer *buffer, int bits_len);
int BitBuffer_write(struct BitBuffer *buffer, char bits, int size);
int BitBuffer_writeBit (struct BitBuffer *buffer, char bit);
void BitBuffer_write_bits(struct BitBuffer *buffer, u_int64_t bits, int length);
int BitBuffer_read(struct BitBuffer *buffer, int size);
void BitBuffer_free(BitBuffer *self);