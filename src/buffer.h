
#include <limits.h>
#include <stdio.h>

typedef struct BitBuffer
{
  int bit_index;
  int byte_index;
  size_t size;
  char *data;
} BitBuffer;

BitBuffer * BitBuffer_new(size_t size);
BitBuffer * BitBuffer_newWithData(size_t size, char *data);
void BitBuffer_write (struct BitBuffer *buffer, char bits, int size);
int BitBuffer_read (struct BitBuffer *buffer, int size);
void BitBuffer_free(BitBuffer *self);