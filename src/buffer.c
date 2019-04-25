#include "buffer.h"
#include <stdio.h>
#include <stdlib.h>


BitBuffer * BitBuffer_new(size_t size) {
    return BitBuffer_newWithData(size, malloc(size));
}

BitBuffer * BitBuffer_newWithData(size_t size, char *data) {
    BitBuffer *buffer;
    buffer = (BitBuffer *)malloc(sizeof(BitBuffer));
    BitBuffer_init(buffer, size, data);

    return buffer;
}

BitBuffer * BitBuffer_init(BitBuffer *buffer, size_t size, char *data) {
    buffer->bit_index = 0;
    buffer->byte_index = 0;
    buffer->size = size;
    buffer->data = data;
}


void BitBuffer_free(BitBuffer *buffer) {
    free((void *)buffer->data);
    free(buffer);
}

int BitBuffer_hasSpaceFor(struct BitBuffer *buffer, int bits_len) {
    if ((buffer->size - buffer->byte_index) * CHAR_BIT < bits_len) {
        return BITBUFFER_ERROR;
    } else {
        return BITBUFFER_OK;
    }
}

int BitBuffer_SeekBack(struct BitBuffer *buffer, int bits_len) {
    bits_len -= buffer->bit_index;
    buffer->bit_index = 0;
    buffer->byte_index -= bits_len / CHAR_BIT;
    buffer->bit_index -= bits_len % CHAR_BIT;
    return BITBUFFER_OK;
}

int BitBuffer_writeBit (struct BitBuffer *buffer, char bit)
{
    printf("Buffer: write 1 bit\n");
    if (buffer->bit_index == CHAR_BIT) {
        buffer->byte_index++;
        buffer->bit_index = 0;
    }

    if (bit) {
        buffer->data[buffer->byte_index] |= 1 << (CHAR_BIT - buffer->bit_index - 1);
    }

    buffer->bit_index++;
}


int BitBuffer_write (struct BitBuffer *buffer, char bits, int length)
{
  printf("Buffer: write length: %d\n", length);
  int i, shift_pos, current_bit;

  if ((buffer->size - buffer->byte_index) * CHAR_BIT < length) {
      return BITBUFFER_OUT_OF_SPACE;
  }

  for (i = 0; i < length; i++)
    {
        if (buffer->bit_index == CHAR_BIT) {
            buffer->byte_index++;
            buffer->bit_index = 0;
        }

        shift_pos = CHAR_BIT - buffer->bit_index - 1;
        current_bit = (bits >> (CHAR_BIT - i - 1)) & 0x1;
        if (current_bit) {
            buffer->data[buffer->byte_index] |= (1 << shift_pos);
        } else {
            buffer->data[buffer->byte_index] &= ~(1 << shift_pos);
        }
        
        (buffer->bit_index)++;
    }
  return BITBUFFER_OK;
}

void BitBuffer_write_bits (struct BitBuffer *buffer, u_int64_t bits, int length)
{
    char *data_ptr = &bits;
    int left = length;
    while (left > 0) {
        int write_bits = 0;
        if (left >= CHAR_BIT) {
            write_bits = CHAR_BIT;
        } else {
            write_bits = left % CHAR_BIT;
        }
        BitBuffer_write(buffer, *data_ptr, write_bits);
        left -= write_bits;
        data_ptr++;
    }
}


int 
BitBuffer_read (struct BitBuffer *buffer, int size)
{
    printf("Buffer: read length: %d\n", size);
  int i, shift_pos;
  int bin = 0;
  unsigned char current_bit;

  for (i = 0; i < size; i++)
    {
      if (buffer->bit_index == CHAR_BIT)
        {
          buffer->bit_index = 0;
          buffer->byte_index++;
        }

      shift_pos = CHAR_BIT - buffer->bit_index - 1;
      bin <<= 1;
      current_bit = (buffer->data[buffer->byte_index] >> shift_pos ) & 0x1;
      bin |= current_bit;
      (buffer->bit_index)++;
    }

  return bin;
}
