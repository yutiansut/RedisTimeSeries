#include "buffer.h"
#include <stdio.h>
#include <stdlib.h>


#define BITBUFFER_TRACE(buffer,msg,args...) BITBUFFER_TRACE_LEVEL(buffer,msg,1,args)

#define BITBUFFER_TRACE_LEVEL(buffer,msg,level, args...)     if (buffer->trace >= level) {\
    printf("Buffer(%s): ", __func__);\
    printf(msg, args);\
    printf("\n> bit_index=%d, byte_index=%d\n", buffer->bit_index, buffer->byte_index);\
    }

BitBuffer * BitBuffer_new(size_t size) {
    return BitBuffer_newWithData(size, malloc(size));
}

BitBuffer * BitBuffer_newWithData(size_t size, char *data) {
    BitBuffer *buffer;
    buffer = (BitBuffer *)malloc(sizeof(BitBuffer));
    BitBuffer_init(buffer, size, data);

    return buffer;
}

void BitBuffer_init(BitBuffer *buffer, size_t size, char *data) {
    buffer->bit_index = 0;
    buffer->byte_index = 0;
    buffer->size = size;
    buffer->data = data;
    buffer->trace = 0;
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
//    bits_len -= buffer->bit_index;
//    buffer->bit_index = 0;
    BITBUFFER_TRACE(buffer, " SeekBack %d", bits_len)
    while (bits_len > 0) {
        BITBUFFER_TRACE_LEVEL(buffer, "left %d", 2, bits_len)
        if (buffer->bit_index > 0) {
            if (buffer->bit_index < bits_len) {
                bits_len -= buffer->bit_index;
                buffer->bit_index = 0;
            } else {
                buffer->bit_index -= bits_len;
                bits_len = 0;
            }
        } else {
            if (bits_len >= CHAR_BIT) {
                buffer->byte_index -= 1;
                bits_len -= CHAR_BIT;
            } else {
                buffer->bit_index = CHAR_BIT - bits_len;
                buffer->byte_index -= 1;
                bits_len = 0;
            }
        }
    }
//    buffer->bit_index -= bits_len % CHAR_BIT;
//    buffer->byte_index -= bits_len / CHAR_BIT;
    BITBUFFER_TRACE(buffer, "seeked %d", bits_len)
    return BITBUFFER_OK;
}

int BitBuffer_writeBit (struct BitBuffer *buffer, char bit)
{
    // TODO: check for space
    BITBUFFER_TRACE(buffer, "write 1 bit: %d", bit)

    if (buffer->bit_index == CHAR_BIT) {
        buffer->byte_index++;
        buffer->bit_index = 0;
    }

    if (bit) {
        buffer->data[buffer->byte_index] |= 1 << (CHAR_BIT - buffer->bit_index - 1);
    } else {
        buffer->data[buffer->byte_index] &= ~(1 << (CHAR_BIT - buffer->bit_index - 1));
    }

    buffer->bit_index++;
    return BITBUFFER_OK;
}


int BitBuffer_write (struct BitBuffer *buffer, char bits, int length)
{
    BITBUFFER_TRACE(buffer, "write length: %d; value:0x%x", length, bits)
  unsigned int i, shift_pos, current_bit;

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
        BITBUFFER_TRACE_LEVEL(buffer, "current_bit %d", 2, current_bit)

        if (current_bit) {
            buffer->data[buffer->byte_index] |= (1 << shift_pos);
        } else {
            buffer->data[buffer->byte_index] &= ~(1 << shift_pos);
        }
        
        (buffer->bit_index)++;
    }
  return BITBUFFER_OK;
}


void BitBuffer_write_bits_le (struct BitBuffer *buffer, u_int64_t bits, int length)
{
    BITBUFFER_TRACE(buffer, "bits: 0x%llx; length:%d", bits, length)
    u_int8_t *data_ptr = (u_int8_t *)&bits;
    int left = length;
    int bytes = length / CHAR_BIT;
    data_ptr += bytes - 1;
    int extra_bits = length % CHAR_BIT;

    if (extra_bits) {
        BitBuffer_write(buffer, *data_ptr, extra_bits);
        data_ptr--;
        left -= extra_bits;
    }

    while (left > 0) {
        BitBuffer_write(buffer, *data_ptr, CHAR_BIT);
        left -= CHAR_BIT;
        data_ptr--;
    }
}

void BitBuffer_write_bits_be (struct BitBuffer *buffer, u_int8_t *bits, int length)
{
    BITBUFFER_TRACE(buffer, "length:%d", length)
    u_int8_t *data_ptr = bits;
    int left = length;
    while (left > 0) {
        BITBUFFER_TRACE_LEVEL(buffer,"left %d, data: %x", 2, left, *data_ptr)
        unsigned int write_bits = 0;
        if (left >= CHAR_BIT) {
            BitBuffer_write(buffer, *data_ptr, CHAR_BIT);
            write_bits = CHAR_BIT;
        } else {
            write_bits = left % CHAR_BIT;
            BitBuffer_write(buffer, *data_ptr, write_bits);
        }
        left -= write_bits;
        data_ptr++;
    }
}


u_int64_t
BitBuffer_read (struct BitBuffer *buffer, int size)
{
    BITBUFFER_TRACE(buffer, "read length: %d;", size)
  unsigned int i, shift_pos;
  u_int64_t bin = 0;
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
        BITBUFFER_TRACE_LEVEL(buffer, "current_bit %d", 2, current_bit)
      bin |= current_bit;
      (buffer->bit_index)++;
    }

    BITBUFFER_TRACE(buffer, "value: 0x%llx", bin)

    return bin;
}

/*
 * varint section taken from Redisearch
 * from: https://github.com/RedisLabsModules/RediSearch/blob/master/src/varint.c
 */

size_t varintEncode(uint64_t value, uint8_t *vbuf) {
    unsigned pos = sizeof(varintBuf) - 1;
    vbuf[pos] = value & 127;
    while (value >>= 7) {
        vbuf[--pos] = 128 | (--value & 127);
    }
    return pos;
}

uint64_t ReadVarint(BitBuffer *b) {

    unsigned char c = BitBuffer_read(b, CHAR_BIT);

    uint64_t val = c & 127;
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

    for (size_t i=0; i < len; i++) {
        if (BitBuffer_write(buf, *VARINT_BUF(varint, pos + i), CHAR_BIT) != BITBUFFER_OK) {
            break;
        }
        written++;

    }
    return written;
}

// End of varint section