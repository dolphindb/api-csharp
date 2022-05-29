using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.compression
{
    class DeltaBitInput
    {
        public static long[] MASK_ARRAY;
        public static long[] BIT_SET_MASK;
        static DeltaBitInput()
        {
            MASK_ARRAY = new long[64];
            long mask = 1;
            long value = 0;
            for (int i = 0; i < MASK_ARRAY.Length; i++)
            {
                value = value | mask;
                mask = mask << 1;

                MASK_ARRAY[i] = value;
            }
            BIT_SET_MASK = new long[64];
            for (int i = 0; i < BIT_SET_MASK.Length; i++)
            {
                BIT_SET_MASK[i] = (1L << i);
            }
        }
        private  long[] longArray;
        private long lB;
        private int position = 0;
        private int bitsLeft = 0;

        public DeltaBitInput(long[] array)
        {
            this.longArray = array;
            flipByte();
        }

        public bool readBit()
        {
            bool bit = (lB & BIT_SET_MASK[bitsLeft - 1]) != 0;
            bitsLeft--;
            checkAndFlipByte();
            return bit;
        }

        public long readBits(int bits)
        {
            long value = 0L;
            for (int i = 0; i < bits; ++i)
            {
                value <<= 1;
                value |= ((lB & BIT_SET_MASK[bitsLeft - 1]) == 0) ? 0 : 1;
                bitsLeft--;
                checkAndFlipByte();
            }
            return value;
        }

        private void flipByte()
        {
            lB = longArray[position++];
            bitsLeft = sizeof(long) * 8;
        }

        private void checkAndFlipByte()
        {
            if (bitsLeft == 0)
            {
                flipByte();
            }
        }

        public int getInt()
        {
            return (int)getLong(32);
        }

        public long getLong(int bits)
        {
            long value;
            if (bits <= bitsLeft)
            {
                // We can read from this word only
                // Shift to correct position and take only n least significant bits
                ulong ullB = (ulong)lB;
                value = (long)(ullB >> (bitsLeft - bits)) & MASK_ARRAY[bits - 1];
                bitsLeft -= bits; // We ate n bits from it
                checkAndFlipByte();
            }
            else
            {
                // This word and next one, no more (max bits is 64)
                value = lB & MASK_ARRAY[bitsLeft - 1]; // Read what's left first
                bits -= bitsLeft;
                flipByte(); // We need the next one
                value <<= bits; // Give n bits of space to value
                ulong ullB = (ulong)lB;
                value |= (long)(ullB >> (bitsLeft - bits));
                bitsLeft -= bits;
                checkAndFlipByte();
            }
            return value;
        }


        public int nextClearBit(int maxBits)
        {
            int val = 0x00;

            for (int i = 0; i < maxBits; i++)
            {
                val <<= 1;
                bool bit = readBit();

                if (bit)
                {
                    val |= 0x01;
                }
                else
                {
                    break;
                }
            }
            return val;
        }

        public void rollBack(int bits)
        {
            if (sizeof(long) * 8 - bitsLeft >= bits)
            {
                bitsLeft += bits;
            }
            else
            {
                position--;
                bitsLeft = bits - sizeof(long) * 8 + bitsLeft;
            }
        }


        public int getPosition()
        {
            return position;
        }
    }
}
