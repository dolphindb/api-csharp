using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace dolphindb.compression
{
    class DeltaBitOutput
    {
        public static int DEFAULT_ALLOCATION = 256;

        private long[] longArray;
        private int position = 0;

        protected long lB;
        protected int bitsLeft = sizeof(long) * 8;

        public  static long[] MASK_ARRAY;
        public  static long[] BIT_SET_MASK;

        // Java does not allow creating 64 bit masks with (1L << 64) - 1; (end result is 0)
        static DeltaBitOutput(){
        MASK_ARRAY = new long[64];
        long mask = 1;
        long value = 0;
        for (int i = 0; i<MASK_ARRAY.Length; i++) {
            value = value | mask;
            mask = mask << 1;

            MASK_ARRAY[i] = value;
        }

        BIT_SET_MASK = new long[64];
        for(int i = 0; i<BIT_SET_MASK.Length; i++) {
            BIT_SET_MASK[i] = (1L << i);
        }
    }


    /**
     * Creates a new ByteBufferBitOutput with a default allocated size of 4096 bytes.
     */
    public DeltaBitOutput():this(DEFAULT_ALLOCATION){ }

/**
 * Give an initialSize different than DEFAULT_ALLOCATIONS. Recommended to use values which are dividable by 4096.
 *
 * @param initialSize New initialsize to use
 */
        public DeltaBitOutput(int initialSize)
        {
            longArray = new long[initialSize];
            lB = longArray[position];
        }

        protected void expandAllocation()
        {
            long[] largerArray = new long[longArray.Length * 2];
            longArray.CopyTo(largerArray, 0);
            longArray = largerArray;
        }

        private void checkAndFlipByte()
        {
            // Wish I could avoid this check in most cases...
            if (bitsLeft == 0)
            {
                flipWord();
            }
        }

        protected int capacityLeft()
        {
            return longArray.Length - position;
        }

        protected void flipWord()
        {
            if (capacityLeft() <= 2)
            { // We want to have always at least 2 longs available
                expandAllocation();
            }
            flipWordWithoutExpandCheck();
        }

        protected void flipWordWithoutExpandCheck()
        {
            longArray[position] = lB;
            ++position;
            resetInternalWord();
        }

        private void resetInternalWord()
        {
            lB = 0;
            bitsLeft = sizeof(long) * 8;
        }

/**
 * Sets the next bit (or not) and moves the bit pointer.
 */
        public void writeBit()
        {
            lB |= BIT_SET_MASK[bitsLeft - 1];
            bitsLeft--;
            checkAndFlipByte();
        }

        public void skipBit()
        {
            bitsLeft--;
            checkAndFlipByte();
        }

/**
 * Writes the given long to the stream using bits amount of meaningful bits. This command does not
 * check input values, so if they're larger than what can fit the bits (you should check this before writing),
 * expect some weird results.
 *
 * @param value Value to be written to the stream
 * @param bits How many bits are stored to the stream
 */
        public void writeBits(long value, int bits)
        {
            if (bits <= bitsLeft)
            {
                int lastBitPosition = bitsLeft - bits;
                lB |= (value << lastBitPosition) & MASK_ARRAY[bitsLeft - 1];
                bitsLeft -= bits;
                checkAndFlipByte(); // We could be at 0 bits left because of the <= condition .. would it be faster with
                                    // the other one?
            }
            else
            {
                value &= MASK_ARRAY[bits - 1];
                int firstBitPosition = bits - bitsLeft;
                ulong ulValue = (ulong)value;
                lB |= (long)(ulValue >> firstBitPosition);
                bits -= bitsLeft;
                flipWord();
                lB |= value << (64 - bits);
                bitsLeft -= bits;
                checkAndFlipByte();
            }
        }

/**
 * Causes the currently handled word to be written to the stream
 */
        public void flush()
        {
            flipWord();
        }

        public long[] getLongArray()
        {
            long[] copy = new long[position +1];
            Array.Copy(longArray, copy, position + 1);
            copy[copy.Length - 1] = lB;
            return copy;
        }
    }
}
