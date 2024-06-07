using System;

namespace dolphindb.io
{

    public class Long2
    {
        public long high;
        public long low;

        public Long2(long high, long low)
        {
            this.high = high;
            this.low = low;
        }

        public bool isNull()
        {
            return high == 0 && low == 0;
        }

        public void setNull()
        {
            high = 0;
            low = 0;
        }

        public bool equals(object o)
        {
            if (!(o is Long2) || o == null)
			return false;
		else
			return high == ((Long2)o).high && low == ((Long2)o).low;
        }

        public int hashCode()
        {
            return (int)(high ^ low >> 32);
        }

        public int hashBucket(int buckets)
        {
            int m = 0x5bd1e995;
            int r = 24;
            int h = 16;

            int k1 = (int)(low & 4294967295L);
            int k2 = (int)((ulong)low >> 32);
            int k3 = (int)(high & 4294967295L);
            int k4 = (int)((ulong)high >> 32);

            k1 *= m;
            k1 ^= (int)((uint)k1 >> r);
            k1 *= m;

            k2 *= m;
            k2 ^= (int)((uint)k2 >> r);
            k2 *= m;

            k3 *= m;
            k3 ^= (int)((uint)k3 >> r);
            k3 *= m;

            k4 *= m;
            k4 ^= (int)((uint)k4 >> r);
            k4 *= m;

            // Mix 4 bytes at a time into the hash
            h *= m;
            h ^= k1;
            h *= m;
            h ^= k2;
            h *= m;
            h ^= k3;
            h *= m;
            h ^= k4;

            // Do a few final mixes of the hash to ensure the last few
            // bytes are well-incorporated.
            h ^= (int)((uint)h >> 13);
            h *= m;
            h ^= (int)((uint)h >> 15);

            if (h >= 0)
                return h % buckets;
            else
            {
                return (int)((4294967296L + h) % buckets);
            }
        }

        public override bool Equals(object o)
        {
            if (!(o is Long2) || o == null)
            {
                return false;
            }
            else
            {
                if (this.low != ((Long2)o).low || this.high != ((Long2)o).high)
                {
                    return false;
                }
                else
                    return true;
            }
        }

        public override int GetHashCode()
        {
            int hashCode = -1466551034;
            hashCode = hashCode * -1521134295 + high.GetHashCode();
            hashCode = hashCode * -1521134295 + low.GetHashCode();
            return hashCode;
        }
    }
}
