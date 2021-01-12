/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package tech.mlsql.tool;

import sun.misc.Unsafe;

import java.nio.charset.Charset;

/**
 * 所有列都表示为为8byte
 * 我们限制z-ordering最大支持1024byte, 这意味着一个z-ordering 索引最多1024/8=128个字段。
 */
public class ZOrderingBytesUtil {
    static final Unsafe theUnsafe;
    public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    static {
        theUnsafe = UnsafeAccess.theUnsafe;

        // sanity check - this should never fail
        if (theUnsafe.arrayIndexScale(byte[].class) != 1) {
            throw new AssertionError();
        }
    }

    public static byte[] toBytes(int val) {
        byte[] b = new byte[4];
        for (int i = 3; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    public static byte[] toBytes(long val) {
        long temp = val;
        // 还原原码
//        if(val <0){
//            temp = (~(val -1))^(1L<<63);
//        }
        byte[] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) temp;
            temp >>>= 8;
        }
        b[0] = (byte) temp;
        return b;
    }


    //考虑负数，如果是负数，还原成原码表示，然后直接将第一位翻转，最后padding 成8字节
    // 正数，第一位翻转，然后Padding成8字节
    public static byte[] intTo8Byte(int a) {
        int temp = a;
        if (a < 0) {
            temp = (~(a - 1))^ (1 << 31);
        }
        temp = temp ^ (1 << 31);
        return paddingTo8Byte(toBytes(temp));
    }

    //考虑负数，如果是负数，还原成原码表示，然后直接将第一位翻转，最后padding 成8字节
    // 正数，第一位翻转，然后Padding成8字节
    public static byte[] longTo8Byte(long a) {
        long temp = a;
        if (a < 0) {
            temp = (~(a - 1))^ (1L << 63);
        }
        temp = temp ^ (1L << 63);
        return toBytes(temp);
    }

    public static byte[] toBytes(final double d) {
        // Encode it as a long
        return toBytes(Double.doubleToRawLongBits(d));
    }

    /**
     * 1.先得到byte[]表示。
     * 2.如果是正数，翻转第一个bit
     * 3.如果是负数，翻转所有的bit
     * 此时可以自然排序
     */
    public static byte[] doubleTo8Byte(double a) {
        
        byte[] temp = toBytes(a);
        if (a > 0) {
            temp[0] = (byte) (temp[0] ^ (1 << 7));
        }
        if (a < 0) {
            for (int i = 0; i < temp.length; i++) {
                temp[i] = (byte) ~temp[i];
            }
        }
        return temp;
    }

    public static byte[] utf8To8Byte(String a) {
        return paddingTo8Byte(a.getBytes(Charset.forName("utf-8")));
    }

    /**
     * buffer1,buffer2 必须是8字节，最后输出是16字节
     * buffer2的值在奇数位
     */
    public static byte[] interleave8Byte(byte[] buffer1, byte[] buffer2) {
        byte[] result = new byte[16];
        int j = 0;
        for (int i = 0; i < 8; i++) {
            byte[] temp = interleaveByte(buffer1[i], buffer2[i]);
            result[j] = temp[0];
            result[++j] = temp[1];
            j++;
        }
        return result;
    }


    //用b 的bpos bit 位，设置a的 apos bit位
    public static byte updatePos(byte a, int apos, byte b, int bpos) {
        //将bpos以外的都设置为0
        byte temp = (byte) (b & (1 << (7 - bpos)));
        //把temp bpos位置的值移动到apos

        //小于的话，左移
        if (apos < bpos) {
            temp = (byte) (temp << (bpos - apos));
        }
        //大于，右边移动
        if (apos > bpos) {
            temp = (byte) (temp >> (apos - bpos));
        }
        //把apos以外的都设置为0
        byte atemp = (byte) (a & (1 << (7 - apos)));
        if ((byte) (atemp ^ temp) == 0) {
            return a;
        }
        return (byte) (a ^ (1 << (7 - apos)));
    }

    //每个属性用8byte表示。但是属性数目不确定。
    public static byte[] interleaveMulti8Byte(byte[][] buffer) {
        int attributesNum = buffer.length;
        byte[] result = new byte[8 * attributesNum];

        //结果的第几个byte的第几个位置
        int resBitPos = 0;

        //每个属性总的bit数
        int totalBits = 64;
        //第一层循环移动bit
        for (int bitStep = 0; bitStep < totalBits; bitStep++) {
            //首先获取当前属性在第几个byte(总共八个)
            int tempBytePos = (int) Math.floor(bitStep / 8);
            //获取bitStep在对应属性的byte位的第几个位置
            int tempBitPos = bitStep % 8;

            //获取每个属性的bitStep位置的值
            for (int i = 0; i < attributesNum; i++) {
                int tempResBytePos = (int) Math.floor(resBitPos / 8);
                int tempResBitPos = resBitPos % 8;
                result[tempResBytePos] = updatePos(result[tempResBytePos], tempResBitPos, buffer[i][tempBytePos], tempBitPos);
                //结果bit要不断累加
                resBitPos++;
            }
        }


        return result;
    }

    /**
     * x在奇数位,y在偶数位
     */
    public static byte[] interleaveByte(byte x, byte y) {
        long z = ((y * 0x0101010101010101L & 0x8040201008040201L) *
                0x0102040810204081L >> 49) & 0x5555 |
                ((x * 0x0101010101010101L & 0x8040201008040201L) *
                        0x0102040810204081L >> 48) & 0xAAAA;
        byte[] eightBytes = toBytes(z);
        return new byte[]{eightBytes[6], eightBytes[7]};
    }

    public static String toString(final byte[] b) {
        StringBuilder sb = new StringBuilder();
        for (byte temp : b) {
            sb.append(String.format("%8s", Integer.toBinaryString(temp & 0xFF)).replace(' ', '0') + " ");
        }
        return sb.toString();
    }

    //来自HBase的代码
    public static int compareTo(byte[] buffer1, int offset1, int length1,
                                byte[] buffer2, int offset2, int length2) {

        // Short circuit equal case
        if (buffer1 == buffer2 &&
                offset1 == offset2 &&
                length1 == length2) {
            return 0;
        }
        final int stride = 8;
        final int minLength = Math.min(length1, length2);
        int strideLimit = minLength & ~(stride - 1);
        final long offset1Adj = offset1 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
        final long offset2Adj = offset2 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
        int i;

        /*
         * Compare 8 bytes at a time. Benchmarking on x86 shows a stride of 8 bytes is no slower
         * than 4 bytes even on 32-bit. On the other hand, it is substantially faster on 64-bit.
         */
        for (i = 0; i < strideLimit; i += stride) {
            long lw = theUnsafe.getLong(buffer1, offset1Adj + i);
            long rw = theUnsafe.getLong(buffer2, offset2Adj + i);
            if (lw != rw) {
                if (!UnsafeAccess.LITTLE_ENDIAN) {
                    return ((lw + Long.MIN_VALUE) < (rw + Long.MIN_VALUE)) ? -1 : 1;
                }

                /*
                 * We want to compare only the first index where left[index] != right[index]. This
                 * corresponds to the least significant nonzero byte in lw ^ rw, since lw and rw are
                 * little-endian. Long.numberOfTrailingZeros(diff) tells us the least significant
                 * nonzero bit, and zeroing out the first three bits of L.nTZ gives us the shift to get
                 * that least significant nonzero byte. This comparison logic is based on UnsignedBytes
                 * comparator from guava v21
                 */
                int n = Long.numberOfTrailingZeros(lw ^ rw) & ~0x7;
                return ((int) ((lw >>> n) & 0xFF)) - ((int) ((rw >>> n) & 0xFF));
            }
        }

        // The epilogue to cover the last (minLength % stride) elements.
        for (; i < minLength; i++) {
            int a = (buffer1[offset1 + i] & 0xFF);
            int b = (buffer2[offset2 + i] & 0xFF);
            if (a != b) {
                return a - b;
            }
        }
        return length1 - length2;
    }

    private static byte[] arrayConcat(byte[]... arrays) {
        int length = 0;
        for (byte[] array : arrays) {
            length += array.length;
        }
        byte[] result = new byte[length];
        int pos = 0;
        for (byte[] array : arrays) {
            System.arraycopy(array, 0, result, pos, array.length);
            pos += array.length;
        }
        return result;
    }

    private static byte[] paddingTo8Byte(byte[] a) {
        if (a.length == 8) return a;
        if (a.length > 8) {
            byte[] result = new byte[8];
            System.arraycopy(a, 0, result, 0, 8);
            return result;
        }
        int paddingSize = 8 - a.length;
        byte[] result = new byte[paddingSize];
        for (int i = 0; i < paddingSize; i++) {
            result[i] = 0;
        }
        return arrayConcat(result, a);
    }
}

