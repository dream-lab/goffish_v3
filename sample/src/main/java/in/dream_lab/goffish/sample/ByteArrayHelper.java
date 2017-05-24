package in.dream_lab.goffish.sample;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;


/**
 * Maintains a byte buffer for reading and writing long and int values.
 *
 * @author Yogesh Simmhan
 *
 * @version 1.0
 * @see <a href="http://www.dream-lab.in/">DREAM:Lab</a>
 *
 *      Copyright 2014 DREAM:Lab, Indian Institute of Science, Bangalore
 *
 *      Licensed under the Apache License, Version 2.0 (the "License"); you may
 *      not use this file except in compliance with the License. You may obtain
 *      a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *      Unless required by applicable law or agreed to in writing, software
 *      distributed under the License is distributed on an "AS IS" BASIS,
 *      WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *      See the License for the specific language governing permissions and
 *      limitations under the License.
 */
public class ByteArrayHelper {

    /**
     * Allows writing of long, list of long and int into byte buffer
     *
     * @author simmhan
     *
     */
    public static class Writer {
        ByteArrayOutputStream buf;

        public Writer() {
            buf = new ByteArrayOutputStream(16 * 8);
        }

        /**
         * Set buffer's initial capacity
         *
         * @param size_
         */
        public Writer(int size_) {
            buf = new ByteArrayOutputStream(size_);
        }

        /**
         * Write a 1-byte
         *
         * @param first
         */
        Writer writeByte(byte b) {
            buf.write(b);
            return this;
        }


        /**
         * Write a 2-byte short, with first byte written being the LSB and second byte
         * written the MSB
         *
         * @param first
         */
        Writer writeShort(short first) {
            // byte 1 (LSB)
            byte b = (byte) (first & 0xFF);
            buf.write(b);
            first >>= 8;

            // byte 2
            b = (byte) (first & 0xFF);
            buf.write(b);

            return this;
        }

        /**
         * Write a 4-byte int, with first byte written being the LSB and last byte
         * written the MSB
         *
         * @param first
         */
        Writer writeInt(int first) {
            // byte 1 (LSB)
            byte b = (byte) (first & 0xFF);
            buf.write(b);
            first >>= 8;

            // byte 2
            b = (byte) (first & 0xFF);
            buf.write(b);
            first >>= 8;

            // byte 3
            b = (byte) (first & 0xFF);
            buf.write(b);
            first >>= 8;

            // byte 4
            b = (byte) (first & 0xFF);
            buf.write(b);

            return this;
        }

        /**
         * Write a 4-byte float. Converts to Integer bits usign Java builtin.
         *
         * @param first
         */
        Writer writeFloat(float first) {
            return writeInt(Float.floatToIntBits(first));
        }

        /**
         * Write an 8-byte long, with first byte written being the LSB and last byte
         * written the MSB
         *
         * @param first
         */
        Writer writeLong(long first) {
            // byte 1 (LSB)
            byte b = (byte) (first & 0xFF);
            buf.write(b);
            first >>= 8;

            // byte 2
            b = (byte) (first & 0xFF);
            buf.write(b);
            first >>= 8;

            // byte 3
            b = (byte) (first & 0xFF);
            buf.write(b);
            first >>= 8;

            // byte 4
            b = (byte) (first & 0xFF);
            buf.write(b);
            first >>= 8;

            // byte 5
            b = (byte) (first & 0xFF);
            buf.write(b);
            first >>= 8;

            // byte 6
            b = (byte) (first & 0xFF);
            buf.write(b);
            first >>= 8;

            // byte 7
            b = (byte) (first & 0xFF);
            buf.write(b);
            first >>= 8;

            // byte 8 (MSB)
            b = (byte) (first & 0xFF);
            buf.write(b);

            return this;
        }

        /**
         * Write a list of longs. Prefix it with an int of the list size, followed
         * by the long values.
         *
         * @param items
         */
        Writer writeLongs(long[] items) {
            writeInt(items.length);
            for (int i = 0; i < items.length; i++) {
                writeLong(items[i]);
            }

            return this;
        }

        /**
         * Write a list of longs. Prefix it with an int of the list size, followed
         * by the long values.
         *
         * @param items
         */
        Writer writeManyLongs(List<Long> items) {
            writeInt(items.size());
            for (Long item : items) {
                writeLong(item);
            }

            return this;
        }

        /**
         * Returns the number of bytes stored in the buffer so far
         *
         * @return
         */
        int size() {
            return buf.size();
        }

        /**
         * Returns the byte array of the bytes contents written to the buffer
         *
         * @return
         */
        byte[] getBytes() {
            return buf.toByteArray();
        }
    }

    /**
     * Reader for accessing bytes that have been written to a buffer in the form
     * of long, list of long and integer
     *
     * @author simmhan
     *
     */
    public static class Reader {
        ByteArrayInputStream buf;

        /**
         * Set the initial contents of the buffer
         *
         * @param allBytes_
         */
        public Reader(byte[] allBytes_) {
            buf = new ByteArrayInputStream(allBytes_);
        }

        /**
         * reads 8 bytes of long without checking if adequate bytes are available.
         *
         * @return long that if formed from next 8 bytes. First byte is LSB, last
         *         byte is MSB.
         */
        private long readLongUnchecked() {
            long val = 0;

            // byte 1 (LSB)
            val |= buf.read();

            // byte 2
            val |= (((long) buf.read()) << 1 * 8);

            // byte 3
            val |= (((long) buf.read()) << 2 * 8);

            // byte 4
            val |= (((long) buf.read()) << 3 * 8);

            // byte 5
            val |= (((long) buf.read()) << 4 * 8);

            // byte 6
            val |= (((long) buf.read()) << 5 * 8);

            // byte 7
            val |= (((long) buf.read()) << 6 * 8);

            // byte 8 (MSB)
            val |= (((long) buf.read()) << 7 * 8);

            return val;

        }

        /**
         * reads 4 bytes of integer without checking if adequate bytes are
         * available.
         *
         * @return int that if formed from next 4 bytes. First byte is LSB, last
         *         byte is MSB.
         */
        private int readIntUnchecked() {

            int val = 0;

            // byte 1
            val |= buf.read();

            // byte 2
            val |= (((int) buf.read()) << 1 * 8);

            // byte 3
            val |= (((int) buf.read()) << 2 * 8);

            // byte 4
            val |= (((int) buf.read()) << 3 * 8);

            return val;
        }


        /**
         * reads 2 bytes of short without checking if adequate bytes are
         * available.
         *
         * @return short that is formed from next 2 bytes. First byte is LSB, second
         *         byte is MSB.
         */
        private short readShortUnchecked() {

            short val = 0;

            // byte 1
            val |= buf.read();

            // byte 2
            val |= (((short) buf.read()) << 1 * 8);

            return val;
        }


        /**
         * reads 8 bytes of long after checking if adequate bytes are available.
         *
         * @return long that if formed from next 8 bytes. First byte is LSB, last
         *         byte is MSB.
         * @throws ArrayIndexOutOfBoundsException
         *           if 8 bytes not available in buffer
         */
        public long readLong() {
            if (buf.available() < 8) throw new ArrayIndexOutOfBoundsException(
                "Available bytes not enough to read 8 bytes of long, " + buf.available());

            return readLongUnchecked();
        }


        /**
         * reads 4 bytes of integer after checking if adequate bytes are available.
         *
         * @return integer that if formed from next 4 bytes. First byte is LSB, last
         *         byte is MSB.
         * @throws ArrayIndexOutOfBoundsException
         *           if 4 bytes not available in buffer
         */
        public int readInt() {
            if (buf.available() < 4) throw new ArrayIndexOutOfBoundsException(
                "Available bytes not enough to read 4 bytes of integer, " + buf.available());

            return readIntUnchecked();
        }

        /**
         * reads 4 bytes of integer after checking if adequate bytes are available.
         *
         * @return integer that if formed from next 4 bytes. First byte is LSB, last
         *         byte is MSB.
         * @throws ArrayIndexOutOfBoundsException
         *           if 4 bytes not available in buffer
         */
        public short readShort() {
            if (buf.available() < 2) throw new ArrayIndexOutOfBoundsException(
                "Available bytes not enough to read 2 bytes of Short, " + buf.available());

            return readShortUnchecked();
        }

        /**
         * reads 1 byte from stream.
         *
         * @return 1 byte that  is read from stream
         * @throws ArrayIndexOutOfBoundsException
         *           if 1 byte not available in buffer
         */
        public byte readByte() {
            int val = buf.read();
            if (val == -1) throw new ArrayIndexOutOfBoundsException(
                "Available bytes not enough to read 1 byte, " + buf.available());

            return (byte)val;
        }

        /**
         * Write a 4-byte float. Converts to Integer bits usign Java builtin.
         *
         * @param first
         */
        public float readFloat() {
            int val = readInt();
            return Float.intBitsToFloat(val);
        }


        /**
         * reads array of long after checking if adequate bytes are available.
         * First reads an integer having count of number of longs, and then reads
         * the long values.
         *
         * @return long[] with size given by int formed from next 4 bytes, and
         *         values from long formed from each successive 8 bytes.
         * @throws ArrayIndexOutOfBoundsException
         *           if 4+8*count bytes not available in buffer
         */
        public long[] readLongs() {
            int count = readInt();

            if (buf.available() < 8 * count) throw new ArrayIndexOutOfBoundsException("Available bytes not enough to read "
                + count + "*8 = " + count * 8 + " bytes of longs, " + buf.available());

            long[] vals = new long[count];
            for (int i = 0; i < count; i++) {
                vals[i] = readLong();
            }
            return vals;
        }

        /**
         * Reads List of long after checking if adequate bytes are available.
         * First reads an integer having count of number of longs, and then reads
         * the long values.
         *
         * @return List<Long> with size given by int formed from next 4 bytes, and
         *         values from long formed from each successive 8 bytes.
         * @throws ArrayIndexOutOfBoundsException
         *           if 4+8*count bytes not available in buffer
         */
        public List<Long> readLongList() {
            int count = readInt();

            if (buf.available() < 8 * count) throw new ArrayIndexOutOfBoundsException("Available bytes not enough to read "
                + count + "*8 = " + count * 8 + " bytes of longs, " + buf.available());

            List<Long> vals = new ArrayList<Long>(count);
            for (int i = 0; i < count; i++) {
                vals.add(readLong());
            }
            return vals;
        }


        /**
         * Number of bytes available in the buffer
         *
         * @return
         */
        public int available() {
            return buf.available();
        }
    }
}
