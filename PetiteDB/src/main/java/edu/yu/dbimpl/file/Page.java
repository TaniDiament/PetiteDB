package edu.yu.dbimpl.file;

import java.nio.ByteBuffer;

/**
 * A Page represents the main-memory region used to hold the contents of a
 * block, and thus is used by the FileMgr in tandem with a BlockId.
 * Conceptually, the "main-memory" region is a sequence of bytes, into which
 * clients can insert values, and from which it can read values.
 *
 * A Page can hold any of the following value TYPES: ints, doubles, booleans,
 * strings, and "blobs" (i.e., arbitrary arrays of bytes).  At the Page API
 * abstraction, a client can store a value at any offset of the page but
 * remains responsible for knowing what values have been stored where.  The
 * result of trying to retrieve a value from the wrong offset is undefined.
 *
 * I prefer to not constrain implementation!  That said: your implementation
 * must follow the design for storing values of a given type (fixed-length
 * versus variable-length) as discussed in lecture and MUST BE FOLLOWED in your
 * implementation (see the Javadoc on maxLength() below).  Your implementation
 * MUST use exactly Integer.BYTES to store an int, Double.BYTES to store a
 * double, and 1 byte to store a boolean.
 *
 * Design note: a value MAY NOT be persisted across blocks!  Implication: the
 * length of a value's persisted bytes (obviously) cannot exceed block size nor
 * can the "offset + length of a value's persisted bytes" exceed block size.
 * Any attempt to "set" or "get" a value whose semantics imply "setting" or
 * "getting" a value that exceeds a single block size MUST RESULT in an
 * IllegalArgumentException.
 *
 * @author Avraham Leff
 */
public class Page extends PageBase{
    private final ByteBuffer ourBuffer;
    private final int blocksize;

    /** Use this constructor when a Page's bytes are supplied implicitly by the
     * Page implementation.
     *
     * @param blocksize specifies the size of the blocks stored by a single Page:
     * must match the value supplied to the FileMgr constructor.
     *
     * Note: it's the client's responsibility to ensure that the blocksize
     * parameter is valid!
     */
    public Page(int blocksize) {
        super(blocksize);
        this.ourBuffer = ByteBuffer.allocateDirect(blocksize);
        this.blocksize = blocksize;
    }

    /** Use this constructor when a Page's bytes are explicitly supplied by the
     * client.  The client is responsible for ensuring that the byte[] has
     * sufficient space to store the data that are to be written into/read from
     * the Page instance.
     *
     * Space calculation should be done in terms of the raw space required by the
     * PetiteDB data-types, keeping in mind that Strings must be properly
     * encoded, per PageBase.maxLength, and per the rules for encoding
     * variable-length data types.
     * That is: the client should not need to do any offset translation when
     * retrieving fields from the Page getter methods.
     *
     * @param b a byte array containing the memory that will be read from/written
     * by the Page instance.  Having invoked this constructor, the client is
     * required to only set the byte array's state via calls to the PageBase API
     * (e.g., setString()), and similarly to only get state via calls to the
     * PageBase's API (e.g., getString()).  Changes made by the Page instance
     * will be reflected in the byte array parameter passed by the client.
     *
     * Note: it's the client's responsibility to ensure that the byte array,
     * after being serialized to disk, can be deserialized to fit into the
     * blocksize supplied to the FileMgr constructor.
     */
    public Page(byte[] b) {
        super(b);
        this.blocksize = b.length;
        this.ourBuffer = ByteBuffer.wrap(b);
    }

    /** For all of the getter methods
     *
     * @param offset the offset into the Page's main-memory from which the
     * initial byte is read, cannot be a negative value
     * @throws IllegalArgumentException per design note above.
     */

    /** For all of the setter methods
     *
     * @param offset the offset into the Page's main-memory at which the initial
     * byte is written, cannot be a negative value
     * 2nd parameter, the value to be written
     * @throws IllegalArgumentException per design note above.
     */
    @Override
    public int getInt(int offset) {
        if( offset < 0 || offset + Integer.BYTES > this.blocksize ) {
            throw new IllegalArgumentException("Invalid offset");
        }
        return ourBuffer.getInt(offset);
    }

    @Override
    public synchronized void setInt(int offset, int n) {
        if( offset < 0 || offset + Integer.BYTES > this.blocksize ) {
            throw new IllegalArgumentException("Invalid offset");
        }
        ourBuffer.putInt(offset, n);
    }

    @Override
    public double getDouble(int offset) {
        if( offset < 0 || offset + Double.BYTES > this.blocksize) {
            throw new IllegalArgumentException("Invalid offset");
        } else {
            return ourBuffer.getDouble(offset);
        }
    }

    @Override
    public synchronized void setDouble(int offset, double d) {
        if( offset < 0 || offset + Double.BYTES > this.blocksize) {
            throw new IllegalArgumentException("Invalid offset");
        } else {
            ourBuffer.putDouble(offset, d);
        }
    }

    @Override
    public boolean getBoolean(int offset) {
        if( offset < 0 || offset + 1 > this.blocksize) {
            throw new IllegalArgumentException("Invalid offset for " + offset);
        } else {
            return ourBuffer.get(offset) != 0;
        }
    }

    @Override
    public synchronized void setBoolean(int offset, boolean d) {
        if( offset < 0 || offset + 1 > this.blocksize) {
            throw new IllegalArgumentException("Invalid offset");
        } else {
            ourBuffer.put(offset, (byte) (d ? 1 : 0));
        }
    }

    @Override
    public byte[] getBytes(int offset) {
        if( offset < 0 || offset + Integer.BYTES > this.blocksize) {
            throw new IllegalArgumentException("Invalid offset");
        }
        int len = ourBuffer.getInt(offset);
        if(offset + Integer.BYTES + len > this.blocksize) {
            throw new IllegalArgumentException("Invalid offset");
        }
        byte[] result = new byte[len];
        for(int i = 0; i < len; i++) {
            result[i] = ourBuffer.get(offset + Integer.BYTES + i);
        }
        return result;
    }

    /** Writes the byte array to the specified offset in the block.
     *
     * @param offset the position to which the value will be written
     * @param b the byte array to write
     * @throws IllegalArgumentException if the byte array is too large to fit
     * into the block or if anything else goes wrong
     */
    @Override
    public synchronized void setBytes(int offset, byte[] b) {
        if( offset < 0 || offset + Integer.BYTES + b.length > this.blocksize) {
            throw new IllegalArgumentException("Invalid offset");
        }
        ourBuffer.putInt(offset, b.length);
        for(int i = 0; i < b.length; i++) {
            ourBuffer.put(offset + Integer.BYTES + i, b[i]);
        }
    }

    @Override
    public String getString(int offset) {
        if( offset < 0 || offset + Integer.BYTES > this.blocksize) {
            throw new IllegalArgumentException("Invalid offset");
        }
        int len = ourBuffer.getInt(offset);
        if(offset + Integer.BYTES + len > this.blocksize) {
            throw new IllegalArgumentException("Invalid offset");
        }
        byte[] strBytes = new byte[len];
        for(int i = 0; i < len; i++) {
            strBytes[i] = ourBuffer.get(offset + Integer.BYTES + i);
        }
        return new String(strBytes, CHARSET);
    }

    /** Writes the string to the specified offset in the block.
     *
     * @param offset the position to which the value will be written
     * @param s the string to write
     * @throws IllegalArgumentException if the string is too large to fit into
     * the block or if anything else goes wrong
     */
    @Override
    public synchronized void setString(int offset, String s) {
        if( offset < 0 || offset + Integer.BYTES + logicalLength(s) > this.blocksize) {
            throw new IllegalArgumentException("Invalid offset");
        }
        byte[] strBytes = s.getBytes(CHARSET);
        ourBuffer.putInt(offset, strBytes.length);
        for(int i = 0; i < strBytes.length; i++) {
            ourBuffer.put(offset + Integer.BYTES + i, strBytes[i]);
        }
    }

    protected ByteBuffer getBuffer() {
        return ourBuffer;
    }

    protected byte[] getArray() {
        return ourBuffer.array();
    }

    protected void setArray(byte[] b) {
        ourBuffer.clear();
        ourBuffer.put(b);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        Page other = (Page) obj;
        if (this.blocksize != other.blocksize) return false;
        for (int i = 0; i < this.blocksize; i++) {
            if (this.ourBuffer.get(i) != other.ourBuffer.get(i)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int result = Integer.hashCode(blocksize);
        for (int i = 0; i < blocksize; i++) {
            result = 31 * result + Byte.hashCode(ourBuffer.get(i));
        }
        return result;
    }
}
