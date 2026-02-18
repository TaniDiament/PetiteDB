package edu.yu.dbimpl.file;

/** Specifies the public API for the BlockId implementation by requiring all
 * BlockId implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * A BlockId object represents the physical position of a block using its file
 * name and logical block number.  A BlockId is an immutable value class:
 * implementations should consider the implications of that design.
 *
 * @author Avraham Leff
 */

public class BlockId extends BlockIdBase{
    /**
     * Constructor
     *
     * @param filename
     * @param blknum   must be a non-negative integer
     * @throws IllegalArgumentException as appropriate
     */
    private final String filename;
    private final int blknum;
    public BlockId(String filename, int blknum) {
        super(filename, blknum);
        if (filename == null || filename.isBlank() || blknum < 0) {
            throw new IllegalArgumentException("Invalid filename or block number");
        }
        this.filename = filename;
        this.blknum = blknum;
    }

    @Override
    public String fileName() {
        return this.filename;
    }

    @Override
    public int number() {
        return this.blknum;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        BlockId other = (BlockId) obj;
        return this.blknum == other.blknum && this.filename.equals(other.filename);
    }

    @Override
    public int hashCode() {
        int result = filename.hashCode();
        result = (31 * result) + (blknum*91);
        return result;
    }
}
