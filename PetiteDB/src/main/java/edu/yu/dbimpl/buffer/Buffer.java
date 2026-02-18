package edu.yu.dbimpl.buffer;

import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.PageBase;
import edu.yu.dbimpl.log.LogMgrBase;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

/** Specifies the public API for the Buffer implementation by requiring all
 * Buffer implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * An single buffer in BufferMgr's buffer pool: it encapsulates a Page, adding
 * additional information such as whether the Page's contents have been
 * modified since it was fetched from disk.
 *
 * Design note: Buffers should be manipulated through the BufferMgr API to the
 * greatest extent possible.  For example, difficult to see how a client can
 * usefully create a Buffer through its constructor since it will not be
 * associated with a disk block.
 *
 * @author Avraham Leff
 */
public class Buffer extends BufferBase{
    private final FileMgrBase fileMgr;
    private final LogMgrBase logMgr;
    private BlockIdBase block = null;
    private PageBase page = null;
    private AtomicInteger pin = new AtomicInteger(0);
    private int modified = 0;
    private int lsn = -1;
    private final Set<Integer> modifyingTxs = Collections.synchronizedSet(new HashSet<>());
    private final Object lock = new Object();


    public Buffer(FileMgrBase fileMgr, LogMgrBase logMgr) {
        super(fileMgr, logMgr);
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;
    }

    /** Returns the Page encapsulated by this Buffer instance.
     *
     * @return the encapsulated Page.
     */
    @Override
    public PageBase contents() {
        return page;
    }

    /** Returns a reference to the disk block allocated to the buffer.
     *
     * @return a reference to a disk block
     */
    @Override
    public BlockIdBase block() {
        return block;
    }

    /** Sets the buffer's "modified" bit.  This method enables performance
     * enhancements since buffers that have not been modified by the client need
     * not be flushed to disk (since the disk block represents the current
     * state).
     *
     * @param txnum identifies the transaction that modified the Buffer.
     * @param lsn The LSN of the most recent log record, set to a negative number
     * to indicate that the client didn't generate a log record when modifying
     * the Buffer.
     * @throws IllegalArgumentException if txnum is negative
     */
    @Override
    public void setModified(int txnum, int lsn) {
        if(txnum < 0){
            throw new IllegalArgumentException("Negative txn");
        }
        synchronized(lock){
            modifyingTxs.add(txnum);
            this.lsn = lsn;
            this.modified = 1;
        }
    }

    /** Return true iff the buffer is currently pinned, defined as "has a pin
     * count that is greater than 0".
     *
     * @return true iff the buffer is pinned.
     */
    @Override
    public boolean isPinned() {
        return pin.get() > 0;
    }

    protected boolean hasTXN(int txnum){
        synchronized(lock){
            return modifyingTxs.contains(txnum);
        }
    }

    protected void removeTXN(int txn){
        modifyingTxs.remove(txn);
    }

    protected boolean isModified(){
        synchronized(lock){
            return this.modified > 0;
        }
    }

    protected  void setBlock(BlockIdBase block) {
        this.block = block;
    }
    protected  void setPage(PageBase page) {
        this.page = page;
    }
    protected void pin(){
        pin.incrementAndGet();
    }
    protected void unpin(){
        pin.decrementAndGet();
    }

    protected void flush(){
        synchronized(lock){
            this.modified = 0;
            this.modifyingTxs.clear();
            this.lsn = -1;
        }
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if(((Buffer)obj).block().equals(block) && ((Buffer) obj).page.equals(page)){
            return true;
        }else {
            return false;
        }
    }
}
