package edu.yu.dbimpl.tx.concurrency;

import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.tx.TxMgr;
import edu.yu.dbimpl.tx.TxMgrBase;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

/** Specifies the public API for the ConcurrencyMgr implementation by requiring
 * all ConcurrencyMgr implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * In this design, every transaction is associated with its own concurrency
 * manager.  This concurrency manager design assumes a lock-based approach to
 * concurrency control: the concurrency manager keeps track of which locks the
 * transaction currently has, and interacts with the global lock table as
 * needed.
 *
 * Design note: the resetAllLockState() method implicitly assumes that the DBMS
 * is using a lock-based concurrency control implementation.  This restriction
 * should be cleaned up in subsequent iterations.
 *
 */
public class ConcurrencyMgr extends ConcurrencyMgrBase{
    private LockTable locktable;
    private Map<BlockIdBase, Integer> blockIdBases;
    private final int txNum;
    /**
     * Create a concurrency manager.
     *
     * @param txMgr
     *
     */
    public ConcurrencyMgr(TxMgrBase txMgr, int txNum) {
        super(txMgr);
        this.locktable = ((TxMgr)txMgr).getLockTable();
        blockIdBases = new HashMap<>();
        this.txNum = txNum;
    }
    /** Obtain an SLock on the block, if necessary.  The method will ask the lock
     * table for an SLock if the transaction currently has no locks on that
     * block.
     *
     * @param blk a reference to the disk block
     */
    @Override
    public void sLock(BlockIdBase blk) {
        if(blockIdBases.containsKey(blk)){
            return;
        }
        try {
            locktable.sLock(blk, txNum);
            blockIdBases.put(blk, 0);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    /** Obtain an XLock on the block, if necessary.  If the transaction does not
     * have an XLock on that block, then the method first gets an SLock on that
     * block (if necessary), and then upgrades it to an XLock.
     *
     * @param blk a reference to the disk block
     */
    @Override
    public void xLock(BlockIdBase blk) {
        if(blockIdBases.get(blk) != null &&  blockIdBases.get(blk) == 1){
            return;
        }
        try {
            locktable.xLock(blk, txNum);
            blockIdBases.put(blk, 1);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
    /** Release all locks held by the concurrency manager's tx by asking the lock
     * table to unlock each one.
     */
    @Override
    public void release() {
        locktable.unlock(new ArrayList<>(blockIdBases.keySet()), txNum);
        blockIdBases.clear();
    }
}
