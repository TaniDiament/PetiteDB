package edu.yu.dbimpl.tx;

import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.tx.concurrency.ConcurrencyMgr;
import edu.yu.dbimpl.tx.concurrency.ConcurrencyMgrBase;
import edu.yu.dbimpl.tx.concurrency.LockTable;
import edu.yu.dbimpl.tx.recovery.RecoveryMgr;
import edu.yu.dbimpl.tx.recovery.RecoveryMgrBase;

import java.util.concurrent.atomic.AtomicInteger;


public class TxMgr extends TxMgrBase{
    private final FileMgrBase fileMgr;
    private final BufferMgrBase bufferMgr;
    private final LogMgrBase logMgr;
    private static final AtomicInteger txCount =  new AtomicInteger(1);
    private final long maxWaitTime;
    private final LockTable locktable;

    public static void resetCount(){
        txCount.set(1);
    }
    /** Creates a TxMgr with the specified max waiting time, also supplying
     * managers for the lower-level DBMS modules.  The client is responsible for
     * ensuring that the file, log, and buffer managers are fully instantiated
     * before invoking this constructor.
     *
     * On DBMS startup, the TxMgr is responsible for TRANSPARENTLY rolling back
     * transactions that (per log record state) were neither committed or rolled
     * back when the DBMS last closed down.  Per lecture and textbook, the
     * persisted state associated with such transactions must be rolled back to
     * their pre-tx state: i.e., no state modifications made by these txs may be
     * visible when the DBMS finished the recovery process and is "open for
     * business".
     *
     * If the TxMgr implementation is in ANY way dependent on knowledge of
     * whether or not the DBMS is in "startup" mode, it MUST access the
     * DBConfiguration singleton to determine if implementation specific actions
     * must be taken to (re)initialize the necessary state.
     *
     * @param fileMgr file manager singleton
     * @param logMgr log manager singleton
     * @param bufferMgr buffer manager singleton
     * @param maxWaitTimeInMillis maximum amount of time that a tx will wait to
     * acquire a lock (whether slock or xlock) before the database throws a
     * LockAbortException.  Must be greater than 0, and is specified in ms.
     * @see edu.yu.dbimpl.tx.concurrency.ConcurrencyMgrBase#sLock
     * @see edu.yu.dbimpl.tx.concurrency.ConcurrencyMgrBase#xLock
     */
    public TxMgr(FileMgrBase fm, LogMgrBase lm, BufferMgrBase bm, long maxWaitTimeInMillis) {
        super(fm, lm, bm, maxWaitTimeInMillis);
        fileMgr = fm;
        bufferMgr = bm;
        logMgr = lm;
        maxWaitTime = maxWaitTimeInMillis;
        locktable = LockTable.INSTANCE;
        locktable.setMaxWaitTimeInMillis(maxWaitTimeInMillis);
        locktable.resetAllLockState();
        boolean isNewDatabase = DBConfiguration.INSTANCE.isDBStartup();
        if(!isNewDatabase){
            ConcurrencyMgr cm = new ConcurrencyMgr(this, 0);
            RecoveryMgrBase recoveryMgr = new RecoveryMgr(0, logMgr, bufferMgr);
            Tx tx = new Tx(0, fileMgr.blockSize(), fileMgr, logMgr, bufferMgr, cm, recoveryMgr);
            tx.recover();

        }
    }

    /** Returns singleton lock table
     */
    public LockTable getLockTable() {
        return locktable;
    }

    /** Returns the maxWaitTime value
     */
    @Override
    public long getMaxWaitTimeInMillis() {
        return maxWaitTime;
    }
    /** Returns a new transaction instance.
     */
    @Override
    public TxBase newTx() {
        int num = txCount.getAndIncrement();
        ConcurrencyMgrBase concurrencyMgr = new ConcurrencyMgr(this, num);
        RecoveryMgrBase recoveryMgr = new RecoveryMgr(num, logMgr, bufferMgr);
        TxBase tx = new Tx(num, fileMgr.blockSize(), fileMgr, logMgr, bufferMgr, concurrencyMgr, recoveryMgr);
        return tx;
    }
    /** Resets global lock-related state to "initial" state.  The TxMgr is
     * conceptually a DBMS singleton (as are the other module managers) and is
     * associated with a single DBMS lock table.  Therefore, invoking this method
     * on ANY TxMgr instance in a given JVM will reset the lock related state for
     * ALL TxMgr instances.
     *
     * This method is needed to prevent errors in one test from cascading test to
     * subsequent tests: whatever locks and state that were held by the previous
     * tx are reset so that subsequent txs can start with a clean state.
     *
     * @protip you really want this method to be bug-free!
     */
    @Override
    public void resetAllLockState() {
        locktable.resetAllLockState();
    }
}
