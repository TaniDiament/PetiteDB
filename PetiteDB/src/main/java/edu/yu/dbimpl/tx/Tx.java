package edu.yu.dbimpl.tx;

import edu.yu.dbimpl.buffer.BufferBase;
import edu.yu.dbimpl.buffer.BufferMgr;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.file.FileMgrBase;
import edu.yu.dbimpl.file.PageBase;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.tx.concurrency.ConcurrencyMgrBase;
import edu.yu.dbimpl.tx.recovery.LogRecord;
import edu.yu.dbimpl.tx.recovery.LogRecordBase;
import edu.yu.dbimpl.tx.recovery.RecoveryMgr;
import edu.yu.dbimpl.tx.recovery.RecoveryMgrBase;

import java.util.*;

/** Specifies the public API for the Transaction implementation by requiring
 * all Transaction implementations to implement this interface.
 *
 * Students MAY NOT modify this interface in any way.
 *
 * Transactions provide clients with the classic "ACID" properties, interfacing
 * with concurrency and recovery managers as necessary.
 *
 * Tx lifespan methods: the constructor begins a new transaction,
 * commit/rollback end that transaction.  "recover" rolls back ALL uncomitted
 * txs.
 *
 * Tx buffer management methods: all state written to, and read from, a buffer
 * is mediated through the appropriate setX/getX method.  All setX/getX methods
 * must acquire the appropriate locks before proceeding.  The method must throw
 * a LockAbortException if the DBMS is unable to acquire the lock within the
 * specified timeout period.  Before invoking these methods, clients must
 * invoke "pin" indicating that the tx should take control of the specified
 * block and the main-memory buffer that encapsulates that disk block.  The tx
 * maintains control until the client invokes "unpin".  The getX/setX methods
 * provide the hooks for Tx implementation to provide concurrency and recovery
 * function.
 *
 * Design note: a value MAY NOT be persisted across blocks!  Implication: the
 * length of a value's persisted bytes (obviously) cannot exceed block size nor
 * can the "offset + length of a value's persisted bytes" exceed block size.
 * Any attempt to "set" or "get" a value whose semantics imply "setting" or
 * "getting" a value that exceeds a single block size MUST RESULT in an
 * IllegalArgumentException.
 *
 * NOTE: Transaction instances are created by invoking TxMgrBase.newTx().
 *
 * @author Avraham Leff
 */
public class Tx implements TxBase{
    private Status status;
    private final int txNum;
    private final int blockSize;
    private final LogMgrBase logMgr;
    private final BufferMgrBase bufferMgr;
    private final ConcurrencyMgrBase concurrencyMgr;
    private final RecoveryMgrBase recoveryMgr;
    private final FileMgrBase  fileMgr;
    private Map<BlockIdBase, BufferBase> blockIdBases;
    private Map<BlockIdBase, Integer>  pinMap;

    public Tx(int number, int blockSize, FileMgrBase fileMgr, LogMgrBase logMgr, BufferMgrBase bufferMgr, ConcurrencyMgrBase concurrencyMgr, RecoveryMgrBase recoveryMgr) {
        this.status = Status.ACTIVE;
        this.txNum = number;
        this.fileMgr = fileMgr;
        this.logMgr = logMgr;
        this.bufferMgr = bufferMgr;
        this.concurrencyMgr = concurrencyMgr;
        this.recoveryMgr = recoveryMgr;
        this.blockSize = blockSize;
        this.blockIdBases = new HashMap<>();
        this.pinMap = new HashMap<>();
        ((RecoveryMgr)recoveryMgr).start();
    }

    /** A Tx enters the ACTIVE status as soon as it's instantiated.  It remains
     * in that state until the client invokes "commit()", at which point it
     * enters the COMMITTING state, and (if commit succeeds), enters the
     * COMMITTED state from which it never exits.  Alternatively, if the client
     * invokes "rollback()" on an ACTIVE tx, the tx enters the ROLLING_BACK
     * state, and (if rollback succeeds), enters the ROLLED_BACK state from which
     * it next exits.
     *
     * A client can only invoke "recover()" when tx status is ACTIVE.  The tx
     * then enters the RECOVERING state, transitioning to RECOVERED when the
     * recovery process is complete, and doesn't exit from that state.
     *
     * All "getter" and "setter" methods MUST throw IllegalStateException if the
     * tx isn't in the ACTIVE state.  For the rest of the API, see the per-method
     * Javadoc to see when IllegalStateException MUST be thrown if the tx is not
     * in the prerequisite state.
     */

    /** Returns the current status of the transaction.  May be invoked regardless
     * of the tx's status.
     *
     * NOTE: the value returned may be only a point in time value since the
     * transaction may change status immediately after the value is returned.
     *
     * @return the current status
     */
    @Override
    public Status getStatus() {//complete
        return status;
    }

    /** Every tx is associated with a unique non-negative integer id: this method
     * returns the tx's id.  May be invoked regardless of tx's status.
     *
     * Suggestion: if only to facilitate debugging, make these values increment
     * monotonically.
     *
     * @return the unique tx id
     */
    @Override
    public int txnum() {//complete
        return txNum;
    }

    /** Commits the current transaction: first flush all modified buffers (and
     * their log records); then write and flush a commit record to the log; then
     * release all locks, and unpin any pinned buffers.
     *
     * @throws IllegalStateException if tx isn't in the ACTIVE state.
     */
    @Override
    public void commit() {//complete
        if(status != Status.ACTIVE){
            throw new IllegalStateException("Status must be active to commit");
        }
        status = Status.COMMITTING;
        bufferMgr.flushAll(txNum);
        recoveryMgr.commit();
        concurrencyMgr.release();
        for (BufferBase buffer : blockIdBases.values()) {
            int times = pinMap.remove(buffer.block());
            for(int i = 0; i < times; i++){
                bufferMgr.unpin(buffer);
            }
        }
        blockIdBases.clear();
        status = Status.COMMITTED;
    }

    /** Roll the current transaction back: first undoes any modified values; then
     * flushes those buffers; then write and flush a rollback record to the log;
     * then releases all locks, and unpins any pinned buffers.
     *
     * @throws IllegalStateException if tx isn't in the ACTIVE state.
     */
    @Override
    public void rollback() {//complete
        if(status != Status.ACTIVE){
            throw new IllegalStateException("Status must be active to rollback");
        }
        status = Status.ROLLING_BACK;
        rollBackRecords();
        recoveryMgr.rollback();
        bufferMgr.flushAll(txNum);
        concurrencyMgr.release();
        for (BufferBase buffer : blockIdBases.values()) {
            int times = pinMap.remove(buffer.block());
            for(int i = 0; i < times; i++){
                bufferMgr.unpin(buffer);
            }
        }
        blockIdBases.clear();
        status = Status.ROLLED_BACK;
    }

    private void rollBackRecords(){//complete
        Iterator<byte[]> records = logMgr.iterator();
        TxBase tx = new Tx(this.txNum, this.blockSize, this.fileMgr, logMgr, bufferMgr, this.concurrencyMgr, recoveryMgr);
        while(records.hasNext()){
            LogRecord lr = LogRecord.getLogRecord(records.next());
            if(lr.op() == LogRecordBase.LogType.START.ordinal() && lr.txNumber() == this.txNum){
                break;
            }else if(lr.txNumber() == this.txNum){
                lr.undo(tx);
            }
        }
    }

    /** Flushes all modified buffers, then traverse the log, rolling back all
     * uncommitted transactions.  Finally, writes a quiescent "checkpoint record"
     * to the log.  This method MUST be called by the DBMS during system startup,
     * before processing user transactions so as to set the system to a
     * consistent state.  The method MAY be called by a client at any time, but
     * the method may then block until the system is deemed quiescent by the
     * DBMS.
     *
     * @throws IllegalStateException if tx isn't in the ACTIVE state.
     */
    @Override
    public void recover() {
        if(status != Status.ACTIVE){
            throw new IllegalStateException("Status must be active to recover");
        }
        this.status = Status.RECOVERING;
        //pin directly and readwrite directly
        ((BufferMgr)bufferMgr).recoverFlush();
        TxBase tx = new Tx(this.txNum, this.blockSize, this.fileMgr, logMgr, bufferMgr, concurrencyMgr, recoveryMgr);
        Iterator<byte[]> logs = logMgr.iterator();
        Set<Integer> commitRolled =  new HashSet<>();
        while(logs.hasNext()){
            byte[] log = logs.next();
            LogRecord lr = LogRecord.getLogRecord(log);
            if(lr.op() == LogRecordBase.LogType.COMMIT.ordinal() || lr.op() == LogRecordBase.LogType.ROLLBACK.ordinal()){
                commitRolled.add(lr.txNumber());
            }else if(lr.op() == LogRecordBase.LogType.CHECKPOINT.ordinal()){
                break;
            }else if(lr.op() == LogRecordBase.LogType.START.ordinal()){
                continue;
            }else{
                if(!commitRolled.contains(lr.txNumber())){
                    lr.undo(tx);
                }
            }
        }
        bufferMgr.flushAll(txNum);
        recoveryMgr.recover();
        concurrencyMgr.release();
        this.status = Status.RECOVERED;
    }

    /** Pins the specified block to a page buffer.  Going forward, the
     * transaction will manage the buffer on behalf of the client (until "unpin"
     * is invoked)
     *
     * @param blk a reference to the disk block
     * @throws IllegalArgumentException if BlockId is null.
     * @throws IllegalStateException if tx isn't in the ACTIVE state.
     * @see #unpin
     */
    @Override
    public synchronized void pin(BlockIdBase blk) {//complete
        if(status != Status.ACTIVE){
            throw new IllegalStateException("Status must be active to pin");
        }
        if(blk == null){
            throw new IllegalArgumentException("blk is null");
        }
        BufferBase buffer = bufferMgr.pin(blk);
        int add = 1;
        if(pinMap.containsKey(blk)){
            add = pinMap.get(blk)+1;
        }
        pinMap.put(blk, add);
        blockIdBases.put(blk, buffer);
    }

    /** Unpins the specified block.
     *
     * @param blk a reference to the disk block
     * @throws IllegalStateException if tx isn't in the ACTIVE state.
     * @throws IllegalArgumentException if block isn't pinned by this tx
     * @see #pin
     */
    @Override
    public synchronized void unpin(BlockIdBase blk) {//complete
        if(status != Status.ACTIVE){
            throw new IllegalStateException("Status must be active to unpin");
        }
        if(!blockIdBases.containsKey(blk)){
            throw new IllegalArgumentException("blk not pinned");
        }
        int times = pinMap.get(blk);
        bufferMgr.unpin(blockIdBases.get(blk));
        if(times == 1){
            blockIdBases.remove(blk);
            pinMap.remove(blk);
        }else{
            pinMap.put(blk, times-1);
        }
    }

    /** Returns the integer value stored at the specified offset of the specified
     * block.  The transaction acquires an "s-lock" on behalf of the client
     * before returning the value.
     *
     * @param blk a reference to a disk block
     * @param offset the byte offset within the block
     * @return the integer stored at that offset
     * @throws IllegalStateException if specified block isn't currently pinned by
     * this tx
     */
    @Override
    public int getInt(BlockIdBase blk, int offset) {//complete
        if(status != Status.ACTIVE || !blockIdBases.containsKey(blk)){
            throw new IllegalStateException("Status must be active to set or get, and need to be pinned");
        }
        concurrencyMgr.sLock(blk);
        PageBase page = blockIdBases.get(blk).contents();
        return page.getInt(offset);
    }

    /** Returns the boolean value stored at the specified offset of the specified
     * block.  The transaction acquires an "s-lock" on behalf of the client
     * before returning the value.
     *
     * @param blk a reference to a disk block
     * @param offset the byte offset within the block
     * @return the boolean stored at that offset
     * @throws IllegalStateException if specified block isn't currently pinned by this tx
     */
    @Override
    public boolean getBoolean(BlockIdBase blk, int offset) {//complete
        if(status != Status.ACTIVE || !blockIdBases.containsKey(blk)){
            throw new IllegalStateException("Status must be active to set or get and pinned");
        }
        concurrencyMgr.sLock(blk);
        PageBase page = blockIdBases.get(blk).contents();
        return page.getBoolean(offset);
    }

    /** Returns the double value stored at the specified offset of the specified
     * block.  The transaction acquires an "s-lock" on behalf of the client
     * before returning the value.
     *
     * @param blk a reference to a disk block
     * @param offset the byte offset within the block
     * @return the boolean stored at that offset
     * @throws IllegalStateException if specified block isn't currently pinned by this tx
     */
    @Override
    public double getDouble(BlockIdBase blk, int offset) {//complete
        if(status != Status.ACTIVE || !blockIdBases.containsKey(blk)){
            throw new IllegalStateException("Status must be active to set or get and pinned");
        }
        concurrencyMgr.sLock(blk);
        PageBase page = blockIdBases.get(blk).contents();
        return page.getDouble(offset);
    }

    /** Returns the string value stored at the specified offset of the specified
     * block.  The transaction acquires an "s-lock" on beghalf of the client
     * before returning the value.
     *
     * @param blk a reference to a disk block
     * @param offset the byte offset within the block
     * @return the string stored at that offset
     * @throws IllegalStateException if specified block isn't currently pinned by this tx
     */
    @Override
    public String getString(BlockIdBase blk, int offset) {//complete
        if(status != Status.ACTIVE || !blockIdBases.containsKey(blk)){
            throw new IllegalStateException("Status must be active to set or get and pinned");
        }
        concurrencyMgr.sLock(blk);
        PageBase page = blockIdBases.get(blk).contents();
        return page.getString(offset);
    }

    /** Returns the byte value stored at the specified offset of the specified
     * block.  The transaction acquires an "s-lock" on behalf of the client
     * before returning the value.
     *
     * @param blk a reference to a disk block
     * @param offset the byte offset within the block
     * @return the byte stored at that offset
     * @throws IllegalStateException if specified block isn't currently pinned by this tx
     */
    @Override
    public byte[] getBytes(BlockIdBase blk, int offset) {//complete
        if(status != Status.ACTIVE || !blockIdBases.containsKey(blk)){
            throw new IllegalStateException("Status must be active to set or get and pinned");
        }
        concurrencyMgr.sLock(blk);
        PageBase page = blockIdBases.get(blk).contents();
        return page.getBytes(offset);
    }

    /** Stores an integer at the specified offset of the specified block.  The
     * transaction acquires an "x-lock" on behalf of the client before reading
     * the value, creating the appropriate "update" log record and adding the
     * record to the log file.  Finally, the modified value is written to the
     * buffer.  The transaction is responsible for invoking the buffer
     * setModified() method, passing in the appropriate parameter values.
     *
     * @param blk a reference to the disk block
     * @param offset a byte offset within that block
     * @param val the value to be stored
     * @param okToLog true iff the client wants the operation to be logged, false
     * otherwise.
     * @throws IllegalStateException if specified block isn't currently pinned by this tx
     */
    @Override
    public void setInt(BlockIdBase blk, int offset, int val, boolean okToLog) {//complete
        if(status != Status.ACTIVE || !blockIdBases.containsKey(blk)){
            throw new IllegalStateException("Status must be active to set or get and pinned");
        }
        concurrencyMgr.xLock(blk);
        int oldVal = 0;
        if(okToLog){
            oldVal = blockIdBases.get(blk).contents().getInt(offset);
        }
        blockIdBases.get(blk).contents().setInt(offset, val);
        int lsn = -1;
        if(okToLog){
            lsn = recoveryMgr.setInt(blk,  offset, val, oldVal);
        }
        blockIdBases.get(blk).setModified(txNum, lsn);
    }

    /** Stores an boolean at the specified offset of the specified block.  The
     * transaction acquires an "x-lock" on behalf of the client before reading
     * the value, creating the appropriate "update" log record and adding the
     * record to the log file.  Finally, the modified value is written to the
     * buffer.  The transaction is responsible for invoking the buffer
     * setModified() method, passing in the appropriate parameter values.
     *
     * @param blk a reference to the disk block
     * @param offset a byte offset within that block
     * @param val the value to be stored
     * @param okToLog true iff the client wants the operation to be logged, false
     * otherwise.
     * @throws IllegalStateException if specified block isn't currently pinned by this tx
     */
    @Override
    public void setBoolean(BlockIdBase blk, int offset, boolean val, boolean okToLog) {//complete
        if(status != Status.ACTIVE || !blockIdBases.containsKey(blk)){
            throw new IllegalStateException("Status must be active to set or get and pinned");
        }
        concurrencyMgr.xLock(blk);
        boolean oldVal = false;
        if(okToLog){
            oldVal = blockIdBases.get(blk).contents().getBoolean(offset);
        }
        blockIdBases.get(blk).contents().setBoolean(offset, val);
        int lsn = -1;
        if(okToLog){
            lsn = recoveryMgr.setBoolean(blk,  offset, val, oldVal);
        }
        blockIdBases.get(blk).setModified(txNum, lsn);
    }

    /** Stores a double at the specified offset of the specified block.  The
     * transaction acquires an "x-lock" on behalf of the client before reading
     * the value, creating the appropriate "update" log record and adding the
     * record to the log file.  Finally, the modified value is written to the
     * buffer.  The transaction is responsible for invoking the buffer
     * setModified() method, passing in the appropriate parameter values.
     *
     * @param blk a reference to the disk block
     * @param offset a byte offset within that block
     * @param val the value to be stored
     * @param okToLog true iff the client wants the operation to be logged, false
     * otherwise.
     * @throws IllegalStateException if specified block isn't currently pinned by this tx   */
    @Override
    public void setDouble(BlockIdBase blk, int offset, double val, boolean okToLog) {//complete
        if(status != Status.ACTIVE || !blockIdBases.containsKey(blk)){
            throw new IllegalStateException("Status must be active to set or get and pinned");
        }
        concurrencyMgr.xLock(blk);
        double oldVal = 0;
        if(okToLog){
            oldVal = blockIdBases.get(blk).contents().getDouble(offset);
        }
        blockIdBases.get(blk).contents().setDouble(offset, val);
        int lsn = -1;
        if(okToLog){
            lsn = recoveryMgr.setDouble(blk,  offset, val, oldVal);
        }
        blockIdBases.get(blk).setModified(txNum, lsn);
    }

    /** Stores a string at the specified offset of the specified block. The
     * transaction acquires an "x-lock" on behalf of the client before reading
     * the value, creating the appropriate "update" log record and adding the
     * record to the log file.  Finally, the modified value is written to the
     * buffer.  The transaction is responsible for invoking the buffer
     * setModified() method, passing in the appropriate parameter values.
     *
     * @param blk a reference to the disk block
     * @param offset a byte offset within that block
     * @param val the value to be stored
     * @param okToLog true iff the client wants the operation to be logged, false
     * otherwise.
     * @throws IllegalStateException if specified block isn't currently pinned by this tx
     */
    @Override
    public void setString(BlockIdBase blk, int offset, String val, boolean okToLog) {//complete
        if(status != Status.ACTIVE || !blockIdBases.containsKey(blk)){
            throw new IllegalStateException("Status must be active to set or get and pinned status is " + Status.ACTIVE.name());
        }
        concurrencyMgr.xLock(blk);
        String oldVal = "";
        if(okToLog){
            oldVal = blockIdBases.get(blk).contents().getString(offset);
        }
        blockIdBases.get(blk).contents().setString(offset, val);
        int lsn = -1;
        if(okToLog){
            recoveryMgr.setString(blk, offset, val, oldVal);
        }
        blockIdBases.get(blk).setModified(txNum, lsn);
    }

    /** Stores a byte[] at the specified offset of the specified block. The
     * transaction acquires an "x-lock" on behalf of the client before reading
     * the value, creating the appropriate "update" log record and adding the
     * record to the log file.  Finally, the modified value is written to the
     * buffer.  The transaction is responsible for invoking the buffer
     * setModified() method, passing in the appropriate parameter values.
     *
     * @param blk a reference to the disk block
     * @param offset a byte offset within that block
     * @param val the value to be stored
     * @param okToLog true iff the client wants the operation to be logged, false
     * otherwise.
     * @throws IllegalStateException if specified block isn't currently pinned by this tx
     */
    @Override
    public void setBytes(BlockIdBase blk, int offset, byte[] val, boolean okToLog) {//complete
        if(status != Status.ACTIVE || !blockIdBases.containsKey(blk)){
            throw new IllegalStateException("Status must be active to set or get and pinned");
        }
        concurrencyMgr.xLock(blk);
        byte[] oldVal = null;
        if(okToLog){
            oldVal = blockIdBases.get(blk).contents().getBytes(offset);
        }
        blockIdBases.get(blk).contents().setBytes(offset, val);
        int lsn = -1;
        if(okToLog){
            recoveryMgr.setBytes(blk, offset, val, oldVal);
        }
        blockIdBases.get(blk).setModified(txNum, lsn);
    }

    /** Returns the number of blocks in the specified file.
     *
     * Note: be sure to provide transactional semantics for this method.
     *
     * @param filename the name of the file
     * @return the number of blocks in the file
     * @throws IllegalStateException if tx isn't in the ACTIVE state.
     */
    @Override
    public int size(String filename) {//complete
        if(status != Status.ACTIVE){
            throw new IllegalStateException("Status must be active to get size");
        }

        int len = fileMgr.length(filename);
        BlockIdBase blk = new BlockId(filename, len);

        concurrencyMgr.sLock(blk);

        // If actual length > block number we locked, we missed the end.
        while (fileMgr.length(filename) > blk.number()) {
            // Update our view of the length
            len = fileMgr.length(filename);
            blk = new BlockId(filename, len);

            // Lock the NEW end (and keep the old lock to be safe/strict)
            concurrencyMgr.sLock(blk);
        }

        return len;
    }

    /** Appends a new block to the end of the specified file and returns a
     * reference to it.
     *
     * Note: be sure to provide transactional semantics for this method.
     *
     * @param filename the name of the file
     * @return a reference to the newly-created disk block
     * @throws IllegalStateException if tx isn't in the ACTIVE state.
     */
    @Override
    public BlockIdBase append(String filename) {//complete
        if(status != Status.ACTIVE){
            throw new IllegalStateException("Status must be active to append");
        }
        int size = size(filename);
        BlockIdBase blk = new BlockId(filename, size);
        concurrencyMgr.xLock(blk);
        blk = fileMgr.append(filename);
        return blk;
    }

    /** Returns the size of blocks, uniform across all disk blocks managed by the
     * DBMS.
     *
     * @return the uniform size  of all disk blocks
     */
    @Override
    public int blockSize() {
        return blockSize;
    }

    /** Returns the number of available (i.e. unpinned) buffers.
     *
     * @return the number of available buffers
     * @throws IllegalStateException if tx isn't in the ACTIVE state.
     */
    @Override
    public int availableBuffs() {
        if(status != Status.ACTIVE){
            throw new IllegalStateException("Status must be active to get buffers");
        }
        return bufferMgr.available();
    }
}
