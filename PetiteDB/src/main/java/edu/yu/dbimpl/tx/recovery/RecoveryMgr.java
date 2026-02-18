package edu.yu.dbimpl.tx.recovery;

import edu.yu.dbimpl.buffer.BufferBase;
import edu.yu.dbimpl.buffer.BufferMgrBase;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.log.LogMgrBase;

import java.util.ArrayList;
import java.util.List;

public class RecoveryMgr extends RecoveryMgrBase{
    /**
     * Create a recovery manager for the specified transaction.
     *
     * @param logMgr    the singleton logMgr for the DBMS
     * @param bufferMgr the singleton bufferMgr for the DBMS
     */

    private final int txNum;
    private final LogMgrBase logMgr;
    private final BufferMgrBase bufferMgr;
    private List<Integer> LSNs;
    public RecoveryMgr(int txNUm, LogMgrBase logMgr, BufferMgrBase bufferMgr) {
        super(logMgr, bufferMgr);
        this.txNum = txNUm;
        this.logMgr = logMgr;
        this.bufferMgr = bufferMgr;
        LSNs = new ArrayList<>();
    }

    /** Write a commit record to the log, and flushes it to disk, and do whatever
     * concommitant processing is required by your implementation.
     *
     */
    @Override
    public void commit() {
        LogRecord lr = new LogRecord(txNum, LogRecordBase.LogType.COMMIT);
        int lsn = logMgr.append(lr.getBytes());
        logMgr.flush(lsn);
    }

    /** Write a rollback record to the log and flush it to disk, and do whatever
     * concommitant processing is required by your implementation.
     */
    @Override
    public void rollback() {
        LogRecord lr = new LogRecord(txNum, LogRecordBase.LogType.ROLLBACK);
        int lsn = logMgr.append(lr.getBytes());
        logMgr.flush(lsn);
    }


    /** Recover uncompleted transactions from the log and then write a quiescent
     * checkpoint record to the log and flush it.
     */
    @Override
    public void recover() {
        LogRecord lr = new LogRecord(txNum, LogRecordBase.LogType.CHECKPOINT);
        int lsn = logMgr.append(lr.getBytes());
        logMgr.flush(lsn);
    }

    public void start(){
        LogRecord lr = new LogRecord(txNum, LogRecordBase.LogType.START);
        logMgr.append(lr.getBytes());
    }
    /** Write a setInt record to the log and return its lsn.
     *
     * @param block the buffer containing the page
     * @param offset the offset of the value in the page
     * @param newval the value to be written
     * @return the LSN after the record has been written to the log
     */
    @Override
    public int setInt(BlockIdBase block, int offset, int newval, int oldval) {
        LogRecord lr = new LogRecord(txNum, LogRecordBase.LogType.SET_INT, block, offset, newval, oldval);
        int lsn = logMgr.append(lr.getBytes());
        LSNs.add(lsn);
        return lsn;
    }

    /** Write a setBoolean record to the log and return its lsn.
     *
     * @param block the buffer containing the page
     * @param offset the offset of the value in the page
     * @param newval the value to be written
     * @return the LSN after the record has been written to the log
     */
    @Override
    public int setBoolean(BlockIdBase block, int offset, boolean newval,  boolean oldval) {
        LogRecord lr = new LogRecord(txNum, LogRecordBase.LogType.SET_BOOL, block, offset, newval, oldval);
        int lsn = logMgr.append(lr.getBytes());
        LSNs.add(lsn);
        return lsn;
    }

    /** Write a setDouble record to the log and return its lsn.
     *
     * @param block the buffer containing the page
     * @param offset the offset of the value in the page
     * @param newval the value to be written
     * @return the LSN after the record has been written to the log
     */
    @Override
    public int setDouble(BlockIdBase block, int offset, double newval,  double oldval) {
        LogRecord lr = new LogRecord(txNum, LogRecordBase.LogType.SET_DOUBLE, block, offset, newval, oldval);
        int lsn = logMgr.append(lr.getBytes());
        LSNs.add(lsn);
        return lsn;
    }

    /** Write a setString record to the log and return its lsn.
     *
     * @param block the buffer containing the page
     * @param offset the offset of the value in the page
     * @param newval the value to be written
     * @return the LSN after the record has been written to the log
     */
    @Override
    public int setString(BlockIdBase block, int offset, String newval, String oldval) {
        LogRecord lr = new LogRecord(txNum, LogRecordBase.LogType.SET_STRING, block, offset, newval, oldval);
        int lsn = logMgr.append(lr.getBytes());
        LSNs.add(lsn);
        return lsn;
    }

    /** Write a setBytes record to the log and return its lsn.
     * @param block the buffer containing the page
     * @param offset the offset of the value in the page
     * @param newval the value to be written
     * @return the LSN after the record has been written to the log
     */
    @Override
    public int setBytes(BlockIdBase block, int offset, byte[] newval, byte[] oldval) {
        LogRecord lr = new LogRecord(txNum, LogRecordBase.LogType.SET_BYTES, block, offset, newval, oldval);
        int lsn = logMgr.append(lr.getBytes());
        LSNs.add(lsn);
        return lsn;
    }
}
