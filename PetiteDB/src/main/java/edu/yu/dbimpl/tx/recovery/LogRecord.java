package edu.yu.dbimpl.tx.recovery;

import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.tx.TxBase;

import java.io.*;

public class LogRecord implements LogRecordBase{
    private LogType logType;
    private BlockIdBase blockId;
    private int offset;
    private byte[] data;
    private int newLength;
    private String words;
    private int txNumber;
    private int intValue;
    private boolean bool;
    private double value;

    //for undo
    private int oldInt;//old value for int
    private boolean oldBool;
    private byte[] oldData;
    private int oldLength;
    private String oldWords;
    private double old;

    /**
     * Constructor for Set String
     * @param logType
     * @param blockIdBase
     * @param offset
     * @param words
     */
    public LogRecord(int txNumber, LogType logType, BlockIdBase blockIdBase, int offset, String words, String oldWords) {
        this.txNumber = txNumber;
        this.logType = logType;
        this.blockId = blockIdBase;
        this.offset = offset;
        this.words = words;
        this.oldWords = oldWords;
    }

    /**
     * Constructor for set int
     * @param logType
     * @param blockIdBase
     * @param offset
     * @param value
     */
    public LogRecord(int txNumber, LogType logType, BlockIdBase blockIdBase, int offset, int value, int oldInt) {
        this.txNumber = txNumber;
        this.logType = logType;
        this.blockId = blockIdBase;
        this.offset = offset;
        this.oldInt = oldInt;
        this.intValue = value;
    }

    /**
     * Constructor for set double
     * @param logType
     * @param blockIdBase
     * @param offset
     * @param value
     */
    public LogRecord(int txNumber, LogType logType, BlockIdBase blockIdBase, int offset, double value, double old) {
        this.txNumber = txNumber;
        this.logType = logType;
        this.blockId = blockIdBase;
        this.offset = offset;
        this.value = value;
        this.old = old;
    }

    /**
     * Constructor for set bool
     * @param logType
     * @param blockIdBase
     * @param offset
     * @param bool
     */
    public LogRecord(int txNumber, LogType logType, BlockIdBase blockIdBase, int offset, boolean bool, boolean oldBool) {
        this.txNumber = txNumber;
        this.logType = logType;
        this.blockId = blockIdBase;
        this.offset = offset;
        this.bool = bool;
        this.oldBool = oldBool;
    }

    /**
     * Constructor for bytes
     * @param logType
     * @param blockIdBase
     * @param offset
     * @param data
     * @param oldData
     */
    public LogRecord(int txNumber, LogType logType, BlockIdBase blockIdBase, int offset, byte[] data, byte[] oldData) {
        this.txNumber = txNumber;
        this.logType = logType;
        this.blockId = blockIdBase;
        this.offset = offset;
        this.oldLength = oldData.length;
        this.newLength = data.length;
        this.data = data;
        this.oldData = oldData;
    }

    /**
     * Constructor for tx log
     * for types START, COMMIT, ROLLBACK, CHECKPOINT
     * @return
     */
    public LogRecord(int txNumber, LogType logType) {
        this.txNumber = txNumber;
        this.logType = logType;
    }

    @Override
    public int op() {
        return logType.ordinal();
    }

    @Override
    public int txNumber() {
        return txNumber;
    }

    /** Undoes the operation encoded by this log record.  The "undo" semantics
     * may not apply to all LogRecord types, and they are free to provide a no-op
     * implementation.
     *
     * @param tx the transaction that is performing the undo operation.
     */
    @Override
    public void undo(TxBase tx) {//complete
        if(logType == LogType.COMMIT || logType == LogType.ROLLBACK || logType == LogType.START || logType == LogType.CHECKPOINT) {
            return;
        }
        tx.pin(blockId);
        switch(logType) {
            case SET_INT:
                tx.setInt(blockId, offset, oldInt, false);
                break;
            case SET_BOOL:
                tx.setBoolean(blockId, offset, oldBool, false);
                break;
            case SET_DOUBLE:
                tx.setDouble(blockId, offset, old, false);
                break;
            case SET_STRING:
                tx.setString(blockId, offset, oldWords, false);
                break;
            case SET_BYTES:
                tx.setBytes(blockId, offset, oldData, false);
                break;
        }
        tx.unpin(blockId);
    }

    /**
     * Serialize transaction for disk
     * @return
     */
    public byte[] getBytes() {//complete
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        try {
            // Write fixed fields
            dos.writeInt(logType.ordinal());
            dos.writeInt(txNumber);

            // For transaction-only logs (START, COMMIT, ROLLBACK, CHECKPOINT), we're done
            if (logType == LogType.START || logType == LogType.COMMIT ||
                    logType == LogType.ROLLBACK || logType == LogType.CHECKPOINT) {
                return baos.toByteArray();
            }

            // Write BlockId (filename and block number)
            dos.writeUTF(blockId.fileName());
            dos.writeInt(blockId.number());

            dos.writeInt(offset);

            // Write variable-length data based on type with undo fields
            if (logType == LogType.SET_STRING) {
                dos.writeUTF(words);
                dos.writeUTF(oldWords);
            } else if (logType == LogType.SET_BYTES) {
                dos.writeInt(newLength);
                dos.write(data);
                dos.writeInt(oldLength);
                dos.write(oldData);
            } else if (logType == LogType.SET_INT) {
                dos.writeInt(intValue); // The int value
                dos.writeInt(oldInt);//old value
            }else if(logType == LogType.SET_DOUBLE){
                dos.writeDouble(value);
                dos.writeDouble(old);
            } else if (logType == LogType.SET_BOOL) {
                dos.writeBoolean(bool);
                dos.writeBoolean(oldBool);
            }
            return baos.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException("Error serializing LogRecord", e);
        }
    }

    public static LogRecord getLogRecord(byte[] bytes) {//complete
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        DataInputStream dis = new DataInputStream(bais);

        try {
            // Read fixed fields
            int logTypeOrdinal = dis.readInt();
            LogType logType = LogType.values()[logTypeOrdinal];
            int txNumber = dis.readInt();

            // For transaction-only logs, return immediately
            if (logType == LogType.START || logType == LogType.COMMIT ||
                    logType == LogType.ROLLBACK || logType == LogType.CHECKPOINT) {
                return new LogRecord(txNumber, logType);
            }

            // Read BlockId
            String fileName = dis.readUTF();
            int blockNumber = dis.readInt();
            BlockIdBase blockId = new BlockId(fileName, blockNumber);

            int offset = dis.readInt();

            // Reconstruct based on type with undo fields
            if (logType == LogType.SET_STRING) {
                String words = dis.readUTF();
                String oldData = dis.readUTF();
                return new LogRecord(txNumber, logType, blockId, offset, words, oldData);
            } else if (logType == LogType.SET_BYTES) {
                int newLength = dis.readInt();
                byte[] data = new byte[newLength];
                dis.readFully(data);
                int oldDataLength = dis.readInt();
                byte[] oldData = new byte[oldDataLength];
                dis.readFully(oldData);
                return new LogRecord(txNumber, logType, blockId, offset,  data,  oldData);
            } else if (logType == LogType.SET_INT) {
                int value = dis.readInt();
                int begin = dis.readInt();
                return new LogRecord(txNumber, logType, blockId, offset, value, begin);
            } else if(logType == LogType.SET_DOUBLE){
                double value = dis.readDouble();
                double old = dis.readDouble();
                return new LogRecord(txNumber, logType, blockId, offset, value, old);
            } else if (logType == LogType.SET_BOOL) {
                boolean bool = dis.readBoolean();
                boolean oldBool = dis.readBoolean();
                return new LogRecord(txNumber, logType, blockId, offset, bool, oldBool);
            } else {
                throw new IllegalArgumentException("Unknown log type: " + logType);
            }
        } catch (IOException e) {
            throw new RuntimeException("Error deserializing LogRecord", e);
        }
    }

}
