package tx;

import edu.yu.dbimpl.buffer.*;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.*;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.tx.TxBase;
import edu.yu.dbimpl.tx.TxMgr;
import edu.yu.dbimpl.tx.TxMgrBase;
import edu.yu.dbimpl.tx.concurrency.LockAbortException;
import edu.yu.dbimpl.tx.concurrency.LockTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static java.lang.Thread.sleep;
import static org.junit.jupiter.api.Assertions.*;

public class rmMicroTest {
    private BlockIdBase b1;
    private BlockIdBase b2;
    private BlockIdBase b3;
    private BlockIdBase b4;
    private BlockIdBase b5;
    private BlockIdBase b6;
    private BlockIdBase b7;
    private BlockIdBase b8;
    private BlockIdBase b9;
    private BlockIdBase b10;
    private File file;
    @BeforeEach
    public  void setup() {
        file = new File("dbDirectory1");
        b1 = new BlockId("testfile1", 0);
        b2 = new BlockId("testfile1", 1);
        b3 = new BlockId("testfile1", 2);
        b4 = new BlockId("testfile1", 3);
        b5 = new BlockId("testfile1", 4);
        b6 = new BlockId("testfile1", 5);
        b7 = new BlockId("testfile1", 6);
        b8 = new BlockId("testfile1", 7);
        b9 = new BlockId("testfile1", 8);
        b10 = new BlockId("testfile1", 9);
    }
    @AfterEach
    public void teardown() {
        LockTable lt = LockTable.INSTANCE;
        lt.resetAllLockState();
        TxMgr.resetCount();
    }
    @Test
    public void rmMicroTestPart1() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");
        BufferMgrBase buffeMgr = new BufferMgr(fm, logManager, 8, 500);

        TxMgrBase txMgr = new TxMgr(fm, logManager, buffeMgr, 500);

        assertEquals(500, txMgr.getMaxWaitTimeInMillis());

        TxBase tx = txMgr.newTx();
        for (int i = 0; i < 2; i++) {
            tx.append("testfile1");
        }
        tx.commit();

        TxBase tx1 = txMgr.newTx();
        TxBase tx2 = txMgr.newTx();

        tx1.pin(b1);
        tx2.pin(b2);

        tx1.setInt(b1, 0, 0, false);
        tx2.setInt(b2, 0, 0, false);

        tx1.setInt(b1, 4, 4, false);
        tx2.setInt(b2, 4, 4, false);

        tx1.setInt(b1, 8, 8, false);
        tx2.setInt(b2, 8, 8, false);

        tx1.setInt(b1, 12, 12, false);
        tx2.setInt(b2, 12, 12, false);

        tx1.setInt(b1, 16, 16, false);
        tx2.setInt(b2, 16, 16, false);

        tx1.setInt(b1, 20, 20, false);
        tx2.setInt(b2, 20, 20, false);

        tx1.setBoolean(b1, 100, true, false);
        tx2.setBoolean(b2, 100, true, false);

        tx1.setDouble(b1, 120, 1.4142, false);
        tx2.setDouble(b2, 120, 1.4142, false);

        tx1.setString(b1, 30, "abc", false);
        tx2.setString(b2, 30, "def", false);

        byte[] ba = new byte[3];
        byte[] ba2 = new byte[3];
        ba[0] = 7;
        ba[1] = 13;
        ba[2] = 42;
        ba2[0] = 42;
        ba2[1] = 13;
        ba2[2] = 7;

        tx1.setBytes(b1, 182, ba, false);
        tx2.setBytes(b2, 182, ba2, false);

        tx1.commit();
        tx2.commit();

        TxBase tx3 = txMgr.newTx();
        tx3.pin(b1);
        assertEquals(0, tx3.getInt(b1, 0));
        assertEquals(4, tx3.getInt(b1, 4));
        assertEquals(8, tx3.getInt(b1, 8));
        assertEquals(12, tx3.getInt(b1, 12));
        assertEquals(20, tx3.getInt(b1, 20));
        assertEquals(16, tx3.getInt(b1, 16));
        assertTrue(tx3.getBoolean(b1, 100));
        assertEquals(1.4142,  tx3.getDouble(b1, 120));
        assertEquals("abc", tx3.getString(b1, 30));
        tx3.commit();

        TxBase tx4 = txMgr.newTx();
        tx4.pin(b2);
        assertEquals(0, tx4.getInt(b2, 0));
        assertEquals(4, tx4.getInt(b2, 4));
        assertEquals(8, tx4.getInt(b2, 8));
        assertEquals(12, tx4.getInt(b2, 12));
        assertEquals(20, tx4.getInt(b2, 20));
        assertEquals(16, tx4.getInt(b2, 16));
        assertTrue(tx4.getBoolean(b2, 100));
        assertEquals(1.4142,  tx4.getDouble(b2, 120));
        assertEquals("def", tx4.getString(b2, 30));
        tx4.commit();

        TxBase tx5 = txMgr.newTx();
        TxBase tx6 = txMgr.newTx();

        tx5.pin(b1);
        tx6.pin(b2);

        tx5.setInt(b1, 0, 100, true);
        tx6.setInt(b2, 0, 100, true);

        tx5.setInt(b1, 4, 104, true);
        tx6.setInt(b2, 4, 104, true);

        tx5.setInt(b1, 8, 108, true);
        tx6.setInt(b2, 8, 108, true);

        tx5.setInt(b1, 12, 112, true);
        tx6.setInt(b2, 12, 112, true);

        tx5.setInt(b1, 16, 116, true);
        tx6.setInt(b2, 16, 116, true);

        tx5.setInt(b1, 20, 120, true);
        tx6.setInt(b2, 20, 120, true);

        tx5.setBoolean(b1, 100, false, true);
        tx6.setBoolean(b2, 100, false, true);

        tx5.setDouble(b1, 120, 2.0, true);
        tx6.setDouble(b2, 120, 1.0, true);

        tx5.setString(b1, 30, "uvw", true);
        tx6.setString(b2, 30, "xyz", true);

        tx5.setBytes(b1, 182, ba2, true);
        tx6.setBytes(b2, 182, ba, true);

        buffeMgr.flushAll(6);
        buffeMgr.flushAll(7);

        tx5.rollback();

        Page p1 = new Page(400);
        fm.read(b1, p1);
        Page p2 = new Page(400);
        fm.read(b2, p2);

        assertEquals(0, p1.getInt(0));
        assertEquals(4, p1.getInt(4));
        assertEquals(8, p1.getInt(8));
        assertEquals(12, p1.getInt(12));
        assertEquals(20, p1.getInt(20));
        assertEquals(16, p1.getInt(16));
    }

    @Test
    public void rmMicroTestPart2() {
        Properties dbProperties2 = new Properties();
        dbProperties2.put(DBConfiguration.DB_STARTUP, Boolean.toString(false));
        DBConfiguration config2 = DBConfiguration.INSTANCE;
        config2.setConfiguration(dbProperties2);
        FileMgrBase fm2 = new FileMgr(file, 400);
        LogMgrBase logManager2 = new LogMgr(fm2, "logfile");
        BufferMgrBase buffeMgr2 = new BufferMgr(fm2, logManager2, 10, 500);

        TxMgrBase txMgr2 = new TxMgr(fm2, logManager2, buffeMgr2, 500);

        TxBase tx7 = txMgr2.newTx();
        tx7.pin(b2);
        assertEquals(0, tx7.getInt(b2, 0));
        assertEquals(4, tx7.getInt(b2, 4));
        assertEquals(8, tx7.getInt(b2, 8));
        assertEquals(12, tx7.getInt(b2, 12));
        assertEquals(20, tx7.getInt(b2, 20));
        assertEquals(16, tx7.getInt(b2, 16));
        assertTrue(tx7.getBoolean(b2, 100));
        assertEquals(1.4142,  tx7.getDouble(b2, 120));
        assertEquals("def", tx7.getString(b2, 30));
        tx7.commit();
    }

}
