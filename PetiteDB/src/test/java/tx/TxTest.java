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
public class TxTest {
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
        for (File f : file.listFiles()) {
            f.delete();
        }
        file.delete();
    }
    @Test
    public void TxMgrClassTest() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");
        BufferMgrBase buffeMgr = new BufferMgr(fm, logManager, 10, 500);

        TxMgrBase txMgr = new TxMgr(fm, logManager, buffeMgr, 500);

        assertEquals(500, txMgr.getMaxWaitTimeInMillis());

        TxBase tx1 = txMgr.newTx();
        assertEquals(TxBase.Status.ACTIVE, tx1.getStatus());
        assertEquals(1, tx1.txnum());
        assertEquals(10, tx1.availableBuffs());
        assertEquals(400, tx1.blockSize());
        for(int i = 0; i < 20; i++){
            tx1.append("testfile1");
        }
        tx1.pin(b1);
        tx1.setString(b1, 4, "Hello there 1", false);
        tx1.setInt(b1, 100, 613, false);
        tx1.setBoolean(b1, 105, true, false);
        tx1.setDouble(b1, 108, 345.87, false);
        byte[] ba = new byte[8];
        for(int i = 0; i < 8; i++){
            ba[i] = (byte)i;
        }
        tx1.setBytes(b1, 200, ba, false);
        tx1.commit();
        assertEquals(TxBase.Status.COMMITTED, tx1.getStatus());

        TxBase tx2 = txMgr.newTx();
        assertEquals(TxBase.Status.ACTIVE, tx2.getStatus());
        assertEquals(2, tx2.txnum());
        tx2.pin(b1);
        tx2.setString(b1, 4, "Hello there 2", true);
        tx2.setInt(b1, 100, 614, true);
        tx2.setBoolean(b1, 105, false, true);
        tx2.setDouble(b1, 108, 345.97, true);
        byte[] ba2 = new byte[8];
        for(int i = 0; i < 8; i++){
            ba2[i] = (byte)(i+1);
        }
        tx2.setBytes(b1, 200, ba2, true);
        logManager.flush(4);

        //crash and start

        Properties dbProperties2 = new Properties();
        dbProperties2.put(DBConfiguration.DB_STARTUP, Boolean.toString(false));
        DBConfiguration config2 = DBConfiguration.INSTANCE;
        config2.setConfiguration(dbProperties2);
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm2 = new FileMgr(file, 400);
        LogMgrBase logManager2 = new LogMgr(fm2, "logfile");
        BufferMgrBase buffeMgr2 = new BufferMgr(fm2, logManager2, 10, 500);

        TxMgrBase txMgr2 = new TxMgr(fm2, logManager2, buffeMgr2, 500);

        //test tx logic
        TxBase tx3 = txMgr2.newTx();
        assertEquals(TxBase.Status.ACTIVE, tx3.getStatus());
        assertEquals(3, tx3.txnum());
        tx3.pin(b1);
        String s1 = tx3.getString(b1, 4);
        assertEquals("Hello there 1", s1);
        tx3.setString(b1, 4, "Hello there 3", true);
        tx3.rollback();
        assertEquals(TxBase.Status.ROLLED_BACK, tx3.getStatus());
        TxBase tx4 = txMgr2.newTx();
        assertEquals(TxBase.Status.ACTIVE, tx4.getStatus());
        assertEquals(4, tx4.txnum());
        tx4.pin(b1);
        String s2 = tx4.getString(b1, 4);
        assertEquals("Hello there 1", s2);
        tx4.setString(b1, 4, "Hello there 4", true);
        tx4.commit();
        assertEquals(TxBase.Status.COMMITTED, tx4.getStatus());
        TxBase tx5 = txMgr2.newTx();
        assertEquals(TxBase.Status.ACTIVE, tx5.getStatus());
        assertEquals(5, tx5.txnum());
        tx5.pin(b1);
        String s3 = tx5.getString(b1, 4);
        assertEquals("Hello there 4", s3);
        tx5.setString(b1, 4, "Hello there 5", true);
        tx5.commit();
        assertEquals(TxBase.Status.COMMITTED, tx5.getStatus());

        //test max wait thrown
        TxBase tx6 = txMgr2.newTx();
        tx6.pin(b1);
        tx6.setString(b1, 4, "Hello there 6", true);

        TxBase tx7 = txMgr2.newTx();
        tx7.pin(b1);
        assertThrows(LockAbortException.class, () -> tx7.setString(b1, 4, "Hello there 7", true));

        tx6.commit();

        TxBase tx8 = txMgr2.newTx();
        tx8.pin(b1);
        String st8 = tx8.getString(b1, 4);
        assertEquals("Hello there 6", st8);
        TxBase tx9 = txMgr2.newTx();
        tx9.pin(b1);
        String st9 = tx9.getString(b1, 4);
        assertEquals("Hello there 6", st9);

        tx9.rollback();
        assertEquals(TxBase.Status.ROLLED_BACK, tx9.getStatus());

        tx8.setString(b1, 4, "Hello there 7", true);

        tx8.commit();
        assertEquals(TxBase.Status.COMMITTED, tx8.getStatus());


        TxBase tx10 = txMgr2.newTx();
        tx10.pin(b1);
        int i10 = tx10.getInt(b1, 100);
        assertEquals(613, i10);
        tx10.setString(b1, 4, "Hello there 8", true);
        tx10.commit();
        assertEquals(TxBase.Status.COMMITTED, tx10.getStatus());


    }

    @Test
    public void performance(){
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");
        BufferMgrBase buffeMgr = new BufferMgr(fm, logManager, 10, 500);

        TxMgrBase txMgr = new TxMgr(fm, logManager, buffeMgr, 500);
        TxBase tx1 = txMgr.newTx();
        assertEquals(TxBase.Status.ACTIVE, tx1.getStatus());
        assertEquals(1, tx1.txnum());
        assertEquals(10, tx1.availableBuffs());
        assertEquals(400, tx1.blockSize());
        for(int i = 0; i < 20; i++){
            tx1.append("testfile1");
        }
        tx1.pin(b1);
        tx1.setString(b1, 4, "Hello there 1", false);
        tx1.setInt(b1, 100, 613, false);
        tx1.setBoolean(b1, 105, true, false);
        tx1.setDouble(b1, 108, 345.87, false);
        byte[] ba = new byte[8];
        for(int i = 0; i < 8; i++){
            ba[i] = (byte)i;
        }
        tx1.setBytes(b1, 200, ba, false);
        tx1.commit();
        assertEquals(TxBase.Status.COMMITTED, tx1.getStatus());


        //6 logs per tx
        for(int i = 0; i < 10000; i++){
            TxBase tx = txMgr.newTx();
            tx.pin(b1);
            tx.getInt(b1, 100);
            tx.getDouble(b1, 108);
            tx.getString(b1, 4);
            tx.getBoolean(b1, 105);

            tx.setString(b1, 4, "Hello there "+i, true);
            tx.setInt(b1, 100, i, true);
            tx.setBoolean(b1, 105, true, true);
            tx.setDouble(b1, 108, 345.87, true);
            tx.commit();
        }

        TxBase tx2 = txMgr.newTx();
        tx2.pin(b1);
        String st2 = tx2.getString(b1, 4);
        assertEquals("Hello there 9999", st2);
        int i2 = tx2.getInt(b1, 100);
        assertEquals(9999, i2);
        boolean b2 = tx2.getBoolean(b1, 105);
        assertTrue(b2);
        double r2 = tx2.getDouble(b1, 108);
        assertEquals(345.87, r2, 0.01);
        tx2.commit();

        Iterator<byte[]> logs = logManager.iterator();
        int total = 0;
        while(logs.hasNext()){
            byte[] log = logs.next();
            total++;
        }
        assertEquals(60004, total);
    }

    @Test
    public void performance2(){
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");
        BufferMgrBase buffeMgr = new BufferMgr(fm, logManager, 10, 500);

        TxMgrBase txMgr = new TxMgr(fm, logManager, buffeMgr, 500);

        assertEquals(500, txMgr.getMaxWaitTimeInMillis());

        TxBase tx1 = txMgr.newTx();
        assertEquals(TxBase.Status.ACTIVE, tx1.getStatus());
        assertEquals(1, tx1.txnum());
        assertEquals(10, tx1.availableBuffs());
        assertEquals(400, tx1.blockSize());
        for(int i = 0; i < 20; i++){
            tx1.append("testfile1");
        }
        tx1.pin(b1);
        tx1.setString(b1, 4, "Hello there 1", false);
        tx1.setInt(b1, 100, 613, false);
        tx1.setBoolean(b1, 105, true, false);
        tx1.setDouble(b1, 108, 345.87, false);
        byte[] ba = new byte[8];
        for(int i = 0; i < 8; i++){
            ba[i] = (byte)i;
        }
        tx1.setBytes(b1, 200, ba, false);
        tx1.commit();
        assertEquals(TxBase.Status.COMMITTED, tx1.getStatus());

        Runnable task1 = () -> {
            for (int i = 0; i < 100; i++) {
                TxBase tx = txMgr.newTx();
                tx.pin(b1);
                tx.getInt(b1, 100);
                tx.getDouble(b1, 108);
                tx.getString(b1, 4);
                tx.getBoolean(b1, 105);

                tx.setString(b1, 4, "Hello there "+i, true);
                tx.setInt(b1, 100, i, true);
                tx.setBoolean(b1, 105, true, true);
                tx.setDouble(b1, 108, 345.87, true);
                tx.commit();
            }
            TxBase tx2 = txMgr.newTx();
            tx2.pin(b1);
            String st2 = tx2.getString(b1, 4);
            assertEquals("Hello there 99", st2);
            tx2.commit();
        };

        Runnable task2 = () -> {
            for (int i = 0; i < 100; i++) {
                TxBase tx = txMgr.newTx();
                tx.pin(b2);
                tx.getInt(b2, 100);
                tx.getDouble(b2, 108);
                tx.getString(b2, 4);
                tx.getBoolean(b2, 105);

                tx.setString(b2, 4, "Hello there "+i, true);
                tx.setInt(b2, 100, i, true);
                tx.setBoolean(b2, 105, true, true);
                tx.setDouble(b2, 108, 345.87, true);
                tx.commit();
            }
            TxBase tx2 = txMgr.newTx();
            tx2.pin(b2);
            String st2 = tx2.getString(b2, 4);
            assertEquals("Hello there 99", st2);
            tx2.commit();
        };

        Runnable task3 = () -> {
            for (int i = 0; i < 100; i++) {
                TxBase tx = txMgr.newTx();
                tx.pin(b3);
                tx.getInt(b3, 100);
                tx.getDouble(b3, 108);
                tx.getString(b3, 4);
                tx.getBoolean(b3, 105);

                tx.setString(b3, 4, "Hello there " + i, true);
                tx.setInt(b3, 100, i, true);
                tx.setBoolean(b3, 105, true, true);
                tx.setDouble(b3, 108, 345.87, true);
                tx.commit();
            }
            TxBase tx2 = txMgr.newTx();
            tx2.pin(b3);
            String st2 = tx2.getString(b3, 4);
            assertEquals("Hello there 99", st2);
            tx2.commit();
        };

        Runnable task4 = () -> {
            for (int i = 0; i < 100; i++) {
                TxBase tx = txMgr.newTx();
                tx.pin(b4);
                tx.getInt(b4, 100);
                tx.getDouble(b4, 108);
                tx.getString(b4, 4);
                tx.getBoolean(b4, 105);

                tx.setString(b4, 4, "Hello there " + i, true);
                tx.setInt(b4, 100, i, true);
                tx.setBoolean(b4, 105, true, true);
                tx.setDouble(b4, 108, 345.87, true);
                tx.commit();
            }
            TxBase tx2 = txMgr.newTx();
            tx2.pin(b4);
            String st2 = tx2.getString(b4, 4);
            assertEquals("Hello there 99", st2);
            tx2.commit();
        };

        Runnable task5 = () -> {
            for (int i = 0; i < 100; i++) {
                TxBase tx = txMgr.newTx();
                tx.pin(b5);
                tx.getInt(b5, 100);
                tx.getDouble(b5, 108);
                tx.getString(b5, 4);
                tx.getBoolean(b5, 105);

                tx.setString(b5, 4, "Hello there " + i, true);
                tx.setInt(b5, 100, i, true);
                tx.setBoolean(b5, 105, true, true);
                tx.setDouble(b5, 108, 345.87, true);
                tx.commit();
            }
            TxBase tx2 = txMgr.newTx();
            tx2.pin(b5);
            String st2 = tx2.getString(b5, 4);
            assertEquals("Hello there 99", st2);
            tx2.commit();
        };

        Runnable task6 = () -> {
            for (int i = 0; i < 100; i++) {
                TxBase tx = txMgr.newTx();
                tx.pin(b6);
                tx.getInt(b6, 100);
                tx.getDouble(b6, 108);
                tx.getString(b6, 4);
                tx.getBoolean(b6, 105);

                tx.setString(b6, 4, "Hello there " + i, true);
                tx.setInt(b6, 100, i, true);
                tx.setBoolean(b6, 105, true, true);
                tx.setDouble(b6, 108, 345.87, true);
                tx.commit();
            }
            TxBase tx2 = txMgr.newTx();
            tx2.pin(b6);
            String st2 = tx2.getString(b6, 4);
            assertEquals("Hello there 99", st2);
            tx2.commit();
        };

        Runnable task7 = () -> {
            for (int i = 0; i < 100; i++) {
                TxBase tx = txMgr.newTx();
                tx.pin(b7);
                tx.getInt(b7, 100);
                tx.getDouble(b7, 108);
                tx.getString(b7, 4);
                tx.getBoolean(b7, 105);

                tx.setString(b7, 4, "Hello there " + i, true);
                tx.setInt(b7, 100, i, true);
                tx.setBoolean(b7, 105, true, true);
                tx.setDouble(b7, 108, 345.87, true);
                tx.commit();
            }
            TxBase tx2 = txMgr.newTx();
            tx2.pin(b7);
            String st2 = tx2.getString(b7, 4);
            assertEquals("Hello there 99", st2);
            tx2.commit();
        };

        Runnable task8 = () -> {
            for (int i = 0; i < 100; i++) {
                TxBase tx = txMgr.newTx();
                tx.pin(b8);
                tx.getInt(b8, 100);
                tx.getDouble(b8, 108);
                tx.getString(b8, 4);
                tx.getBoolean(b8, 105);

                tx.setString(b8, 4, "Hello there " + i, true);
                tx.setInt(b8, 100, i, true);
                tx.setBoolean(b8, 105, true, true);
                tx.setDouble(b8, 108, 345.87, true);
                tx.commit();
            }
            TxBase tx2 = txMgr.newTx();
            tx2.pin(b8);
            String st2 = tx2.getString(b8, 4);
            assertEquals("Hello there 99", st2);
            tx2.commit();
        };

        Runnable task9 = () -> {
            for (int i = 0; i < 100; i++) {
                TxBase tx = txMgr.newTx();
                tx.pin(b9);
                tx.getInt(b9, 100);
                tx.getDouble(b9, 108);
                tx.getString(b9, 4);
                tx.getBoolean(b9, 105);

                tx.setString(b9, 4, "Hello there " + i, true);
                tx.setInt(b9, 100, i, true);
                tx.setBoolean(b9, 105, true, true);
                tx.setDouble(b9, 108, 345.87, true);
                tx.commit();
            }
            TxBase tx2 = txMgr.newTx();
            tx2.pin(b9);
            String st2 = tx2.getString(b9, 4);
            assertEquals("Hello there 99", st2);
            tx2.commit();
        };

        Runnable task10 = () -> {
            for (int i = 0; i < 100; i++) {
                TxBase tx = txMgr.newTx();
                tx.pin(b10);
                tx.getInt(b10, 100);
                tx.getDouble(b10, 108);
                tx.getString(b10, 4);
                tx.getBoolean(b10, 105);

                tx.setString(b10, 4, "Hello there " + i, true);
                tx.setInt(b10, 100, i, true);
                tx.setBoolean(b10, 105, true, true);
                tx.setDouble(b10, 108, 345.87, true);
                tx.commit();
            }
            TxBase tx2 = txMgr.newTx();
            tx2.pin(b10);
            String st2 = tx2.getString(b10, 4);
            assertEquals("Hello there 99", st2);
            tx2.commit();
        };

        Thread thread1 = new Thread(task1);
        Thread thread2 = new Thread(task2);
        Thread thread3 = new Thread(task3);
        Thread thread4 = new Thread(task4);
        Thread thread5 = new Thread(task5);
        Thread thread6 = new Thread(task6);
        Thread thread7 = new Thread(task7);
        Thread thread8 = new Thread(task8);
        Thread thread9 = new Thread(task9);
        Thread thread10 = new Thread(task10);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

        executor.execute(thread1);
        executor.execute(thread2);
        executor.execute(thread3);
        executor.execute(thread4);
        executor.execute(thread5);
        executor.execute(thread6);
        executor.execute(thread7);
        executor.execute(thread8);
        executor.execute(thread9);
        executor.execute(thread10);
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        Iterator<byte[]> logs = logManager.iterator();
        int total = 0;
        while(logs.hasNext()){
            byte[] log = logs.next();
            total++;
        }
        assertEquals(6022, total);

    }

    @Test
    public void testRollbackRestoresOriginalValue() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");
        BufferMgrBase buffeMgr = new BufferMgr(fm, logManager, 10, 500);

        TxMgrBase txMgr = new TxMgr(fm, logManager, buffeMgr, 500);

        assertEquals(500, txMgr.getMaxWaitTimeInMillis());

        TxBase tx1 = txMgr.newTx();
        assertEquals(TxBase.Status.ACTIVE, tx1.getStatus());
        assertEquals(1, tx1.txnum());
        assertEquals(10, tx1.availableBuffs());
        assertEquals(400, tx1.blockSize());
        for(int i = 0; i < 20; i++){
            tx1.append("testfile1");
        }
        tx1.pin(b1);
        tx1.setString(b1, 4, "Hello there 1", false);
        tx1.setInt(b1, 100, 613, false);
        tx1.setBoolean(b1, 105, true, false);
        tx1.setDouble(b1, 108, 345.87, false);
        byte[] ba = new byte[8];
        for(int i = 0; i < 8; i++){
            ba[i] = (byte)i;
        }
        tx1.setBytes(b1, 200, ba, false);
        tx1.commit();
        assertEquals(TxBase.Status.COMMITTED, tx1.getStatus());
        // Step 1: Initialize the block with a value of 0
        tx1 = txMgr.newTx();
        BlockId blk = new BlockId("testfile", 0);
        tx1.pin(blk);
        // We explicitly set the value to 0 at offset 0
        tx1.setInt(blk, 0, 0, true);
        tx1.unpin(blk);
        tx1.commit();

        // Step 2: Create a new transaction that modifies the value
        TxBase tx2 = txMgr.newTx();
        tx2.pin(blk);

        // Verify start state
        int valBefore = tx2.getInt(blk, 0);
        assertEquals(0, valBefore, "Pre-check: Value should be 0 before modification");

        System.out.println("Tx2: Modifying value from 0 to 100");
        tx2.setInt(blk, 0, 100, true);

        // Verify modification happened in memory
        int valModified = tx2.getInt(blk, 0);
        assertEquals(100, valModified, "Pre-check: Value should be 100 after modification inside Tx2");

        tx2.unpin(blk);

        // Step 3: Rollback the transaction
        // This should trigger the LogRecord undo() logic
        System.out.println("Tx2: Rolling back");
        tx2.rollback();

        // Step 4: Verify the data was restored
        TxBase tx3 = txMgr.newTx();
        tx3.pin(blk);
        int valFinal = tx3.getInt(blk, 0);
        tx3.unpin(blk);
        tx3.commit();

        // THIS IS WHERE THE LOGS SHOWED FAILURE
        // Expected: 0 (Original value)
        // Actual (Likely): 100 (Modified value, meaning undo failed)
        System.out.println("Tx3 Read: " + valFinal);
        assertEquals(0, valFinal, "FAILURE REPRODUCED: After rollback, value should return to 0, but found " + valFinal);
    }

    @Test
    public void transparentUnpinAtTxCloseTest(){
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");
        BufferMgrBase buffeMgr = new BufferMgr(fm, logManager, 2, 500);

        TxMgrBase txMgr = new TxMgr(fm, logManager, buffeMgr, 500);

        assertEquals(2, buffeMgr.available());

        TxBase tx = txMgr.newTx();
        tx.pin(b1);
        tx.pin(b1);
        tx.pin(b2);
        tx.setInt(b1, 100, 614, false);
        tx.setInt(b2, 105, 456, false);
        tx.commit();
        assertEquals(2, buffeMgr.available());
        TxBase tx2 = txMgr.newTx();
        tx2.pin(b1);
        tx2.pin(b2);
        int i1 = tx2.getInt(b1, 100);
        assertEquals(614, i1);
        int i2 = tx2.getInt(b2, 105);
        assertEquals(456, i2);
        tx2.commit();
    }
}
