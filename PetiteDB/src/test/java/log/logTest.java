package log;

import edu.yu.dbimpl.file.*;
import edu.yu.dbimpl.config.*;
import edu.yu.dbimpl.log.*;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class logTest {
    private BlockIdBase b1;
    private BlockIdBase b2;
    private BlockIdBase b3;
    private PageBase p1;
    private PageBase p2;
    private PageBase p3;

    @BeforeEach
    public  void setup() {
        b1 = new BlockId("testfile1", 0);
        b2 = new BlockId("testfile1", 1);
        b3 = new BlockId("testfile1", 2);
        p1 = new Page(512);
        p2 = new Page(512);
        p3 = new Page(512);
    }

    @Test
    public void simpleDBTest() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        File file = new File("dbDirectory");
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");

        assertEquals(0, fm.length("logfile"));
        Iterator<byte[]> iter0 = logManager.iterator();
        assertFalse(iter0.hasNext());
        assertThrows(NoSuchElementException.class, () -> {iter0.next();});
        for (int i = 0; i < 2001; i++) {
            byte[] b = new byte[8];
            // Store i at b[0]..b[3]
            b[0] = (byte) (i >> 24);
            b[1] = (byte) (i >> 16);
            b[2] = (byte) (i >> 8);
            b[3] = (byte) (i);
            // Store i again at b[4]..b[7]
            b[4] = (byte) (i >> 24);
            b[5] = (byte) (i >> 16);
            b[6] = (byte) (i >> 8);
            b[7] = (byte) (i);
            int r = logManager.append(b);
            assertEquals(i, r);
        }
        Iterator<byte[]> iter = logManager.iterator();
        int count = 2000;
        assertTrue(iter.hasNext());
        while(iter.hasNext()) {
            byte[] b = iter.next();
            int first = ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
            int second = ((b[4] & 0xFF) << 24) | ((b[5] & 0xFF) << 16) | ((b[6] & 0xFF) << 8) | (b[7] & 0xFF);
            assertEquals(count, first);
            assertEquals(count, second);
            count--;
        }
        assertEquals(-1, count);
        assertFalse(iter.hasNext());

        for (File f : file.listFiles()) {
            f.delete();
        }
        file.delete();
    }

    @Test
    public void threadSafeTest(){
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        File file = new File("dbDirectory");
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 4096);
        LogMgrBase logManager = new LogMgr(fm, "logfile");

        Runnable logTask = () -> {
            for (int i = 0; i < 10000; i++) {
                byte[] b = new byte[8];
                // Store i at b[0]..b[3]
                b[0] = (byte) (i >> 24);
                b[1] = (byte) (i >> 16);
                b[2] = (byte) (i >> 8);
                b[3] = (byte) (i);
                // Store i again at b[4]..b[7]
                b[4] = (byte) (i >> 24);
                b[5] = (byte) (i >> 16);
                b[6] = (byte) (i >> 8);
                b[7] = (byte) (i);
                logManager.append(b);
            }
        };

        Thread thread1 = new Thread(logTask);
        Thread thread2 = new Thread(logTask);
        Thread thread3 = new Thread(logTask);
        Thread thread4 = new Thread(logTask);
        Thread thread5 = new Thread(logTask);
        Thread thread6 = new Thread(logTask);
        Thread thread7 = new Thread(logTask);
        Thread thread8 = new Thread(logTask);
        Thread thread9 = new Thread(logTask);
        Thread thread10 = new Thread(logTask);
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
        Iterator<byte[]> iter = logManager.iterator();
        int count = 0;
        while(iter.hasNext()) {
            byte[] b = iter.next();
            count++;
        }
        assertEquals(100000, count);

        for (File f : file.listFiles()) {
            f.delete();
        }
        file.delete();
    }

    @Test
    public void throwTest() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        File file = new File("dbDirectory");
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 40);
        LogMgrBase logManager = new LogMgr(fm, "logfile");

        assertThrows(IllegalArgumentException.class, () -> {logManager.flush(1);});
        byte[] b = new byte[29];
        assertThrows(IllegalArgumentException.class, () -> {logManager.append(b);});

        for (File f : file.listFiles()) {
            f.delete();
        }
        file.delete();
    }

    @Test
    public void simpleDB2Test() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        File file = new File("dbDirectory");
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");

        for (int i = 0; i < 2001; i++) {
            byte[] b = new byte[8];
            // Store i at b[0]..b[3]
            b[0] = (byte) (i >> 24);
            b[1] = (byte) (i >> 16);
            b[2] = (byte) (i >> 8);
            b[3] = (byte) (i);
            // Store i again at b[4]..b[7]
            b[4] = (byte) (i >> 24);
            b[5] = (byte) (i >> 16);
            b[6] = (byte) (i >> 8);
            b[7] = (byte) (i);
            int r = logManager.append(b);
            assertEquals(i, r);
        }
        logManager.flush(2000);

        Properties dbProperties2 = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(false));
        DBConfiguration config2 = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        fm = new FileMgr(file, 400);
        logManager = new LogMgr(fm, "logfile");

        for (int i = 2001; i < 2021; i++) {
            byte[] b = new byte[8];
            // Store i at b[0]..b[3]
            b[0] = (byte) (i >> 24);
            b[1] = (byte) (i >> 16);
            b[2] = (byte) (i >> 8);
            b[3] = (byte) (i);
            // Store i again at b[4]..b[7]
            b[4] = (byte) (i >> 24);
            b[5] = (byte) (i >> 16);
            b[6] = (byte) (i >> 8);
            b[7] = (byte) (i);
            int r = logManager.append(b);
            assertEquals(i, r);
        }

        Iterator<byte[]> iter = logManager.iterator();
        int count = 2020;
        assertTrue(iter.hasNext());
        while(iter.hasNext()) {
            byte[] b = iter.next();
            int first = ((b[0] & 0xFF) << 24) | ((b[1] & 0xFF) << 16) | ((b[2] & 0xFF) << 8) | (b[3] & 0xFF);
            int second = ((b[4] & 0xFF) << 24) | ((b[5] & 0xFF) << 16) | ((b[6] & 0xFF) << 8) | (b[7] & 0xFF);
            assertEquals(count, first);
            assertEquals(count, second);
            count--;
        }
        assertEquals(-1, count);
        assertFalse(iter.hasNext());

        for (File f : file.listFiles()) {
            f.delete();
        }
        file.delete();
    }

    @Test
    public void NoFlushTest() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        File file = new File("dbDirectory");
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");

        assertEquals(0, fm.length("logfile"));
        Iterator<byte[]> iter0 = logManager.iterator();
        assertFalse(iter0.hasNext());
        assertThrows(NoSuchElementException.class, () -> {
            iter0.next();
        });
        assertEquals(0, fm.length("logfile"));
        String s = "Hello, world!";
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        int r = logManager.append(b);
        assertEquals(0, r);
        assertEquals(0, fm.length("logfile"));
        for (File f : file.listFiles()) {
            f.delete();
        }
        file.delete();
    }

    @Test
    public void startUpTest(){
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        File file = new File("dbDirectory");
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");
        String s = "Hello, world!";
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        int r = logManager.append(b);
        assertEquals(0, r);
        assertEquals(0, fm.length("logfile"));
        String s2 = "Hello, world!2";
        byte[] b2 = s2.getBytes(StandardCharsets.UTF_8);
        int r2 = logManager.append(b2);
        assertEquals(1, r2);
        logManager.flush(1);
        assertEquals(1, fm.length("logfile"));

        Iterator<byte[]> iter = logManager.iterator();
        assertEquals("Hello, world!2", new String(iter.next(), StandardCharsets.UTF_8));
        assertEquals("Hello, world!", new String(iter.next(), StandardCharsets.UTF_8));
        assertFalse(iter.hasNext());

        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(false));
        config.setConfiguration(dbProperties);
        fm = new FileMgr(file, 400);
        logManager = new LogMgr(fm, "logfile");

        String s3 = "Hello, world!3";
        byte[] b3 = s3.getBytes(StandardCharsets.UTF_8);
        int r3 = logManager.append(b3);
        assertEquals(2, r3);

        iter = logManager.iterator();
        int count = 0;
        assertTrue(iter.hasNext());
        while(iter.hasNext()) {
            byte[] b4 = iter.next();
            String str = new String(b4, StandardCharsets.UTF_8);
            if(count == 0) {
                assertEquals(s3, str);
            } else if(count == 1) {
                assertEquals(s2, str);
            } else if(count == 2) {
                assertEquals(s, str);
            }
            count++;
        }
        assertFalse(iter.hasNext());

        for (File f : file.listFiles()) {
            f.delete();
        }
        file.delete();
    }
}
