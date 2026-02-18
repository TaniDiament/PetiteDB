package Buffer;

import edu.yu.dbimpl.buffer.*;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.*;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

public class bufferTest {
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
    private BlockIdBase b11;
    private BlockIdBase b12;
    private BlockIdBase b13;
    private BlockIdBase b14;
    private BlockIdBase b15;
    private BlockIdBase b16;
    private BlockIdBase b17;
    private BlockIdBase b18;
    private BlockIdBase b19;
    private BlockIdBase b20;
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
        b11 = new BlockId("testfile1", 10);
        b12 = new BlockId("testfile1", 11);
        b13 = new BlockId("testfile1", 12);
        b14 = new BlockId("testfile1", 13);
        b15 = new BlockId("testfile1", 14);
        b16 = new BlockId("testfile1", 15);
        b17 = new BlockId("testfile1", 16);
        b18 = new BlockId("testfile1", 17);
        b19 = new BlockId("testfile1", 18);
        b20 = new BlockId("testfile1", 19);
    }

    @AfterEach
    public void teardown() {
        for (File f : file.listFiles()) {
            f.delete();
        }
        file.delete();
    }

    @Test
    public void BufferClassTest() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");

       BufferBase buf = new Buffer(fm, logManager);
       assertThrows(IllegalArgumentException.class, () -> {buf.setModified(-2, 2);});
    }

    @Test
    public void BufferTest() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");

        BufferMgrBase manager = new BufferMgr(fm, logManager, 10, 500, BufferMgrBase.EvictionPolicy.CLOCK);
        assertEquals(10, manager.available());

        assertEquals(BufferMgrBase.EvictionPolicy.CLOCK, manager.getEvictionPolicy());

        BufferBase buf = manager.pin(b1);
        BufferBase buf2 = manager.pin(b2);
        BufferBase buf3 = manager.pin(b3);
        BufferBase buf4 = manager.pin(b4);
        BufferBase buf5 = manager.pin(b5);
        BufferBase buf6 = manager.pin(b6);
        BufferBase buf7 = manager.pin(b7);
        BufferBase buf8 = manager.pin(b8);
        BufferBase buf9 = manager.pin(b9);
        BufferBase buf10 = manager.pin(b10);

        assertEquals(0, manager.available());

        PageBase p1 = buf.contents();
        p1.setInt(1, 1);
        buf.setModified(1, 1);
        PageBase p2 = buf2.contents();
        p2.setInt(2, 2);
        buf2.setModified(2, 2);
        manager.unpin(buf2);

        assertEquals(1,  manager.available());
        BufferBase buf11 = manager.pin(b11);
        assertEquals(0, manager.available());

        BufferBase buf12 = manager.pin(b11);
        BufferBase buf14 = manager.pin(b11);
        manager.unpin(buf11);
        assertEquals(0, manager.available());
        manager.unpin(buf12);
        assertEquals(0, manager.available());
        manager.unpin(buf14);
        assertEquals(1, manager.available());
        buf12 = manager.pin(b11);

        manager.flushAll(1);

        PageBase p12 = new Page(400);
        fm.read(b1, p12);
        int i = p12.getInt(1);
        assertEquals(1, i);

        BufferBase buf15 = manager.pin(b6);
        assertEquals(buf15, buf6);

        assertThrows(BufferAbortException.class, () -> {BufferBase buf16 = manager.pin(b12);});

        assertThrows(IllegalArgumentException.class, () -> {manager.unpin(new Buffer(fm, logManager));});

        assertThrows(IllegalArgumentException.class, () -> {manager.unpin(null);});
    }

    @Test
    public void bufferThreadTest() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");

        BufferMgrBase manager = new BufferMgr(fm, logManager, 10, 500, BufferMgrBase.EvictionPolicy.CLOCK);

        final BufferBase[] buf = new BufferBase[20];

        Runnable pinTask1 = () -> {
           buf[0] = manager.pin(b1);
        };
        Runnable pinTask2 = () -> {
            buf[1] = manager.pin(b2);
        };
        Runnable pinTask3 = () -> {
            buf[2] = manager.pin(b3);
        };
        Runnable pinTask4 = () -> {
            buf[3] = manager.pin(b4);
        };
        Runnable pinTask5 = () -> {
            buf[4] = manager.pin(b5);
        };
        Runnable pinTask6 = () -> {
            buf[5] = manager.pin(b6);
        };
        Runnable pinTask7 = () -> {
            buf[6] = manager.pin(b7);
        };
        Runnable pinTask8 = () -> {
            buf[7] = manager.pin(b8);
        };
        Runnable pinTask9 = () -> {
            buf[8] = manager.pin(b9);
        };
        Runnable pinTask10 = () -> {
            buf[9] = manager.pin(b10);
        };

        Thread pinThread1 = new Thread(pinTask1);
        Thread pinThread2 = new Thread(pinTask2);
        Thread pinThread3 = new Thread(pinTask3);
        Thread pinThread4 = new Thread(pinTask4);
        Thread pinThread5 = new Thread(pinTask5);
        Thread pinThread6 = new Thread(pinTask6);
        Thread pinThread7 = new Thread(pinTask7);
        Thread pinThread8 = new Thread(pinTask8);
        Thread pinThread9 = new Thread(pinTask9);
        Thread pinThread10 = new Thread(pinTask10);

        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        executor.execute(pinThread1);
        executor.execute(pinThread2);
        executor.execute(pinThread3);
        executor.execute(pinThread4);
        executor.execute(pinThread5);
        executor.execute(pinThread6);
        executor.execute(pinThread7);
        executor.execute(pinThread8);
        executor.execute(pinThread9);
        executor.execute(pinThread10);
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertThrows(BufferAbortException.class, () -> {BufferBase buf12 = manager.pin(b11);});
    }

    @Test
    public void performanceTest() {
        HashSet<String> wordSet = new HashSet<>();
        while(wordSet.size() < 10000){
            StringBuilder sb = new StringBuilder();
            Random random = new Random();

            for (int i = 0; i < 15; i++) {
                // Generate a random lowercase letter (a-z)
                char randomChar = (char) ('a' + random.nextInt(26));
                sb.append(randomChar);
            }
            wordSet.add(sb.toString());
        }
        List<String> strings = new ArrayList<>(wordSet);
        String[][] stings = new String[10][1000];
        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 1000; j++) {
                stings[i][j] = strings.get(j + (1000*i));
            }
        }
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if (!file.exists()) {
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 400);
        LogMgrBase logManager = new LogMgr(fm, "logfile");

        BufferMgrBase manager = new BufferMgr(fm, logManager, 1000, 500, BufferMgrBase.EvictionPolicy.CLOCK);

        BlockId[] blocks = new BlockId[10000];

        Runnable pinTask1 = () -> {
            for (int i = 0; i < 1000; i++) {
                blocks[i] = new BlockId("testfile1", i);
                BufferBase b = manager.pin(blocks[i]);
                PageBase p = b.contents();
                p.setInt(1, i+1);
                p.setString(20, stings[0][i]);
                b.setModified(1, 1);
                manager.unpin(b);
            }
        };

        Runnable pinTask2 = () -> {
            for (int i = 0; i < 1000; i++) {
                blocks[i+1000] = new BlockId("testfile1", i+1000);
                BufferBase b = manager.pin(blocks[i+1000]);
                PageBase p = b.contents();
                p.setInt(1, i+10001);
                p.setString(20, stings[1][i]);
                b.setModified(1, 1);
                manager.unpin(b);
            }
        };

        Runnable pinTask3 = () -> {
            for (int i = 0; i < 1000; i++) {
                blocks[i+2000] = new BlockId("testfile1", i+2000);
                BufferBase b = manager.pin(blocks[i+2000]);
                PageBase p = b.contents();
                p.setInt(1, i+20001);
                p.setString(20, stings[2][i]);
                b.setModified(1, 1);
                manager.unpin(b);
            }
        };

        Runnable pinTask4 = () -> {
            for (int i = 0; i < 1000; i++) {
                blocks[i+3000] = new BlockId("testfile1", i+3000);
                BufferBase b = manager.pin(blocks[i+3000]);
                PageBase p = b.contents();
                p.setInt(1, i+30001);
                p.setString(20, stings[3][i]);
                b.setModified(1, 1);
                manager.unpin(b);
            }
        };

        Runnable pinTask5 = () -> {
            for (int i = 0; i < 1000; i++) {
                blocks[i+4000] = new BlockId("testfile1", i+4000);
                BufferBase b = manager.pin(blocks[i+4000]);
                PageBase p = b.contents();
                p.setInt(1, i+40001);
                p.setString(20, stings[3][i]);
                b.setModified(1, 1);
                manager.unpin(b);
            }
        };

        Runnable pinTask6 = () -> {
            for (int i = 0; i < 1000; i++) {
                blocks[i+5000] = new BlockId("testfile1", i+5000);
                BufferBase b = manager.pin(blocks[i+5000]);
                PageBase p = b.contents();
                p.setInt(1, i+50001);
                p.setString(20, stings[5][i]);
                b.setModified(1, 1);
                manager.unpin(b);
            }
        };

        Runnable pinTask7 = () -> {
            for (int i = 0; i < 1000; i++) {
                blocks[i+6000] = new BlockId("testfile1", i+6000);
                BufferBase b = manager.pin(blocks[i+6000]);
                PageBase p = b.contents();
                p.setInt(1, i+60001);
                p.setString(20, stings[6][i]);
                b.setModified(1, 1);
                manager.unpin(b);
            }
        };

        Runnable pinTask8 = () -> {
            for (int i = 0; i < 1000; i++) {
                blocks[i+7000] = new BlockId("testfile1", i+7000);
                BufferBase b = manager.pin(blocks[i+7000]);
                PageBase p = b.contents();
                p.setInt(1, i+70001);
                p.setString(20, stings[7][i]);
                b.setModified(1, 1);
                manager.unpin(b);
            }
        };

        Runnable pinTask9 = () -> {
            for (int i = 0; i < 1000; i++) {
                blocks[i+8000] = new BlockId("testfile1", i+8000);
                BufferBase b = manager.pin(blocks[i+8000]);
                PageBase p = b.contents();
                p.setInt(1, i+80001);
                p.setString(20, stings[8][i]);
                b.setModified(1, 1);
                manager.unpin(b);
            }
        };

        Runnable pinTask10 = () -> {
            for (int i = 0; i < 1000; i++) {
                blocks[i+9000] = new BlockId("testfile1", i+9000);
                BufferBase b = manager.pin(blocks[i+9000]);
                PageBase p = b.contents();
                p.setInt(1, i+90001);
                p.setString(20, stings[9][i]);
                b.setModified(1, 1);
                manager.unpin(b);
            }
        };


        Thread pinThread1 = new Thread(pinTask1);
        Thread pinThread2 = new Thread(pinTask2);
        Thread pinThread3 = new Thread(pinTask3);
        Thread pinThread4 = new Thread(pinTask4);
        Thread pinThread5 = new Thread(pinTask5);
        Thread pinThread6 = new Thread(pinTask6);
        Thread pinThread7 = new Thread(pinTask7);
        Thread pinThread8 = new Thread(pinTask8);
        Thread pinThread9 = new Thread(pinTask9);
        Thread pinThread10 = new Thread(pinTask10);

        long time1 = System.currentTimeMillis();

        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        executor.execute(pinThread1);
        executor.execute(pinThread2);
        executor.execute(pinThread3);
        executor.execute(pinThread4);
        executor.execute(pinThread5);
        executor.execute(pinThread6);
        executor.execute(pinThread7);
        executor.execute(pinThread8);
        executor.execute(pinThread9);
        executor.execute(pinThread10);
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        manager.flushAll(1);
        assertEquals(10000, fm.length("testfile1"));

        AtomicInteger trakcer =  new AtomicInteger();

        ThreadPoolExecutor readExecutor = new ThreadPoolExecutor(10, 10, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());


        List<Integer> allBlockIndices = new ArrayList<>();
        for (int i = 0; i < 10000; i++) {
            allBlockIndices.add(i);
        }
        Collections.shuffle(allBlockIndices);

        String[][] stings2 = new String[10][1000];

        for (int threadId = 0; threadId < 10; threadId++) {
            final int startIdx = threadId * 1000;
            final int endIdx = startIdx + 1000;

            int finalThreadId = threadId;
            Runnable readTask = () -> {
                int q = 0;
                for (int j = startIdx; j < endIdx; j++) {
                    int blockNum = allBlockIndices.get(j);
                    BlockId blockToRead = blocks[blockNum];

                    BufferBase buffer = manager.pin(blockToRead);
                    PageBase page = buffer.contents();

                    int intValue = page.getInt(1);
                    String stringValue = page.getString(20);
                    stings2[finalThreadId][q] = stringValue;
                    q++;
                    trakcer.incrementAndGet();
                    manager.unpin(buffer);
                }
            };
            readExecutor.execute(readTask);
        }

        readExecutor.shutdown();
        try {
            readExecutor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        assertEquals(10000, trakcer.get());

        long time2 = System.currentTimeMillis();
        System.out.println("Time was "+(time2-time1) + " milliseconds");

       List<String> strings2 = new ArrayList<>();
       for(int i = 0; i < 10; i++) {
           strings2.addAll((List.of(stings2[i])));
       }

       Collections.sort(strings2);
       Collections.sort(strings);
       for(String s: strings2) {
           assertTrue(strings.contains(s));
       }
    }

    @Test
    public void clockVsNaiveEvictionTest() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if (!file.exists()) {
            file.mkdir();
        }

        // Test with NAIVE policy
        FileMgrBase fmNaive = new FileMgr(file, 1000);
        LogMgrBase logManagerNaive = new LogMgr(fmNaive, "logfile_naive");
        BufferMgrBase naiveManager = new BufferMgr(fmNaive, logManagerNaive, 3, 500, BufferMgrBase.EvictionPolicy.NAIVE);

        // Fill buffer pool with 3 blocks
        BufferBase naiveBuf1 = naiveManager.pin(b1);
        BufferBase naiveBuf2 = naiveManager.pin(b2);
        BufferBase naiveBuf3 = naiveManager.pin(b3);
        naiveManager.unpin(naiveBuf1);
        naiveManager.unpin(naiveBuf2);
        naiveManager.unpin(naiveBuf3);

        long naiveStartTime = System.currentTimeMillis();

        // Repeatedly access the same 2 blocks (b1 and b2) - should cause many disk I/O with NAIVE
        for (int i = 0; i < 100000; i++) {
            BufferBase buf1 = naiveManager.pin(b1);
            PageBase p1 = buf1.contents();
            p1.setInt(1, i);
            buf1.setModified(1, 1);
            naiveManager.unpin(buf1);

            BufferBase buf2 = naiveManager.pin(b2);
            PageBase p2 = buf2.contents();
            p2.setInt(1, i * 2);
            buf2.setModified(1, 1);
            naiveManager.unpin(buf2);
        }

        long naiveEndTime = System.currentTimeMillis();
        long naiveTime = naiveEndTime - naiveStartTime;

        // Clean up
        teardown();
        setup();

        // Test with CLOCK policy
        FileMgrBase fmClock = new FileMgr(file, 1000);
        LogMgrBase logManagerClock = new LogMgr(fmClock, "logfile_clock");
        BufferMgrBase clockManager = new BufferMgr(fmClock, logManagerClock, 3, 500, BufferMgrBase.EvictionPolicy.CLOCK);

        // Fill buffer pool with 3 blocks
        BufferBase clockBuf1 = clockManager.pin(b1);
        BufferBase clockBuf2 = clockManager.pin(b2);
        BufferBase clockBuf3 = clockManager.pin(b3);
        clockManager.unpin(clockBuf1);
        clockManager.unpin(clockBuf2);
        clockManager.unpin(clockBuf3);

        long clockStartTime = System.currentTimeMillis();

        // Repeatedly access the same 2 blocks (b1 and b2) - should stay in cache with CLOCK
        for (int i = 0; i < 100000; i++) {
            BufferBase buf1 = clockManager.pin(b1);
            PageBase p1 = buf1.contents();
            p1.setInt(1, i);
            buf1.setModified(1, 1);
            clockManager.unpin(buf1);

            BufferBase buf2 = clockManager.pin(b2);
            PageBase p2 = buf2.contents();
            p2.setInt(1, i * 2);
            buf2.setModified(1, 1);
            clockManager.unpin(buf2);
        }

        long clockEndTime = System.currentTimeMillis();
        long clockTime = clockEndTime - clockStartTime;

        System.out.println("NAIVE policy time: " + naiveTime + " ms");
        System.out.println("CLOCK policy time: " + clockTime + " ms");
        System.out.println("Performance improvement: " + ((naiveTime - clockTime) * 100.0 / naiveTime) + "%");

        // CLOCK should be faster because it keeps frequently accessed blocks in cache
        assertTrue(clockTime < naiveTime, "CLOCK policy should be faster than NAIVE for repeated access patterns");

        // Verify data integrity
        clockManager.flushAll(1);
        PageBase verifyP1 = new Page(400);
        PageBase verifyP2 = new Page(400);
        fmClock.read(b1, verifyP1);
        fmClock.read(b2, verifyP2);
        assertEquals(99999, verifyP1.getInt(1));
    }

}
