package file;

import edu.yu.dbimpl.file.*;
import edu.yu.dbimpl.config.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;


public class fileTest {

    private BlockIdBase b1;
    private BlockIdBase b2;
    private BlockIdBase b3;
    private PageBase p1;
    private PageBase p2;
    private PageBase p3;
    private File file;

    @BeforeEach
    public  void setup() {
        b1 = new BlockId("testfile1", 0);
        b2 = new BlockId("testfile1", 1);
        b3 = new BlockId("testfile1", 2);
        p1 = new Page(512);
        p2 = new Page(512);
        p3 = new Page(512);
        file = new File("dbDirectory");
    }
    @AfterEach
    public void teardown() {
        if(file.exists()) {
            for (File f : file.listFiles()) {
                f.delete();
            }
            file.delete();
        }
    }
    @Test
    public void simpleDBTest(){
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if(!file.exists()){
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 512);
        p1.setString(0, "Hello, World!");
        p1.setInt(400, 42);
        fm.write(b1, p1);

        PageBase pRead = new Page(512);
        fm.read(b1, pRead);
        String str = pRead.getString(0);
        int num = pRead.getInt(400);
        assertEquals("Hello, World!", str);
        assertEquals(42, num);
        // Clean up test file
        File testFile = new File("C:\\Users\\tani\\IdeaProjects\\Yonasan_Diament_800509867" +
                "\\DatabaseImplementation\\assignments\\PetiteDB\\dbDirectory\\testfile1");
    }

    @Test
    public void noDirectoryYetDBTest(){
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        File file = new File("dbDirectory1");
        FileMgrBase fm = new FileMgr(file, 512);
        p1.setString(0, "Hello, World!");
        p1.setInt(400, 42);
        fm.write(b1, p1);

        PageBase pRead = new Page(512);
        fm.read(b1, pRead);
        String str = pRead.getString(0);
        int num = pRead.getInt(400);
        assertEquals("Hello, World!", str);
        assertEquals(42, num);
        // Clean up test file
        File testFile = new File("C:\\Users\\tani\\IdeaProjects\\Yonasan_Diament_800509867" +
                "\\DatabaseImplementation\\assignments\\PetiteDB\\dbDirectory1\\testfile1");
        File testFile2 = new File("C:\\Users\\tani\\IdeaProjects\\Yonasan_Diament_800509867" +
                "\\DatabaseImplementation\\assignments\\PetiteDB\\dbDirectory1");
        if (testFile.exists()) {
            testFile.delete();
        }
        if (testFile2.exists()) {
            testFile2.delete();
        }
        file.delete();
    }

    @Test
    public void fileManagerThrowTest(){
        //Test if properties is not there throw exception
        assertThrows(IllegalStateException.class, ()->{FileMgrBase fm = new FileMgr(new File("dbDirectory1"), 512);});
    }

    @Test
    public void blockIdTest(){
        assertThrows(IllegalArgumentException.class, ()->{BlockIdBase blockId = new BlockId("testfile1", -1);});
        assertThrows(IllegalArgumentException.class, ()->{BlockIdBase blockId = new BlockId("", 0);});
        assertThrows(IllegalArgumentException.class, ()->{BlockIdBase blockId = new BlockId(null, 0);});
    }

    @Test
    public void pageSimpleTest(){
        for(int i = 0; i < 128; i++) {
            p1.setInt(i * 4, i);
        }
        for(int i = 0; i < 128; i++) {
            assertEquals(i, p1.getInt(i * 4));
        }
        for(int i = 0; i < 64; i++) {
            p1.setDouble(i * 8, (double)i + 0.5);
        }
        for(int i = 0; i < 64; i++) {
            assertEquals((double)i + 0.5, p1.getDouble(i * 8));
        }
        for (int i = 0; i < 512; i++) {
            p1.setBoolean(i, i % 2 == 0);
        }
        for (int i = 0; i < 512; i++) {
            assertEquals(i % 2 == 0, p1.getBoolean(i));
        }
        byte[] byteArray = new byte[508];
        for (int i = 0; i < 508; i++) {
            byteArray[i] = (byte) i;
        }
        p1.setBytes(0, byteArray);
        byte[] retrievedArray = p1.getBytes(0);
        assertArrayEquals(byteArray, retrievedArray);

        p1.setString(0, "Hello, World!");
        assertEquals("Hello, World!", p1.getString(0));
        p1.setString(100, "Another String");
        assertEquals("Another String", p1.getString(100));
        p1.setInt(85, 46);
        String longString = "This string is definitely way too long to fit in the remaining space of the page.";
        p1.setString(0, longString);
        assertEquals(longString, p1.getString(0));
        assertEquals("Another String", p1.getString(100));
        assertEquals(46, p1.getInt(85));


        assertThrows(IllegalArgumentException.class, ()->{p1.getInt(-1);});
        assertThrows(IllegalArgumentException.class, ()->{p1.getInt(509);});
        assertThrows(IllegalArgumentException.class, ()->{p1.setInt(-1, 5);});
        assertThrows(IllegalArgumentException.class, ()->{p1.setInt(509, 5);});
        assertThrows(IllegalArgumentException.class, ()->{p1.getDouble(-1);});
        assertThrows(IllegalArgumentException.class, ()->{p1.getDouble(505);});
        assertThrows(IllegalArgumentException.class, ()->{p1.setDouble(-1, 5.0);});
        assertThrows(IllegalArgumentException.class, ()->{p1.setDouble(505, 5.0);});
        assertThrows(IllegalArgumentException.class, ()->{p1.getBoolean(-1);});
        assertThrows(IllegalArgumentException.class, ()->{p1.getBoolean(512);});
        assertThrows(IllegalArgumentException.class, ()->{p1.setBoolean(-1, true);});
        assertThrows(IllegalArgumentException.class, ()->{p1.setBoolean(512, true);});
        assertThrows(IllegalArgumentException.class, ()->{p1.getBytes(-1);});
        assertThrows(IllegalArgumentException.class, ()->{p1.setString(-1, "test");});
        assertThrows(IllegalArgumentException.class, ()->{p1.setString(509, "test oveflow bad");});
    }

    @Test
    public void performanceTest(){
        long startTime = System.currentTimeMillis();

        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if(!file.exists()){
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 4096);

        PageBase p1 = new Page(fm.blockSize());
        byte[] byteArray = new byte[4092];
        for (int i = 0; i < 4092; i++) {
            byteArray[i] = (byte) (i % 256);
        }
        p1.setBytes(0, byteArray);

        for(int i= 0; i < 4000; i++){
            BlockIdBase b = new BlockId("perfTestFile", i);
            fm.write(b, p1);
        }

        for(int i= 0; i < 4000; i++){
            BlockIdBase b = new BlockId("perfTestFile", i);
            PageBase pRead = new Page(fm.blockSize());
            fm.read(b, pRead);
            byte[] retrievedArray = pRead.getBytes(0);
            assertArrayEquals(byteArray, retrievedArray);
        }

        long endTime = System.currentTimeMillis();
        long duration = endTime - startTime;
        System.out.println("Performance Test Duration: " + duration + " milliseconds");
    }

    @Test
    public void writeTest(){
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if(!file.exists()){
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 512);
        fm.append("newFile");
        assertEquals(1, fm.length("newFile"));
        PageBase p1 = new Page(fm.blockSize());
        p1.setString(0, "Hello, World!");
        BlockIdBase b = new BlockId("newFile", 1);
        fm.write(b, p1);
        assertEquals(2, fm.length("newFile"));
    }

//    @Test
//    public void performanceTest2(){
//        long startTime = System.currentTimeMillis();
//
//        Properties dbProperties = new Properties();
//        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
//        DBConfiguration config = DBConfiguration.INSTANCE;
//        config.setConfiguration(dbProperties);
//        if(!file.exists()){
//            file.mkdir();
//        }
//        FileMgrBase fm = new FileMgr(file, 512);
//
//        PageBase p1 = new Page(fm.blockSize());
//        p1.setString(88, "hello world");
//
//        for(int i= 0; i < 30000; i++){
//            BlockIdBase b = new BlockId("perfTestFile"+i, 0);
//            fm.write(b, p1);
//        }
//
//        for(int i= 0; i < 30000; i++){
//            BlockIdBase b = new BlockId("perfTestFile"+i, 0);
//            PageBase pRead = new Page(fm.blockSize());
//            fm.read(b, pRead);
//            assertEquals("hello world", pRead.getString(88));
//        }
//
//        long endTime = System.currentTimeMillis();
//        long duration = endTime - startTime;
//        System.out.println("Performance Test Duration: " + duration + " milliseconds");
//    }

    @Test
    public void FileLengthTest() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if(!file.exists()){
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 512);
        assertEquals(0, fm.length("newFile"));
        p1.setString(0, "Hello, World!");
        p1.setInt(400, 42);
        fm.write(new BlockId("newFile", 0), p1);
        assertEquals(1, fm.length("newFile"));
        fm.write(new BlockId("newFile", 1), p1);
        assertEquals(2, fm.length("newFile"));
        fm.write(new BlockId("newFile", 2), p1);
        assertEquals(3, fm.length("newFile"));
        BlockIdBase blk1 = fm.append("newFile");
        assertEquals(4, fm.length("newFile"));
        assertEquals(3, blk1.number());

        fm.append("newFile2");
        BlockIdBase blk = fm.append("newFile2");
        assertEquals(2, fm.length("newFile2"));
        assertEquals(1, blk.number());
    }

    @Test
    public void FileLengthTestRead() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if(!file.exists()){
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 512);
        assertEquals(0, fm.length("newFile"));
        b1 = new BlockId("newFile", 17);
        PageBase p1 = new Page(fm.blockSize());
        fm.read(b1, p1);
    }

    @Test
    public void FileLengthTestWrite() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if(!file.exists()){
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 512);
        assertEquals(0, fm.length("newFile"));
        b1 = new BlockId("newFile", 17);
        PageBase p1 = new Page(fm.blockSize());
        fm.write(b1, p1);
    }

    @Test
    public void ThreadSafeTest() {
        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if(!file.exists()){
            file.mkdir();
        }
        FileMgrBase fm = new FileMgr(file, 512);

        Runnable writeTask = () -> {
            for (int i = 0; i < 100; i++) {
                BlockIdBase b = new BlockId("threadTestFile", i);
                PageBase p = new Page(fm.blockSize());
                p.setInt(0, i);
                fm.write(b, p);
            }
        };

        Thread writer1 = new Thread(writeTask);
        Thread writer2 = new Thread(writeTask);
        ThreadPoolExecutor executor = new ThreadPoolExecutor(4, 4, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        executor.execute(writer1);
        executor.execute(writer2);
        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        BlockIdBase b = new BlockId("threadTestFile", 99);
        PageBase pRead = new Page(fm.blockSize());
        fm.read(b, pRead);
        int value = pRead.getInt(0);
        assertEquals(99, value);
    }

    @Test
    public void blockTest(){
        assertThrows(IllegalArgumentException.class, ()->{BlockIdBase blockId = new BlockId("testfile1", -1);});
        assertThrows(IllegalArgumentException.class, ()->{BlockIdBase blockId = new BlockId("   ", 0);});
        assertThrows(IllegalArgumentException.class, ()->{BlockIdBase blockId = new BlockId(null, 0);});
    }

}

