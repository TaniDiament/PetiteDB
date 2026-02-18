package record;

import edu.yu.dbimpl.buffer.*;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.*;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.query.Datum;
import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.record.*;
import edu.yu.dbimpl.tx.TxBase;
import edu.yu.dbimpl.tx.TxMgr;
import edu.yu.dbimpl.tx.TxMgrBase;
import edu.yu.dbimpl.tx.concurrency.LockTable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Types;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class RecordTest {
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
    public void DatumClassTest(){
        DatumBase datumBase = new Datum(5678);
        assertEquals(5678, datumBase.asInt());
        assertEquals(Types.INTEGER, datumBase.getSQLType());

        String st = "hello";
        byte[] bytes = st.getBytes();
        DatumBase db = new Datum(bytes);
        assertEquals("hello", db.asString());

        DatumBase db1 = new Datum(5678);
        assertTrue(db1.equals(datumBase));

        DatumBase db2 = new Datum(6789);
        boolean greater = db2.compareTo(db1) > 0;
        assertTrue(greater);
    }

    @Test
    public void SchemaClassTest(){
        SchemaBase schema = new Schema();
        schema.addField("string1", Types.VARCHAR, 20);
        schema.addIntField("int1");
        schema.addBooleanField("bool1");
        schema.addDoubleField("doub1");

        SchemaBase addF = new Schema();
        addF.addDoubleField("doub2");

        SchemaBase addAll = new Schema();
        addAll.addDoubleField("doub3");
        addAll.addIntField("int2");

        schema.add("doub2", addF);
        schema.addAll(addAll);

        assertTrue(schema.hasField("doub2"));
        assertTrue(schema.hasField("doub1"));
        assertTrue(schema.hasField("doub3"));
        assertTrue(schema.hasField("int2"));

        assertEquals(Types.INTEGER, schema.type("int2"));

        assertEquals(20, schema.length("string1"));
        assertEquals(4, schema.length("int1"));
        assertEquals(8, schema.length("doub1"));

        List<String> fields = schema.fields();
        assertTrue(fields.contains("bool1"));

        assertThrows(IllegalArgumentException.class, () -> schema.length("789"));
        assertThrows(IllegalArgumentException.class, () -> schema.length(""));
        assertThrows(IllegalArgumentException.class, () -> schema.length(null));
    }

    @Test
    public void LayoutClassTest(){
        SchemaBase schema = new Schema();
        schema.addField("string1", Types.VARCHAR, 20);//24
        schema.addIntField("int1");//4
        schema.addBooleanField("bool1");//1
        schema.addDoubleField("doub1");//8

        SchemaBase addF = new Schema();
        addF.addDoubleField("doub2");//8

        SchemaBase addAll = new Schema();
        addAll.addDoubleField("doub3");//8
        addAll.addIntField("int2");//4

        schema.add("doub2", addF);
        schema.addAll(addAll);

        LayoutBase layout = new Layout(schema);

        assertEquals(1, layout.offset("string1"));
        assertEquals(25, layout.offset("int1"));
        assertEquals(58, layout.slotSize());
        assertEquals(54, layout.offset("int2"));
    }

    @Test
    public void RecordPageTest(){
        SchemaBase schema = new Schema();
        schema.addField("string1", Types.VARCHAR, 20);//24
        schema.addIntField("int1");//4
        schema.addBooleanField("bool1");//1
        schema.addDoubleField("doub1");//8

        SchemaBase addF = new Schema();
        addF.addDoubleField("doub2");//8

        SchemaBase addAll = new Schema();
        addAll.addDoubleField("doub3");//8
        addAll.addIntField("int2");//4

        schema.add("doub2", addF);
        schema.addAll(addAll);

        LayoutBase layout = new Layout(schema);//58 size

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

        TxBase tx = txMgr.newTx();

        RecordPageBase recordPage = new RecordPage(tx, b1, layout);
        assertEquals(-1, recordPage.nextAfter(2));

        for(int i = -1; i < 5; i++){
            assertEquals(i+1, recordPage.insertAfter(i));
        }
        assertEquals(-1, recordPage.insertAfter(4));

        assertEquals(b1, recordPage.block());
        assertEquals(-1, recordPage.nextAfter(5));
        assertEquals(2, recordPage.nextAfter(1));

        recordPage.delete(4);
        assertEquals(5, recordPage.nextAfter(3));

        assertEquals(4, recordPage.insertAfter(0));

        assertThrows(IllegalArgumentException.class, () -> recordPage.setString(3, "string1", "wifwef iwf iwf i fiwfiwe iewfnw ifnewfiewnf ewifnw finwfiewnf ewif"));
        assertThrows(IllegalArgumentException.class, () -> recordPage.setString(8, "string1", "hi"));
        assertThrows(IllegalArgumentException.class, () -> recordPage.setString(3, "string2", "hi"));
        assertThrows(IllegalArgumentException.class, () -> recordPage.setString(2, "bool1", "hi"));

        //set values
        for(int i = 0; i < 6; i++){
            recordPage.setString(i, "string1", "abc");
            recordPage.setInt(i, "int1", 456);
            recordPage.setBoolean(i, "bool1", true);
            recordPage.setDouble(i, "doub3", 7.89);
        }

        //get values
        for(int i = 0; i < 6; i++){
            assertEquals("abc", recordPage.getString(i, "string1"));
            assertEquals(456, recordPage.getInt(i, "int1"));
            assertTrue(recordPage.getBoolean(i, "bool1"));
            assertEquals(7.89, recordPage.getDouble(i, "doub3"));
        }

        recordPage.delete(4);
        assertThrows(IllegalStateException.class, () -> recordPage.getInt(4, "int1"));
    }

    @Test
    public void TableScanClassTest() {
        SchemaBase schema = new Schema();
        schema.addField("string1", Types.VARCHAR, 20);//24
        schema.addIntField("int1");//4
        schema.addBooleanField("bool1");//1
        schema.addDoubleField("doub1");//8

        SchemaBase addF = new Schema();
        addF.addDoubleField("doub2");//8

        SchemaBase addAll = new Schema();
        addAll.addDoubleField("doub3");//8
        addAll.addIntField("int2");//4

        schema.add("doub2", addF);
        schema.addAll(addAll);

        LayoutBase layout = new Layout(schema);//58 size

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

        TxBase tx = txMgr.newTx();

        //Test filename method
        TableScanBase tableScan = new TableScan(tx, "tani_Table", layout);
        assertEquals("tani_Table_data.tbl", tableScan.getTableFileName());

        //Test RID update on inserts
        for(int i= 0; i < 18; i++){
            tableScan.insert();
        }
        RID rid = tableScan.getRid();
        assertEquals(5, rid.slot());
        assertEquals(2, rid.blockNumber());

        //Test move to RID works
        RID move = new RID(1, 4);
        tableScan.moveToRid(move);

        assertTrue(tableScan.next());

        RID rid2 = tableScan.getRid();
        assertEquals(5, rid2.slot());
        assertEquals(1, rid2.blockNumber());

        //test buffers
        assertEquals(9, buffeMgr.available());

        //Test before first
        tableScan.beforeFirst();
        assertTrue(tableScan.next());

        RID rid3 = tableScan.getRid();
        assertEquals(0, rid3.slot());
        assertEquals(0, rid3.blockNumber());

        //Test hasfield and type
        assertTrue(tableScan.hasField("bool1"));
        assertEquals(Types.BOOLEAN, tableScan.getType("bool1"));

        //Test delete and next
        RID rid4 = new RID(2, 2);
        tableScan.moveToRid(rid4);
        tableScan.delete();
        assertThrows(IllegalStateException.class, () -> tableScan.setBoolean("bool1", true));

        RID rid5 = new RID(2, 1);
        tableScan.moveToRid(rid5);
        tableScan.insert();

        RID rid6 = tableScan.getRid();
        assertEquals(2, rid6.blockNumber());
        assertEquals(2, rid6.slot());

        tableScan.setString("string1", "abc");
        tableScan.setInt("int1", 42);
        tableScan.setDouble("doub1", 42.42);
        tableScan.setBoolean("bool1", true);
        tableScan.setDouble("doub2", 43.43);
        tableScan.setDouble("doub3", 44.44);
        tableScan.setInt("int2", 613);


        //test buffers
        assertEquals(9, buffeMgr.available());
        tableScan.close();
        tx.commit();
        TxBase TX2 = txMgr.newTx();

        TableScanBase tableScanBase2 = new TableScan(TX2, "tani_Table", layout);

        tableScanBase2.moveToRid(rid6);
        DatumBase db = tableScanBase2.getVal("bool1");
        assertTrue(db.asBoolean());
        assertTrue(tableScanBase2.getBoolean("bool1"));
        assertEquals("abc", tableScanBase2.getString("string1"));
        assertEquals(42, tableScanBase2.getInt("int1"));
        assertEquals(613, tableScanBase2.getInt("int2"));
        assertEquals(42.42, tableScanBase2.getDouble("doub1"));
        assertEquals(43.43, tableScanBase2.getDouble("doub2"));
        assertEquals(44.44, tableScanBase2.getDouble("doub3"));
        tableScanBase2.close();
        assertEquals(10, buffeMgr.available());
        TX2.commit();
    }

    @Test
    public void performanceTest(){
        SchemaBase schema = new Schema();
        schema.addField("string1", Types.VARCHAR, 20);//24
        schema.addIntField("int1");//4
        schema.addBooleanField("bool1");//1
        schema.addDoubleField("doub1");//8

        SchemaBase addF = new Schema();
        addF.addDoubleField("doub2");//8

        SchemaBase addAll = new Schema();
        addAll.addDoubleField("doub3");//8
        addAll.addIntField("int2");//4

        schema.add("doub2", addF);
        schema.addAll(addAll);

        LayoutBase layout = new Layout(schema);//58 size

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
        TxBase tx = txMgr.newTx();

        //Test filename method
        TableScanBase tableScan = new TableScan(tx, "tani_Table", layout);
        assertEquals("tani_Table_data.tbl", tableScan.getTableFileName());

        //Test RID update on inserts
        for(int i= 0; i < 36; i++){
            tableScan.insert();
        }
        tableScan.close();
        tx.commit();
        assertEquals(6, fm.length("tani_Table_data.tbl"));

        ExecutorService executor = Executors.newFixedThreadPool(6);
        CountDownLatch latch = new CountDownLatch(6);

        for (int blockNum = 0; blockNum < 6; blockNum++) {
            final int block = blockNum;
            executor.submit(() -> {
                try {
                    TxBase tx2 = txMgr.newTx();
                    TableScanBase tableScan2 = new TableScan(tx, "perf_Table", layout);

                    RID rid = new RID(block, 0);
                    tableScan2.moveToRid(rid);

                    // Write values
                    tableScan2.setString("string1", "block" + block);
                    tableScan2.setInt("int1", block * 100);
                    tableScan2.setBoolean("bool1", block % 2 == 0);
                    tableScan2.setDouble("doub1", block * 1.1);
                    tableScan2.setDouble("doub2", block * 2.2);
                    tableScan2.setDouble("doub3", block * 3.3);
                    tableScan2.setInt("int2", block * 10);

                    // Read and verify values
                    tableScan2.moveToRid(rid);
                    assertEquals("block" + block, tableScan2.getString("string1"));
                    assertEquals(block * 100, tableScan2.getInt("int1"));
                    assertEquals(block % 2 == 0, tableScan2.getBoolean("bool1"));
                    assertEquals(block * 1.1, tableScan2.getDouble("doub1"), 0.001);
                    assertEquals(block * 2.2, tableScan2.getDouble("doub2"), 0.001);
                    assertEquals(block * 3.3, tableScan2.getDouble("doub3"), 0.001);
                    assertEquals(block * 10, tableScan2.getInt("int2"));

                    tableScan2.close();
                    tx.commit();
                } finally {
                    latch.countDown();
                }
            });
        }

        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        executor.shutdown();
        try {
            executor.awaitTermination(300, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
