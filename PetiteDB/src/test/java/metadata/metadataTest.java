package metadata;

import edu.yu.dbimpl.buffer.*;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.*;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.metadata.TableMgr;
import edu.yu.dbimpl.metadata.TableMgrBase;
import edu.yu.dbimpl.record.*;
import edu.yu.dbimpl.tx.TxBase;
import edu.yu.dbimpl.tx.TxMgr;
import edu.yu.dbimpl.tx.TxMgrBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Types;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class metadataTest {
    private File file;
    @BeforeEach
    public  void setup() {
        file = new File("dbDirectory1");
    }
    @Test
    public void metadataTest(){
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
        FileMgrBase fm = new FileMgr(file, 1000);
        LogMgrBase logManager = new LogMgr(fm, "logfile");
        BufferMgrBase buffeMgr = new BufferMgr(fm, logManager, 10, 500);

        TxMgrBase txMgr = new TxMgr(fm, logManager, buffeMgr, 500);

        TxBase tx = txMgr.newTx();

        TableMgrBase tableMgr = new TableMgr(tx);
        tx.commit();

        TxBase tx2 = txMgr.newTx();

        tableMgr.createTable("tani", schema, tx2);
        assertThrows(IllegalArgumentException.class, () -> tableMgr.createTable("tani", schema, tx2));
        tx2.commit();

        TxBase tx3 = txMgr.newTx();

        LayoutBase layout2 = tableMgr.getLayout("tani", tx3);
        tx3.commit();

        assertTrue(layout2.equals(layout));

        SchemaBase schema2 = new Schema();
        schema2.addStringField("hi", 44);
        LayoutBase layoutBase = new Layout(schema2);
        tx3 = txMgr.newTx();
        tableMgr.replace("tani", schema2, tx3);
        tx3.commit();
        tx3 = txMgr.newTx();
        LayoutBase layoutBase1 = tableMgr.getLayout("tani", tx3);
        tx3.commit();
        assertTrue(layoutBase1.equals(layoutBase));

        tx3 = txMgr.newTx();
        tableMgr.replace("tani", null, tx3);
        tx3.commit();

        final TxBase tx5 = txMgr.newTx();
        assertNull(tableMgr.getLayout("tani", tx5));
        assertThrows(IllegalArgumentException.class, () -> tableMgr.replace("tani", schema, tx5));
        tx5.commit();

    }

}
