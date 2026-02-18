package index;

import edu.yu.dbimpl.buffer.*;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.*;
import edu.yu.dbimpl.index.IndexBase;
import edu.yu.dbimpl.index.IndexDescriptorBase;
import edu.yu.dbimpl.index.IndexMgr;
import edu.yu.dbimpl.index.IndexMgrBase;
import edu.yu.dbimpl.log.LogMgr;
import edu.yu.dbimpl.log.LogMgrBase;
import edu.yu.dbimpl.metadata.TableMgr;
import edu.yu.dbimpl.metadata.TableMgrBase;
import edu.yu.dbimpl.query.Datum;
import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.record.*;
import edu.yu.dbimpl.tx.TxBase;
import edu.yu.dbimpl.tx.TxMgr;
import edu.yu.dbimpl.tx.TxMgrBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Types;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class indexTest {

    static final String INDEX_METADATA_FILE = "indexMetaFile";
    static final String TABLE_NAME_FIELD = "tableName";
    static final String INDEX_ID_FIELD = "indexId";
    static final String INDEX_NAME_FIELD = "indexName";
    static final String RID_BLOCK_ID_FIELD = "RIDBlock";
    static final String RID_SLOT_FIELD = "RIDSlot";
    private File file;
    private SchemaBase schema;
    private LayoutBase layout;
    private FileMgrBase fm;
    private LogMgrBase logManager;
    private BufferMgrBase buffeMgr;
    private TxMgrBase txMgr;
    private TableMgrBase tableMgr;

    @BeforeEach
    public  void setup() {
        file = new File("dbDirectory1");
        schema = new Schema();
        schema.addField("Name", Types.VARCHAR, 20);//24
        schema.addIntField("Age");//4
        schema.addBooleanField("Man");//1
        schema.addDoubleField("NetWorth");//8
        schema.addStringField("Description", 600);//604

        layout = new Layout(schema);//641 size

        Properties dbProperties = new Properties();
        dbProperties.put(DBConfiguration.DB_STARTUP, Boolean.toString(true));
        dbProperties.put(DBConfiguration.N_STATIC_HASH_BUCKETS, Integer.toString(10));
        DBConfiguration config = DBConfiguration.INSTANCE;
        config.setConfiguration(dbProperties);
        if (!file.exists()) {
            file.mkdir();
        }
        fm = new FileMgr(file, 2048);
        logManager = new LogMgr(fm, "logfile");
        buffeMgr = new BufferMgr(fm, logManager, 10, 500);
        txMgr = new TxMgr(fm, logManager, buffeMgr, 500);

        TxBase tx = txMgr.newTx();

        tableMgr = new TableMgr(tx);
        tx.commit();

        TxBase tx2 = txMgr.newTx();

        tableMgr.createTable("tani", schema, tx2);
        tx2.commit();
    }

    @AfterEach
    public void tearDown() {
        if(file.isDirectory()) {
            for(File file : file.listFiles()) {
                file.delete();
            }
        }
        file.delete();
    }

    /**
     * Helper method to populate the table with 50 different people.
     * Uses the schema defined in @BeforeEach:
     * - Name: VARCHAR(20) // 24 bytes
     * - Age: INTEGER // 4 bytes
     * - Man: BOOLEAN // 1 byte
     * - NetWorth: DOUBLE // 8 bytes
     * - Description: VARCHAR(600) // 604 bytes
     */
    private void populateTableWith20People() {
        TxBase tx = txMgr.newTx();
        TableScanBase tableScan = new TableScan(tx, "tani", layout);
        tableScan.beforeFirst();

        // Person 1
        tableScan.insert();
        tableScan.setString("Name", "Bob");
        tableScan.setInt("Age", 30);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 42424242.42);
        tableScan.setString("Description", "Bob is a great guy who worked in finance and is now mega rich.");

        // Person 2
        tableScan.insert();
        tableScan.setString("Name", "Smith");
        tableScan.setInt("Age", 42);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 32323232.32);
        tableScan.setString("Description", "Smith is also very rich for the same reason as Bob.");

        // Person 3
        tableScan.insert();
        tableScan.setString("Name", "Alice");
        tableScan.setInt("Age", 28);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 55555555.55);
        tableScan.setString("Description", "Alice is a successful entrepreneur who founded multiple startups.");

        // Person 4
        tableScan.insert();
        tableScan.setString("Name", "Emma");
        tableScan.setInt("Age", 35);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 67890123.45);
        tableScan.setString("Description", "Emma is a tech executive who led several IPOs.");

        // Person 5
        tableScan.insert();
        tableScan.setString("Name", "Charlie");
        tableScan.setInt("Age", 50);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 98765432.10);
        tableScan.setString("Description", "Charlie is a real estate mogul with properties worldwide.");

        // Person 6
        tableScan.insert();
        tableScan.setString("Name", "Diana");
        tableScan.setInt("Age", 33);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 44444444.44);
        tableScan.setString("Description", "Diana is a venture capitalist who invests in green technology.");

        // Person 7
        tableScan.insert();
        tableScan.setString("Name", "Frank");
        tableScan.setInt("Age", 45);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 77777777.77);
        tableScan.setString("Description", "Frank is a hedge fund manager known for aggressive trading strategies.");

        // Person 8
        tableScan.insert();
        tableScan.setString("Name", "Grace");
        tableScan.setInt("Age", 29);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 33333333.33);
        tableScan.setString("Description", "Grace is a software developer who created a popular mobile app.");

        // Person 9
        tableScan.insert();
        tableScan.setString("Name", "Henry");
        tableScan.setInt("Age", 38);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 88888888.88);
        tableScan.setString("Description", "Henry is an investment banker who specializes in mergers and acquisitions.");

        // Person 10
        tableScan.insert();
        tableScan.setString("Name", "Ivy");
        tableScan.setInt("Age", 31);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 22222222.22);
        tableScan.setString("Description", "Ivy is a fashion designer with her own luxury brand.");

        // Person 11
        tableScan.insert();
        tableScan.setString("Name", "Jack");
        tableScan.setInt("Age", 47);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 66666666.66);
        tableScan.setString("Description", "Jack is a pharmaceutical executive who developed breakthrough drugs.");

        // Person 12
        tableScan.insert();
        tableScan.setString("Name", "Kate");
        tableScan.setInt("Age", 26);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 11111111.11);
        tableScan.setString("Description", "Kate is a social media influencer who built a massive following.");

        // Person 13
        tableScan.insert();
        tableScan.setString("Name", "Leo");
        tableScan.setInt("Age", 52);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 99999999.99);
        tableScan.setString("Description", "Leo is a private equity investor with a diverse portfolio.");

        // Person 14
        tableScan.insert();
        tableScan.setString("Name", "Mia");
        tableScan.setInt("Age", 34);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 55555555.00);
        tableScan.setString("Description", "Mia is a biotech researcher who holds multiple patents.");

        // Person 15
        tableScan.insert();
        tableScan.setString("Name", "Nathan");
        tableScan.setInt("Age", 41);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 44444444.00);
        tableScan.setString("Description", "Nathan is an oil tycoon with investments in renewable energy.");

        // Person 16
        tableScan.insert();
        tableScan.setString("Name", "Olivia");
        tableScan.setInt("Age", 27);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 33333333.00);
        tableScan.setString("Description", "Olivia is a cryptocurrency trader who made fortunes during the boom.");

        // Person 17
        tableScan.insert();
        tableScan.setString("Name", "Paul");
        tableScan.setInt("Age", 48);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 77777777.00);
        tableScan.setString("Description", "Paul is a media mogul who owns several television networks.");

        // Person 18
        tableScan.insert();
        tableScan.setString("Name", "Quinn");
        tableScan.setInt("Age", 36);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 66666666.00);
        tableScan.setString("Description", "Quinn is an aerospace engineer who founded a rocket company.");

        // Person 19
        tableScan.insert();
        tableScan.setString("Name", "Ryan");
        tableScan.setInt("Age", 39);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 88888888.00);
        tableScan.setString("Description", "Ryan is a professional athlete turned successful businessman.");

        // Person 20
        tableScan.insert();
        tableScan.setString("Name", "Sophia");
        tableScan.setInt("Age", 32);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 22222222.00);
        tableScan.setString("Description", "Sophia is a lawyer who specializes in high-profile corporate cases.");

        // Person 21
        tableScan.insert();
        tableScan.setString("Name", "Thomas");
        tableScan.setInt("Age", 44);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 55555500.00);
        tableScan.setString("Description", "Thomas is a hotel chain owner with properties across continents.");

        // Person 22
        tableScan.insert();
        tableScan.setString("Name", "Ursula");
        tableScan.setInt("Age", 37);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 44444400.00);
        tableScan.setString("Description", "Ursula is a marine biologist who commercialized ocean cleaning technology.");

        // Person 23
        tableScan.insert();
        tableScan.setString("Name", "Victor");
        tableScan.setInt("Age", 53);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 91919191.91);
        tableScan.setString("Description", "Victor is a casino magnate with establishments in major cities.");

        // Person 24
        tableScan.insert();
        tableScan.setString("Name", "Wendy");
        tableScan.setInt("Age", 29);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 38383838.38);
        tableScan.setString("Description", "Wendy is a food blogger who turned her brand into a restaurant empire.");

        // Person 25
        tableScan.insert();
        tableScan.setString("Name", "Xavier");
        tableScan.setInt("Age", 46);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 82828282.82);
        tableScan.setString("Description", "Xavier is a mining executive with interests in precious metals.");

        // Person 26
        tableScan.insert();
        tableScan.setString("Name", "Yara");
        tableScan.setInt("Age", 33);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 47474747.47);
        tableScan.setString("Description", "Yara is a professional poker player who won multiple world championships.");

        // Person 27
        tableScan.insert();
        tableScan.setString("Name", "Zachary");
        tableScan.setInt("Age", 40);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 73737373.73);
        tableScan.setString("Description", "Zachary is a video game developer whose studio created blockbuster titles.");

        // Person 28
        tableScan.insert();
        tableScan.setString("Name", "Abigail");
        tableScan.setInt("Age", 28);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 29292929.29);
        tableScan.setString("Description", "Abigail is a YouTuber who built a media production company.");

        // Person 29
        tableScan.insert();
        tableScan.setString("Name", "Benjamin");
        tableScan.setInt("Age", 51);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 84848484.84);
        tableScan.setString("Description", "Benjamin is a shipping magnate with a global logistics network.");

        // Person 30
        tableScan.insert();
        tableScan.setString("Name", "Chloe");
        tableScan.setInt("Age", 30);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 36363636.36);
        tableScan.setString("Description", "Chloe is a cosmetics entrepreneur with a billion-dollar beauty brand.");

        // Person 31
        tableScan.insert();
        tableScan.setString("Name", "Daniel");
        tableScan.setInt("Age", 43);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 69696969.69);
        tableScan.setString("Description", "Daniel is a commercial pilot who founded a budget airline.");

        // Person 32
        tableScan.insert();
        tableScan.setString("Name", "Eleanor");
        tableScan.setInt("Age", 35);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 58585858.58);
        tableScan.setString("Description", "Eleanor is an art dealer who owns prestigious galleries worldwide.");

        // Person 33
        tableScan.insert();
        tableScan.setString("Name", "Felix");
        tableScan.setInt("Age", 49);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 95959595.95);
        tableScan.setString("Description", "Felix is a construction mogul specializing in skyscrapers.");

        // Person 34
        tableScan.insert();
        tableScan.setString("Name", "Georgia");
        tableScan.setInt("Age", 31);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 41414141.41);
        tableScan.setString("Description", "Georgia is a wine producer with award-winning vineyards.");

        // Person 35
        tableScan.insert();
        tableScan.setString("Name", "Harrison");
        tableScan.setInt("Age", 54);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 87878787.87);
        tableScan.setString("Description", "Harrison is a defense contractor with government contracts.");

        // Person 36
        tableScan.insert();
        tableScan.setString("Name", "Isabella");
        tableScan.setInt("Age", 26);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 25252525.25);
        tableScan.setString("Description", "Isabella is a musician who created a revolutionary music streaming service.");

        // Person 37
        tableScan.insert();
        tableScan.setString("Name", "James");
        tableScan.setInt("Age", 42);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 71717171.71);
        tableScan.setString("Description", "James is an automotive executive who pioneered electric vehicles.");

        // Person 38
        tableScan.insert();
        tableScan.setString("Name", "Kimberly");
        tableScan.setInt("Age", 34);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 52525252.52);
        tableScan.setString("Description", "Kimberly is a fitness guru who built a chain of luxury gyms.");

        // Person 39
        tableScan.insert();
        tableScan.setString("Name", "Liam");
        tableScan.setInt("Age", 38);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 64646464.64);
        tableScan.setString("Description", "Liam is a financial advisor who manages portfolios for celebrities.");

        // Person 40
        tableScan.insert();
        tableScan.setString("Name", "Madison");
        tableScan.setInt("Age", 27);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 31313131.31);
        tableScan.setString("Description", "Madison is a tech blogger who sold her site to a major publisher.");

        // Person 41
        tableScan.insert();
        tableScan.setString("Name", "Noah");
        tableScan.setInt("Age", 45);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 78787878.78);
        tableScan.setString("Description", "Noah is a robotics engineer who founded an AI research company.");

        // Person 42
        tableScan.insert();
        tableScan.setString("Name", "Penelope");
        tableScan.setInt("Age", 32);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 45454545.45);
        tableScan.setString("Description", "Penelope is a jewelry designer with a clientele of royal families.");

        // Person 43
        tableScan.insert();
        tableScan.setString("Name", "Oscar");
        tableScan.setInt("Age", 50);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 92929292.92);
        tableScan.setString("Description", "Oscar is a timber baron with sustainable forestry operations.");

        // Person 44
        tableScan.insert();
        tableScan.setString("Name", "Rachel");
        tableScan.setInt("Age", 29);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 34343434.34);
        tableScan.setString("Description", "Rachel is a neuroscientist who commercialized brain-computer interfaces.");

        // Person 45
        tableScan.insert();
        tableScan.setString("Name", "Samuel");
        tableScan.setInt("Age", 47);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 81818181.81);
        tableScan.setString("Description", "Samuel is a thoroughbred horse breeder with championship winning horses.");

        // Person 46
        tableScan.insert();
        tableScan.setString("Name", "Taylor");
        tableScan.setInt("Age", 36);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 56565656.56);
        tableScan.setString("Description", "Taylor is a renewable energy entrepreneur with wind and solar farms.");

        // Person 47
        tableScan.insert();
        tableScan.setString("Name", "Ulysses");
        tableScan.setInt("Age", 52);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 89898989.89);
        tableScan.setString("Description", "Ulysses is a publishing magnate who owns newspapers and magazines.");

        // Person 48
        tableScan.insert();
        tableScan.setString("Name", "Victoria");
        tableScan.setInt("Age", 33);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 48484848.48);
        tableScan.setString("Description", "Victoria is a film producer who won multiple Academy Awards.");

        // Person 49
        tableScan.insert();
        tableScan.setString("Name", "William");
        tableScan.setInt("Age", 41);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 74747474.74);
        tableScan.setString("Description", "William is a theme park developer with attractions around the world.");

        // Person 50
        tableScan.insert();
        tableScan.setString("Name", "Zoe");
        tableScan.setInt("Age", 28);
        tableScan.setBoolean("Man", false);
        tableScan.setDouble("NetWorth", 27272727.27);
        tableScan.setString("Description", "Zoe is a mobile game developer whose app has billions of downloads.");

        tableScan.close();
        tx.commit();
    }

    /**
     * Using table with design:
     * - Name: VARCHAR(20) // 24 bytes
     * - Age: INTEGER // 4 bytes
     * - Man: BOOLEAN // 1 byte
     * - NetWorth: DOUBLE // 8 bytes
     * - Description: VARCHAR(500) // 604 bytes
     * Total layout size: 641 bytes
     */
    @Test
    public void exceptionTest(){
        TxBase tx = txMgr.newTx();
        TableScanBase tableScan = new TableScan(tx, "tani", layout);
        tableScan.beforeFirst();
        tableScan.insert();
        tableScan.setString("Name", "Bob");
        tableScan.setInt("Age", 30);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 42424242.42);
        tableScan.setString("Description", "Bob is a great guy who worked in finance and is now mega rich.");
        tableScan.insert();
        tableScan.setString("Name", "Smith");
        tableScan.setInt("Age", 42);
        tableScan.setBoolean("Man", true);
        tableScan.setDouble("NetWorth", 32323232.32);
        tableScan.setString("Description", "Smith is also very rich for the same reason as Bob.");
        tableScan.close();
        tx.commit();

        TxBase tx1 = txMgr.newTx();
        IndexMgrBase indexMgr = new IndexMgr(tx1, tableMgr);
        tx1.commit();

        TxBase tx2 = txMgr.newTx();
        assertThrows(IllegalArgumentException.class, () -> {indexMgr.persistIndexDescriptor(null, "tani", "Name", IndexMgrBase.IndexType.STATIC_HASH);});
        assertThrows(IllegalArgumentException.class, () -> {indexMgr.persistIndexDescriptor(tx2, "taniD", "Name", IndexMgrBase.IndexType.STATIC_HASH);});
        assertThrows(IllegalArgumentException.class, () -> {indexMgr.persistIndexDescriptor(tx2, "tani", "Named", IndexMgrBase.IndexType.STATIC_HASH);});
        assertThrows(IllegalArgumentException.class, () -> {indexMgr.persistIndexDescriptor(tx2, "tani", "Name", null);});

        int id = indexMgr.persistIndexDescriptor(tx2, "tani", "Name", IndexMgrBase.IndexType.STATIC_HASH);
        tx2.commit();

        TxBase tx3 = txMgr.newTx();

        assertThrows(IllegalArgumentException.class, () -> indexMgr.get(null, id));
        assertThrows(IllegalArgumentException.class, () -> indexMgr.indexIds(tx3, "bobby"));
        assertThrows(IllegalArgumentException.class, () -> indexMgr.instantiate(tx3, 56));
        IndexDescriptorBase indexDescriptor = indexMgr.get(tx3, id);
        assertEquals("Name", indexDescriptor.getFieldName());
        assertEquals("tani", indexDescriptor.getTableName());
        assertEquals(schema, indexDescriptor.getIndexedTableSchema());

        IndexBase index = indexMgr.instantiate(tx3, id);
        DatumBase datumBase = new Datum(76);
        assertThrows(IllegalArgumentException.class, () -> index.beforeFirst(datumBase));
        assertThrows(IllegalStateException.class, index::next);
        assertThrows(IllegalStateException.class, index::getRID);

        DatumBase datumBase2 = new Datum("Bob");
        index.beforeFirst(datumBase2);
        assertThrows(IllegalStateException.class, index::getRID);
        index.insert(datumBase2, new RID(0,0));
        index.close();
        tx3.commit();
    }

    @Test
    public void indexTest(){
        populateTableWith20People();

        TxBase tx1 = txMgr.newTx();
        IndexMgrBase indexMgr = new IndexMgr(tx1, tableMgr);
        tx1.commit();

        TxBase tx2 = txMgr.newTx();

        int id = indexMgr.persistIndexDescriptor(tx2, "tani", "Name", IndexMgrBase.IndexType.STATIC_HASH);
        int id2 = indexMgr.persistIndexDescriptor(tx2, "tani", "Age", IndexMgrBase.IndexType.STATIC_HASH);
        tx2.commit();

        TxBase tx3 = txMgr.newTx();

        IndexBase index = indexMgr.instantiate(tx3, id);
        IndexBase index2 = indexMgr.instantiate(tx3, id2);

        // Create Datum objects for all 20 people's names
        DatumBase datumName1 = new Datum("Bob");
        Datum datumName2 = new Datum("Smith");
        Datum datumName3 = new Datum("Alice");
        Datum datumName4 = new Datum("Emma");
        Datum datumName5 = new Datum("Charlie");
        Datum datumName6 = new Datum("Diana");
        Datum datumName7 = new Datum("Frank");
        Datum datumName8 = new Datum("Grace");
        Datum datumName9 = new Datum("Henry");
        Datum datumName10 = new Datum("Ivy");
        Datum datumName11 = new Datum("Jack");
        Datum datumName12 = new Datum("Kate");
        Datum datumName13 = new Datum("Leo");
        Datum datumName14 = new Datum("Mia");
        Datum datumName15 = new Datum("Nathan");
        Datum datumName16 = new Datum("Olivia");
        Datum datumName17 = new Datum("Paul");
        Datum datumName18 = new Datum("Quinn");
        Datum datumName19 = new Datum("Ryan");
        Datum datumName20 = new Datum("Sophia");

        // Create Datum objects for all 20 people's ages
        Datum datumAge1 = new Datum(30);
        Datum datumAge2 = new Datum(42);
        Datum datumAge3 = new Datum(28);
        Datum datumAge4 = new Datum(35);
        Datum datumAge5 = new Datum(50);
        Datum datumAge6 = new Datum(33);
        Datum datumAge7 = new Datum(45);
        Datum datumAge8 = new Datum(29);
        Datum datumAge9 = new Datum(38);
        Datum datumAge10 = new Datum(31);
        Datum datumAge11 = new Datum(47);
        Datum datumAge12 = new Datum(26);
        Datum datumAge13 = new Datum(52);
        Datum datumAge14 = new Datum(34);
        Datum datumAge15 = new Datum(41);
        Datum datumAge16 = new Datum(27);
        Datum datumAge17 = new Datum(48);
        Datum datumAge18 = new Datum(36);
        Datum datumAge19 = new Datum(39);
        Datum datumAge20 = new Datum(32);

        index.beforeFirst(datumName1);
        index2.beforeFirst(datumAge1);

        // Block 0, slots 0-2 (People 1-3)
        index.insert(datumName1, new RID(0, 0));   // Bob
        index2.insert(datumAge1, new RID(0, 0));   // Age 30

        index.insert(datumName2, new RID(0, 1));   // Smith
        index2.insert(datumAge2, new RID(0, 1));   // Age 42

        index.insert(datumName3, new RID(0, 2));   // Alice
        index2.insert(datumAge3, new RID(0, 2));   // Age 28

        // Block 1, slots 0-2 (People 4-6)
        index.insert(datumName4, new RID(1, 0));   // Emma
        index2.insert(datumAge4, new RID(1, 0));   // Age 35

        index.insert(datumName5, new RID(1, 1));   // Charlie
        index2.insert(datumAge5, new RID(1, 1));   // Age 50

        index.insert(datumName6, new RID(1, 2));   // Diana
        index2.insert(datumAge6, new RID(1, 2));   // Age 33

        // Block 2, slots 0-2 (People 7-9)
        index.insert(datumName7, new RID(2, 0));   // Frank
        index2.insert(datumAge7, new RID(2, 0));   // Age 45

        index.insert(datumName8, new RID(2, 1));   // Grace
        index2.insert(datumAge8, new RID(2, 1));   // Age 29

        index.insert(datumName9, new RID(2, 2));   // Henry
        index2.insert(datumAge9, new RID(2, 2));   // Age 38

        // Block 3, slots 0-2 (People 10-12)
        index.insert(datumName10, new RID(3, 0));  // Ivy
        index2.insert(datumAge10, new RID(3, 0));  // Age 31

        index.insert(datumName11, new RID(3, 1));  // Jack
        index2.insert(datumAge11, new RID(3, 1));  // Age 47

        index.insert(datumName12, new RID(3, 2));  // Kate
        index2.insert(datumAge12, new RID(3, 2));  // Age 26

        // Block 4, slots 0-2 (People 13-15)
        index.insert(datumName13, new RID(4, 0));  // Leo
        index2.insert(datumAge13, new RID(4, 0));  // Age 52

        index.insert(datumName14, new RID(4, 1));  // Mia
        index2.insert(datumAge14, new RID(4, 1));  // Age 34

        index.insert(datumName15, new RID(4, 2));  // Nathan
        index2.insert(datumAge15, new RID(4, 2));  // Age 41

        // Block 5, slots 0-2 (People 16-18)
        index.insert(datumName16, new RID(5, 0));  // Olivia
        index2.insert(datumAge16, new RID(5, 0));  // Age 27

        index.insert(datumName17, new RID(5, 1));  // Paul
        index2.insert(datumAge17, new RID(5, 1));  // Age 48

        index.insert(datumName18, new RID(5, 2));  // Quinn
        index2.insert(datumAge18, new RID(5, 2));  // Age 36

        // Block 6, slots 0-1 (People 19-20)
        index.insert(datumName19, new RID(6, 0));  // Ryan
        index2.insert(datumAge19, new RID(6, 0));  // Age 39

        index.insert(datumName20, new RID(6, 1));  // Sophia
        index2.insert(datumAge20, new RID(6, 1));  // Age 32

        index.beforeFirst(datumName8);//Grace
        index2.beforeFirst(datumAge8);//29
        index.beforeFirst(datumName8);
        index2.beforeFirst(datumAge8);
        assertTrue(index.next());
        assertTrue(index2.next());

        RID rid1 = index.getRID();
        RID rid2 = index2.getRID();
        assertEquals(rid1, rid2);
        index.close();
        index2.close();
        tx3.commit();

        TxBase tx4 = txMgr.newTx();
        TableScan scan1 = new TableScan(tx4, "tani", layout);
        scan1.beforeFirst();
        scan1.moveToRid(rid1);
        String name = scan1.getString("Name");
        int age = scan1.getInt("Age");
        assertEquals(29, age);
        assertEquals("Grace", name);


        IndexBase index3 = indexMgr.instantiate(tx4, id2);
        index3.delete(datumAge8, rid1);
        index3.beforeFirst(datumAge8);
        index3.next();
        RID rid3 = index3.getRID();
        scan1.moveToRid(rid3);
        assertNotEquals(scan1.getString("Name"), name);
        scan1.close();

        index3.deleteAll();
        index3.beforeFirst(datumAge8);
        assertFalse(index3.next());
        tx4.commit();

        TxBase tx5 = txMgr.newTx();
        indexMgr.deleteAll(tx5, "tani");

        SchemaBase indexMDSchema = new Schema();
        indexMDSchema.addField(TABLE_NAME_FIELD, Types.VARCHAR, 16);
        indexMDSchema.addField(INDEX_NAME_FIELD, Types.VARCHAR, 16);
        indexMDSchema.addIntField("indexType");
        indexMDSchema.addIntField(INDEX_ID_FIELD);
        LayoutBase indexMDLayout = new Layout(indexMDSchema);

        TableScanBase tableScan = new TableScan(tx5, INDEX_METADATA_FILE, indexMDLayout);
        tableScan.beforeFirst();
        assertFalse(tableScan.next());
        tableScan.close();

        Schema ourSchema = new Schema();
        ourSchema.addField("key", Types.VARCHAR, 16);
        ourSchema.addIntField(RID_BLOCK_ID_FIELD);
        ourSchema.addIntField(RID_SLOT_FIELD);
        Layout layout1 = new Layout(ourSchema);

        TableScanBase tableScan1 = new TableScan(tx5, "tani_Name_1", layout1);
        tableScan1.beforeFirst();
        assertFalse(tableScan1.next());
        tableScan1.close();
        tx5.commit();
    }

    @Test
    public void speedTest(){
        populateTableWith20People();

        TxBase tx1 = txMgr.newTx();
        IndexMgrBase indexMgr = new IndexMgr(tx1, tableMgr);
        tx1.commit();

        TxBase tx2 = txMgr.newTx();

        int id = indexMgr.persistIndexDescriptor(tx2, "tani", "Name", IndexMgrBase.IndexType.STATIC_HASH);
        int id2 = indexMgr.persistIndexDescriptor(tx2, "tani", "Age", IndexMgrBase.IndexType.STATIC_HASH);
        tx2.commit();

        TxBase tx3 = txMgr.newTx();

        IndexBase index = indexMgr.instantiate(tx3, id);
        IndexBase index2 = indexMgr.instantiate(tx3, id2);

        // Create Datum objects for all 20 people's names
        DatumBase datumName1 = new Datum("Bob");
        Datum datumName2 = new Datum("Smith");
        Datum datumName3 = new Datum("Alice");
        Datum datumName4 = new Datum("Emma");
        Datum datumName5 = new Datum("Charlie");
        Datum datumName6 = new Datum("Diana");
        Datum datumName7 = new Datum("Frank");
        Datum datumName8 = new Datum("Grace");
        Datum datumName9 = new Datum("Henry");
        Datum datumName10 = new Datum("Ivy");
        Datum datumName11 = new Datum("Jack");
        Datum datumName12 = new Datum("Kate");
        Datum datumName13 = new Datum("Leo");
        Datum datumName14 = new Datum("Mia");
        Datum datumName15 = new Datum("Nathan");
        Datum datumName16 = new Datum("Olivia");
        Datum datumName17 = new Datum("Paul");
        Datum datumName18 = new Datum("Quinn");
        Datum datumName19 = new Datum("Ryan");
        Datum datumName20 = new Datum("Sophia");

        // Create Datum objects for all 20 people's ages
        Datum datumAge1 = new Datum(30);
        Datum datumAge2 = new Datum(42);
        Datum datumAge3 = new Datum(28);
        Datum datumAge4 = new Datum(35);
        Datum datumAge5 = new Datum(50);
        Datum datumAge6 = new Datum(33);
        Datum datumAge7 = new Datum(45);
        Datum datumAge8 = new Datum(29);
        Datum datumAge9 = new Datum(38);
        Datum datumAge10 = new Datum(31);
        Datum datumAge11 = new Datum(47);
        Datum datumAge12 = new Datum(26);
        Datum datumAge13 = new Datum(52);
        Datum datumAge14 = new Datum(34);
        Datum datumAge15 = new Datum(41);
        Datum datumAge16 = new Datum(27);
        Datum datumAge17 = new Datum(48);
        Datum datumAge18 = new Datum(36);
        Datum datumAge19 = new Datum(39);
        Datum datumAge20 = new Datum(32);

        // Create Datum objects for people 21-50 names
        Datum datumName21 = new Datum("Thomas");
        Datum datumName22 = new Datum("Ursula");
        Datum datumName23 = new Datum("Victor");
        Datum datumName24 = new Datum("Wendy");
        Datum datumName25 = new Datum("Xavier");
        Datum datumName26 = new Datum("Yara");
        Datum datumName27 = new Datum("Zachary");
        Datum datumName28 = new Datum("Abigail");
        Datum datumName29 = new Datum("Benjamin");
        Datum datumName30 = new Datum("Chloe");
        Datum datumName31 = new Datum("Daniel");
        Datum datumName32 = new Datum("Eleanor");
        Datum datumName33 = new Datum("Felix");
        Datum datumName34 = new Datum("Georgia");
        Datum datumName35 = new Datum("Harrison");
        Datum datumName36 = new Datum("Isabella");
        Datum datumName37 = new Datum("James");
        Datum datumName38 = new Datum("Kimberly");
        Datum datumName39 = new Datum("Liam");
        Datum datumName40 = new Datum("Madison");
        Datum datumName41 = new Datum("Noah");
        Datum datumName42 = new Datum("Penelope");
        Datum datumName43 = new Datum("Oscar");
        Datum datumName44 = new Datum("Rachel");
        Datum datumName45 = new Datum("Samuel");
        Datum datumName46 = new Datum("Taylor");
        Datum datumName47 = new Datum("Ulysses");
        Datum datumName48 = new Datum("Victoria");
        Datum datumName49 = new Datum("William");
        Datum datumName50 = new Datum("Zoe");

        // Create Datum objects for people 21-50 ages
        Datum datumAge21 = new Datum(44);
        Datum datumAge22 = new Datum(37);
        Datum datumAge23 = new Datum(53);
        Datum datumAge24 = new Datum(29);
        Datum datumAge25 = new Datum(46);
        Datum datumAge26 = new Datum(33);
        Datum datumAge27 = new Datum(40);
        Datum datumAge28 = new Datum(28);
        Datum datumAge29 = new Datum(51);
        Datum datumAge30 = new Datum(30);
        Datum datumAge31 = new Datum(43);
        Datum datumAge32 = new Datum(35);
        Datum datumAge33 = new Datum(49);
        Datum datumAge34 = new Datum(31);
        Datum datumAge35 = new Datum(54);
        Datum datumAge36 = new Datum(26);
        Datum datumAge37 = new Datum(42);
        Datum datumAge38 = new Datum(34);
        Datum datumAge39 = new Datum(38);
        Datum datumAge40 = new Datum(27);
        Datum datumAge41 = new Datum(45);
        Datum datumAge42 = new Datum(32);
        Datum datumAge43 = new Datum(50);
        Datum datumAge44 = new Datum(29);
        Datum datumAge45 = new Datum(47);
        Datum datumAge46 = new Datum(36);
        Datum datumAge47 = new Datum(52);
        Datum datumAge48 = new Datum(33);
        Datum datumAge49 = new Datum(41);
        Datum datumAge50 = new Datum(28);

        index.beforeFirst(datumName1);
        index2.beforeFirst(datumAge1);

        // Block 0, slots 0-2 (People 1-3)
        index.insert(datumName1, new RID(0, 0));   // Bob
        index2.insert(datumAge1, new RID(0, 0));   // Age 30

        index.insert(datumName2, new RID(0, 1));   // Smith
        index2.insert(datumAge2, new RID(0, 1));   // Age 42

        index.insert(datumName3, new RID(0, 2));   // Alice
        index2.insert(datumAge3, new RID(0, 2));   // Age 28

        // Block 1, slots 0-2 (People 4-6)
        index.insert(datumName4, new RID(1, 0));   // Emma
        index2.insert(datumAge4, new RID(1, 0));   // Age 35

        index.insert(datumName5, new RID(1, 1));   // Charlie
        index2.insert(datumAge5, new RID(1, 1));   // Age 50

        index.insert(datumName6, new RID(1, 2));   // Diana
        index2.insert(datumAge6, new RID(1, 2));   // Age 33

        // Block 2, slots 0-2 (People 7-9)
        index.insert(datumName7, new RID(2, 0));   // Frank
        index2.insert(datumAge7, new RID(2, 0));   // Age 45

        index.insert(datumName8, new RID(2, 1));   // Grace
        index2.insert(datumAge8, new RID(2, 1));   // Age 29

        index.insert(datumName9, new RID(2, 2));   // Henry
        index2.insert(datumAge9, new RID(2, 2));   // Age 38

        // Block 3, slots 0-2 (People 10-12)
        index.insert(datumName10, new RID(3, 0));  // Ivy
        index2.insert(datumAge10, new RID(3, 0));  // Age 31

        index.insert(datumName11, new RID(3, 1));  // Jack
        index2.insert(datumAge11, new RID(3, 1));  // Age 47

        index.insert(datumName12, new RID(3, 2));  // Kate
        index2.insert(datumAge12, new RID(3, 2));  // Age 26

        // Block 4, slots 0-2 (People 13-15)
        index.insert(datumName13, new RID(4, 0));  // Leo
        index2.insert(datumAge13, new RID(4, 0));  // Age 52

        index.insert(datumName14, new RID(4, 1));  // Mia
        index2.insert(datumAge14, new RID(4, 1));  // Age 34

        index.insert(datumName15, new RID(4, 2));  // Nathan
        index2.insert(datumAge15, new RID(4, 2));  // Age 41

        // Block 5, slots 0-2 (People 16-18)
        index.insert(datumName16, new RID(5, 0));  // Olivia
        index2.insert(datumAge16, new RID(5, 0));  // Age 27

        index.insert(datumName17, new RID(5, 1));  // Paul
        index2.insert(datumAge17, new RID(5, 1));  // Age 48

        index.insert(datumName18, new RID(5, 2));  // Quinn
        index2.insert(datumAge18, new RID(5, 2));  // Age 36

        // Block 6, slots 0-1 (People 19-20)
        index.insert(datumName19, new RID(6, 0));  // Ryan
        index2.insert(datumAge19, new RID(6, 0));  // Age 39

        index.insert(datumName20, new RID(6, 1));  // Sophia
        index2.insert(datumAge20, new RID(6, 1));  // Age 32

        // Block 6, slot 2 (Person 21)
        index.insert(datumName21, new RID(6, 2));  // Thomas
        index2.insert(datumAge21, new RID(6, 2));  // Age 44

        // Block 7, slots 0-2 (People 22-24)
        index.insert(datumName22, new RID(7, 0));  // Ursula
        index2.insert(datumAge22, new RID(7, 0));  // Age 37

        index.insert(datumName23, new RID(7, 1));  // Victor
        index2.insert(datumAge23, new RID(7, 1));  // Age 53

        index.insert(datumName24, new RID(7, 2));  // Wendy
        index2.insert(datumAge24, new RID(7, 2));  // Age 29

        // Block 8, slots 0-2 (People 25-27)
        index.insert(datumName25, new RID(8, 0));  // Xavier
        index2.insert(datumAge25, new RID(8, 0));  // Age 46

        index.insert(datumName26, new RID(8, 1));  // Yara
        index2.insert(datumAge26, new RID(8, 1));  // Age 33

        index.insert(datumName27, new RID(8, 2));  // Zachary
        index2.insert(datumAge27, new RID(8, 2));  // Age 40

        // Block 9, slots 0-2 (People 28-30)
        index.insert(datumName28, new RID(9, 0));  // Abigail
        index2.insert(datumAge28, new RID(9, 0));  // Age 28

        index.insert(datumName29, new RID(9, 1));  // Benjamin
        index2.insert(datumAge29, new RID(9, 1));  // Age 51

        index.insert(datumName30, new RID(9, 2));  // Chloe
        index2.insert(datumAge30, new RID(9, 2));  // Age 30

        // Block 10, slots 0-2 (People 31-33)
        index.insert(datumName31, new RID(10, 0)); // Daniel
        index2.insert(datumAge31, new RID(10, 0)); // Age 43

        index.insert(datumName32, new RID(10, 1)); // Eleanor
        index2.insert(datumAge32, new RID(10, 1)); // Age 35

        index.insert(datumName33, new RID(10, 2)); // Felix
        index2.insert(datumAge33, new RID(10, 2)); // Age 49

        // Block 11, slots 0-2 (People 34-36)
        index.insert(datumName34, new RID(11, 0)); // Georgia
        index2.insert(datumAge34, new RID(11, 0)); // Age 31

        index.insert(datumName35, new RID(11, 1)); // Harrison
        index2.insert(datumAge35, new RID(11, 1)); // Age 54

        index.insert(datumName36, new RID(11, 2)); // Isabella
        index2.insert(datumAge36, new RID(11, 2)); // Age 26

        // Block 12, slots 0-2 (People 37-39)
        index.insert(datumName37, new RID(12, 0)); // James
        index2.insert(datumAge37, new RID(12, 0)); // Age 42

        index.insert(datumName38, new RID(12, 1)); // Kimberly
        index2.insert(datumAge38, new RID(12, 1)); // Age 34

        index.insert(datumName39, new RID(12, 2)); // Liam
        index2.insert(datumAge39, new RID(12, 2)); // Age 38

        // Block 13, slots 0-2 (People 40-42)
        index.insert(datumName40, new RID(13, 0)); // Madison
        index2.insert(datumAge40, new RID(13, 0)); // Age 27

        index.insert(datumName41, new RID(13, 1)); // Noah
        index2.insert(datumAge41, new RID(13, 1)); // Age 45

        index.insert(datumName42, new RID(13, 2)); // Penelope
        index2.insert(datumAge42, new RID(13, 2)); // Age 32

        // Block 14, slots 0-2 (People 43-45)
        index.insert(datumName43, new RID(14, 0)); // Oscar
        index2.insert(datumAge43, new RID(14, 0)); // Age 50

        index.insert(datumName44, new RID(14, 1)); // Rachel
        index2.insert(datumAge44, new RID(14, 1)); // Age 29

        index.insert(datumName45, new RID(14, 2)); // Samuel
        index2.insert(datumAge45, new RID(14, 2)); // Age 47

        // Block 15, slots 0-2 (People 46-48)
        index.insert(datumName46, new RID(15, 0)); // Taylor
        index2.insert(datumAge46, new RID(15, 0)); // Age 36

        index.insert(datumName47, new RID(15, 1)); // Ulysses
        index2.insert(datumAge47, new RID(15, 1)); // Age 52

        index.insert(datumName48, new RID(15, 2)); // Victoria
        index2.insert(datumAge48, new RID(15, 2)); // Age 33

        // Block 16, slots 0-1 (People 49-50)
        index.insert(datumName49, new RID(16, 0)); // William
        index2.insert(datumAge49, new RID(16, 0)); // Age 41

        index.insert(datumName50, new RID(16, 1)); // Zoe
        index2.insert(datumAge50, new RID(16, 1)); // Age 28

        tx3.commit();

        TxBase tx4 = txMgr.newTx();
        TableScanBase tableScanSlow = new TableScan(tx4, "tani", layout);
        long time1 = System.currentTimeMillis();
        tableScanSlow.beforeFirst();
        boolean found = false;
        while(!found && tableScanSlow.next()) {
            if(tableScanSlow.getString("Name").equals("Zoe")) {
                found = true;
            }
        }
        long time2 = System.currentTimeMillis();
        tableScanSlow.close();
        System.out.println("Time taken for slow: " + (time2 - time1) + "ms");

        IndexBase indexFast = indexMgr.instantiate(tx4, id);
        TableScanBase tableScanFast = new TableScan(tx4, "tani", layout);
        long time3 = System.currentTimeMillis();
        indexFast.beforeFirst(datumName50);
        indexFast.next();
        RID rid = indexFast.getRID();
        tableScanFast.beforeFirst();
        tableScanFast.moveToRid(rid);
        String name = tableScanFast.getString("Name");
        long time4 = System.currentTimeMillis();
        tableScanFast.close();
        System.out.println("Time taken for fast: " + (time4 - time3) + "ms");
        System.out.println("Name : " + name);
        tx4.commit();

    }

}
