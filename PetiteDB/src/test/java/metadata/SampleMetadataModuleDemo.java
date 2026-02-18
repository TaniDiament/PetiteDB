package metadata;

/** Illustrates SOME happy-path usage of the Metadata Module APIs.
 *
 * @author Avraham Leff
 */

import java.io.File;
import java.util.Properties;

import edu.yu.dbimpl.buffer.*;
import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.file.*;
import edu.yu.dbimpl.log.*;
import edu.yu.dbimpl.metadata.TableMgr;
import edu.yu.dbimpl.metadata.TableMgrBase;
import edu.yu.dbimpl.tx.*;
import edu.yu.dbimpl.record.*;
import static edu.yu.dbimpl.record.RecordPageBase.BEFORE_FIRST_SLOT;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SampleMetadataModuleDemo {

  public static void main(final String[] args) {
    try {
      logger.info("Entered main");

      final Properties dbProperties = new Properties();
      dbProperties.setProperty(DBConfiguration.DB_STARTUP, String.valueOf(true));
      logger.info("Setting DBConfiguration properties with: {}", dbProperties);
      DBConfiguration.INSTANCE.get().setConfiguration(dbProperties);

      final String dirName = "SampleMetadataModuleDemo";
      final String logFile = "temp_logfile";
      final String dbFile = "testfile";
      final File dbDirectory = new File(dirName);
      final int blockSize = 400;
      final int bufferSize = 1_000;
      final int maxWaitTime = 500; // ms
      final int maxTxWaitTime = 500; // ms

      final FileMgrBase fileMgr = new FileMgr(dbDirectory, blockSize);
      final LogMgrBase logMgr = new LogMgr(fileMgr, logFile);
      final BufferMgrBase bufferMgr =
        new BufferMgr(fileMgr, logMgr, bufferSize, maxWaitTime);
      logger.info("Created BufferMgr with a buffer size of {} and maxWaitTime "+
                  "of {}", bufferSize, maxWaitTime); 
      final TxMgrBase txMgr = new TxMgr(fileMgr, logMgr, bufferMgr, maxTxWaitTime);
      final TxBase tx = txMgr.newTx();

      logger.info("Creating a schema");
      final SchemaBase schema = new Schema();
      schema.addIntField(fieldA);
      schema.addStringField(fieldB, logicalLengthOfFieldB);
      logger.info("Added int field {} and string field {}",
                  fieldA, fieldB);

      final String tableName = "TDemo";
      final TableMgrBase tableMgr = new TableMgr(tx);
      logger.info("Adding table {}'s metadata {} to catalog",
                  tableName, schema);
      tableMgr.createTable(tableName, schema, tx);

      logger.info("Accessing system catalog to get table {}'s layout",
                  tableName);
      final LayoutBase layout = tableMgr.getLayout(tableName, tx);
      logger.info("Record slot size: {}", layout.slotSize());

      final int nRecords = 50;  // to ensure multiple blocks
      logger.info("Using layout to store {} records in table {}",
                  nRecords, tableName);
      final TableScanBase ts = new TableScan(tx, tableName, layout);

      for (int i=0; i<nRecords; i++) {
        ts.insert();            // position cursor on new, empty, record
        ts.setInt(fieldA, i);
        ts.setString(fieldB, "record"+i);
      } // creating records
      
      ts.close();
      tx.commit();

      logger.info("Closed the scan, committed the transaction");
    }
    catch (Exception e) {
      logger.error("Problem: ", e);
      throw new RuntimeException(e);
    }
    finally {
      logger.info("Exiting main");
    }
  }

  private final static Logger logger =
    LogManager.getLogger(SampleMetadataModuleDemo.class);

  private final static String fieldA = "A";
  private final static String fieldB = "B";
  private final static String fieldC = "C";
  private final static int logicalLengthOfFieldB = 12;
  

} // class
