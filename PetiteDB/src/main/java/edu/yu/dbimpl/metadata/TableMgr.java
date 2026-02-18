package edu.yu.dbimpl.metadata;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.record.*;
import edu.yu.dbimpl.tx.TxBase;

import java.util.HashMap;
import java.util.Map;

/** Specifies the public API for the TableMgr implementation by requiring all
 * TableMgr implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * TableMgrBase constrains the TableMgr design by implicitly assuming an
 * implementation consisting of one table that stores meta-data about table
 * names, and another tables that stores meta-data about field names.  Also:
 * the names of these tables and some of their field names are constrained to
 * match the specified public constants.  In general, PetiteDB tries to give
 * implementations more freedom, but hard to see how to define a testable
 * interface that doesn't use e.g., Java's reflection APIs to do the job.
 *
 * A TableMgr stores the meta-data about tables created by users, thus creating
 * and managing a catalog.  Implementations MUST SUPPORT client ability to
 * TableScan iterate over the TABLE_META_DATA_TABLE and FIELD_META_DATA_TABLE
 * TableScan iterate over the TABLE_META_DATA_TABLE and FIELD_META_DATA_TABLE
 * catalog tables.  The TABLE_META_DATA_TABLE MUST STORE table name information
 * in a TABLE_NAME field.  Implementations NEED NOT support field names and
 * tables names that are larger than MAX_LENGTH_PER_NAME.
 *
 * The DBMS has exactly one TableMgr object (singleton pattern), which is
 * (conceptually) created during system startup, and in practice by a single
 * invocation of the constructor.
 *
 * @author Avraham Leff
 */
public class TableMgr extends TableMgrBase{
    private static final String SLOT_SIZE = "slotsize";
    private static final String FIELD_NAME = "fldname";
    private static final String TYPE = "type";      // Stores java.sql.Types as int
    private static final String LENGTH = "length";
    private static final String OFFSET = "offset";

    private final LayoutBase tcatLayout;
    private final LayoutBase fcatLayout;
    /**
     * Constructor: create a new table (catalog) manager.
     * <p>
     * A table manager MUST access the DBConfiguration singleton to determine if
     * it is required to manage a brand-new database or to use an existing
     * database.  If the latter, the table manager is responsible for loading
     * previously persisted catlog information before servicing client requests.
     *
     * @param tx supplies the transactional scope for database operations used in
     *           the constructor implementation.  The client is responsible for managing
     *           the transaction's life-cycle, and to ensure that the tx is active when
     *           passed to the table manager.
     */
    public TableMgr(TxBase tx) {
        super(tx);
        // Define layout for the table catalog (tblcat)
        Schema tcatSchema = new Schema();
        tcatSchema.addStringField(TABLE_NAME, MAX_LENGTH_PER_NAME);
        tcatSchema.addIntField(SLOT_SIZE);
        tcatLayout = new Layout(tcatSchema);

        // Define layout for the field catalog (fldcat)
        Schema fcatSchema = new Schema();
        fcatSchema.addStringField(TABLE_NAME, MAX_LENGTH_PER_NAME);
        fcatSchema.addStringField(FIELD_NAME, MAX_LENGTH_PER_NAME);
        fcatSchema.addIntField(TYPE);   // Integer field to store java.sql.Types constants
        fcatSchema.addIntField(LENGTH);
        fcatSchema.addIntField(OFFSET);
        fcatLayout = new Layout(fcatSchema);

        // If starting a brand-new database, create the catalog tables themselves
        if (DBConfiguration.INSTANCE.isDBStartup()) {
            createTable(TABLE_META_DATA_TABLE, tcatSchema, tx);
            createTable(FIELD_META_DATA_TABLE, fcatSchema, tx);
        }
    }

    /**
     * Retrieves the layout of the specified table.  If the table is not in the
     * catalog, return null.
     *
     * @param tableName the name of the table whose meta-data is being requested
     * @param tx        supplies the transactional scope for the method's implementation.
     * @return the meta-data for the specified table, null if no such table.
     */
    @Override
    public LayoutBase getLayout(String tableName, TxBase tx) {
        int size = -1;
        // Search table catalog for the record slot size
        TableScanBase tcatScan = new TableScan(tx, TABLE_META_DATA_TABLE, tcatLayout);
        while (tcatScan.next()) {
            if (tcatScan.getString(TABLE_NAME).equals(tableName)) {
                size = tcatScan.getInt(SLOT_SIZE);
                break;
            }
        }
        tcatScan.close();

        if (size == -1) return null; // Metadata not found

        // Reconstruct schema and physical offsets from the field catalog
        Schema schema = new Schema();
        Map<String, Integer> offsets = new HashMap<>();
        TableScanBase fcatScan = new TableScan(tx, FIELD_META_DATA_TABLE, fcatLayout);
        while (fcatScan.next()) {
            if (fcatScan.getString(TABLE_NAME).equals(tableName)) {
                String fldName = fcatScan.getString(FIELD_NAME);
                int typeInt = fcatScan.getInt(TYPE); // Retrieve the integer type code
                int fldLen = fcatScan.getInt(LENGTH);
                int fldOffset = fcatScan.getInt(OFFSET);

                schema.addField(fldName, typeInt, fldLen);
                offsets.put(fldName, fldOffset);
            }
        }
        fcatScan.close();

        // Return the layout using the catalog-retrieval constructor
        return new Layout(schema, offsets, size);
    }

    /**
     * Supplies the meta-data that should be persisted to the system catalog
     * about a new database table.
     * <p>
     * NOTE: the table itself need not exist at the time that this method is
     * invoked (or even be created subsequently in the same tx).  It's OK if the
     * user is entering metadata about a table to be created later.
     *
     * @param tableName the name of the new table
     * @param schema    the table's schema
     * @param tx        supplies the transactional scope for the method's implementation
     * @return the layout that the DBMS has now associated with this table name.
     * @throws IllegalArgumentException if the catalog already contains an entry
     *                                  for the specified table.
     * @see #replace
     */
    @Override
    public LayoutBase createTable(String tableName, SchemaBase schema, TxBase tx) {
        if (getLayout(tableName, tx) != null) {
            throw new IllegalArgumentException("Table metadata already exists for: " + tableName);
        }

        // Calculate physical layout based on the logical schema
        LayoutBase layout = new Layout(schema);

        // Persist table-level info
        TableScanBase tcatScan = new TableScan(tx, TABLE_META_DATA_TABLE, tcatLayout);
        tcatScan.insert();
        tcatScan.setString(TABLE_NAME, tableName);
        tcatScan.setInt(SLOT_SIZE, layout.slotSize());
        tcatScan.close();

        // Persist field-level info (Types are stored as integers)
        TableScanBase fcatScan = new TableScan(tx, FIELD_META_DATA_TABLE, fcatLayout);
        for (String fldName : schema.fields()) {
            fcatScan.insert();
            fcatScan.setString(TABLE_NAME, tableName);
            fcatScan.setString(FIELD_NAME, fldName);
            fcatScan.setInt(TYPE, schema.type(fldName)); // Reconstructable via java.sql.Types
            fcatScan.setInt(LENGTH, schema.length(fldName));
            fcatScan.setInt(OFFSET, layout.offset(fldName));
        }
        fcatScan.close();

        return layout;
    }

    /**
     * Replaces existing metadata associated with the specified table.
     * <p>
     * NOTE: the table itself need not exist at the time that this method is
     * invoked (or even be created subsequently in the same tx).  It's OK if the
     * user is entering metadata about a table to be created later.
     *
     * @param tableName the name of the table.
     * @param schema    the table's new schema.  If the schema is null, the effect
     *                  is to only delete the existing metadata.
     * @param tx        supplies the transactional scope for the method's implementation
     * @return LayoutBase the metadata previously associated with the table.
     * @throws IllegalArgumentException if metadata for the table isn't currently
     *                                  in the catalog.
     */
    @Override
    public LayoutBase replace(String tableName, SchemaBase schema, TxBase tx) {
        LayoutBase oldLayout = getLayout(tableName, tx);
        if (oldLayout == null) {
            throw new IllegalArgumentException("Cannot replace metadata: " + tableName + " not found.");
        }

        // 1. Delete old table entry
        TableScanBase tcatScan = new TableScan(tx, TABLE_META_DATA_TABLE, tcatLayout);
        while (tcatScan.next()) {
            if (tcatScan.getString(TABLE_NAME).equals(tableName)) {
                tcatScan.delete();
                break;
            }
        }
        tcatScan.close();

        // 2. Delete old field entries
        TableScanBase fcatScan = new TableScan(tx, FIELD_META_DATA_TABLE, fcatLayout);
        while (fcatScan.next()) {
            if (fcatScan.getString(TABLE_NAME).equals(tableName)) {
                fcatScan.delete();
            }
        }
        fcatScan.close();

        // 3. Re-create metadata if a new schema is provided
        if (schema != null) {
            createTable(tableName, schema, tx);
        }

        return oldLayout;
    }
}
