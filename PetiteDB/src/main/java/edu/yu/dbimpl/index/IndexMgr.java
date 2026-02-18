package edu.yu.dbimpl.index;

import edu.yu.dbimpl.config.DBConfiguration;
import edu.yu.dbimpl.metadata.TableMgrBase;
import edu.yu.dbimpl.record.*;
import edu.yu.dbimpl.tx.TxBase;

import java.io.File;
import java.sql.Types;
import java.util.*;

/** Specifies the public API for the IndexMgr implementation by requiring all
 * IndexMgr implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * An IndexMgr manages and persists index meta-data (allowing clients to create
 * and retrieve such meta-data).  An IndexMgr also instantiates corresponding
 * index "runtime index" instances, allowing clients to iterate over an index's
 * records and access the corresponding data records.
 *
 * The DBMS has exactly one IndexMgr object (singleton pattern), which is
 * (conceptually) created during system startup, and in practice by a single
 * invocation of the constructor.
 *
 * Note to implementors: while not adequate for production deployments,
 * worst-case linear performance for these APIs is sufficient.
 *
 * @author Avraham Leff
 */

public class IndexMgr extends IndexMgrBase{
    private final TxBase txBase;
    private final int buckets;
    private final TableMgrBase tableMgr;
    static final String INDEX_METADATA_FILE = "indexMetaFile";
    static final String TABLE_NAME_FIELD = "tableName";
    static final String INDEX_ID_FIELD = "indexId";
    static final String INDEX_NAME_FIELD = "indexName";
    static final String RID_BLOCK_ID_FIELD = "RIDBlock";
    static final String RID_SLOT_FIELD = "RIDSlot";
    private final LayoutBase indexMDLayout;
    private int nextIndexID = 0;

    /**
     * Constructor creates a new index manager.
     * <p>
     * An index manager MUST access the DBConfiguration singleton to determine if
     * it is required to manage a brand-new database or to use an existing
     * database.  If the latter, the index manager is responsible for loading
     * previously persisted state before servicing client requests.
     * <p>
     * An index manager MUST access the DBConfiguration singleton to determine
     * the number of buckets to use in a static hashing scheme.  It's the DBMS
     * client's responsibility to ensure that the value doesn't change after it's
     * initial ("startup") state.
     *
     * @param tx       supplies the transactional scope for database operations used in
     *                 the constructor implementation, cannot be null.  It's the client's
     *                 responsibility to manage the lifecycle of this transaction, and to ensure
     *                 that the tx is active when passed to the constructor.
     * @param tableMgr to be used to persist index meta-data, cannot be null.
     * @throws IllegalArgumentException if arguments don't meet the
     *                                  pre-conditions.
     */
    public IndexMgr(TxBase tx, TableMgrBase tableMgr) {
        super(tx, tableMgr);
        if(tx == null || tableMgr == null){
            throw new IllegalArgumentException("tx or table mgr is null");
        }
        this.txBase = tx;
        this.tableMgr = tableMgr;
        this.buckets = DBConfiguration.INSTANCE.nStaticHashBuckets();
        SchemaBase indexMDSchema = new Schema();
        indexMDSchema.addField(TABLE_NAME_FIELD, Types.VARCHAR, 16);
        indexMDSchema.addField(INDEX_NAME_FIELD, Types.VARCHAR, 16);
        indexMDSchema.addIntField("indexType");
        indexMDSchema.addIntField(INDEX_ID_FIELD);
        indexMDLayout = new Layout(indexMDSchema);
        if(DBConfiguration.INSTANCE.isDBStartup()){
            tableMgr.createTable(INDEX_METADATA_FILE, indexMDSchema, txBase);
        }else{
            int blockSize = tx.blockSize();
            TableScanBase ts = new TableScan(tx, INDEX_METADATA_FILE, indexMDLayout);
            File indexFile = new File(ts.getTableFileName());
            RID rid = new RID((int)indexFile.length()/blockSize, 0);
            ts.moveToRid(rid);
            while(ts.next()){
                continue;
            }
            nextIndexID = ts.getInt(INDEX_ID_FIELD)+1;
            ts.close();
        }
    }

    /**
     * Persists information about the specified index and returns the unique id
     * that the IndexMgr associates with this index.  If the index already
     * exists, no exception is thrown (the operation is a no-op), and the
     * implementation returns the id of the previously persisted index.
     *
     * @param tx        supplies the transactional scope for database operations,
     *                  client's responsibility to ensure that not null.  It's the client's
     *                  responsibility to manage the lifecycle of this transaction.
     * @param tableName the name of the table-to-be-indexed, catalog information
     *                  for this table must already exist.
     * @param fieldName the name of the field-to-be-indexed, must match a field
     *                  in the table's catalog information.  The name of the index is identical to
     *                  the field name.
     * @param indexType type of the index (e.g., static hashing, B-Tree), must be
     *                  non-null.
     * @return the persisted id that is associated with the index information.
     * @throws IllegalArgumentException if arguments don't meet the
     *                                  pre-conditions.
     * @see #get
     * @see #indexIds
     * @see #instantiate
     */
    @Override
    public int persistIndexDescriptor(TxBase tx, String tableName, String fieldName, IndexType indexType) {
        if(tx == null || indexType == null){
            throw new IllegalArgumentException("tx or index type is null");
        }
        LayoutBase lb = tableMgr.getLayout(tableName, tx);
        if(lb == null || !lb.schema().hasField(fieldName)){
            throw new IllegalArgumentException("field name not found");
        }
        int sqlType = lb.schema().type(fieldName);
        TableScanBase tableScan = new TableScan(tx, INDEX_METADATA_FILE, indexMDLayout);
        tableScan.beforeFirst();
        while(tableScan.next()){
            if(tableScan.getString(TABLE_NAME_FIELD).equals(tableName) && tableScan.getString(INDEX_NAME_FIELD).equals(fieldName)){
                int returnInt = tableScan.getInt(INDEX_ID_FIELD);
                tableScan.close();
                return returnInt;
            }
        }
        tableScan.insert();
        tableScan.setString(TABLE_NAME_FIELD, tableName);
        tableScan.setString(INDEX_NAME_FIELD, fieldName);
        tableScan.setInt(INDEX_ID_FIELD, nextIndexID);
        nextIndexID++;
        tableScan.close();

        //schema for indexes
        Schema ourSchema = new Schema();
        ourSchema.addField("key", sqlType, 16);
        ourSchema.addIntField(RID_BLOCK_ID_FIELD);
        ourSchema.addIntField(RID_SLOT_FIELD);
        //make actual index files
        for(int i = 0; i < buckets; i++){
            tableMgr.createTable(tableName + "_" + fieldName+ "_" + i, ourSchema, tx);
        }
        return nextIndexID-1;
    }

    /**
     * Returns the unique index ids associated with the specified table name.
     *
     * @param tx        supplies the transactional scope for database operations used in
     *                  the implementation, cannot be null.  It's the client's responsibility to
     *                  manage the lifecycle of this transaction.
     * @param tableName the table about which index information is being requested
     * @return Set containing the ids, empty set if no indices are defined for the table.
     * @throws IllegalArgumentException if IndexMgr.persistIndexDescriptor()
     *                                  hasn't been invoked for this table name.
     */
    @Override
    public Set<Integer> indexIds(TxBase tx, String tableName) {
        Set<Integer> ids = new HashSet<Integer>();
        TableScanBase tableScan = new TableScan(tx, INDEX_METADATA_FILE, indexMDLayout);
        tableScan.beforeFirst();
        while(tableScan.next()){
            if(tableScan.getString(TABLE_NAME_FIELD).equals(tableName)){
                ids.add(tableScan.getInt(INDEX_ID_FIELD));
            }
        }
        tableScan.close();
        if(ids.isEmpty()){
            throw new IllegalArgumentException("call persistIndexDescriptor(tx, tableName) first");
        }
        return ids;
    }

    /**
     * Given a unique index id, returns the associated IndexDescriptor.
     *
     * @param tx      supplies the transactional scope for database operations used in
     *                the implementation, cannot be null.  It's the client's responsibility to
     *                manage the lifecycle of this transaction.
     * @param indexId
     * @return corresponding IndexDescriptor, null if id is not associated
     * with an index.
     */
    @Override
    public IndexDescriptorBase get(TxBase tx, int indexId) {
        if(tx == null){
            throw new IllegalArgumentException("tx is null");
        }
        TableScanBase tableScan = new TableScan(tx, INDEX_METADATA_FILE, indexMDLayout);
        tableScan.beforeFirst();
        while(tableScan.next()){
            if(tableScan.getInt(INDEX_ID_FIELD) == indexId){
                String tableName = tableScan.getString(TABLE_NAME_FIELD);
                String indexName = tableScan.getString(INDEX_NAME_FIELD);
                SchemaBase ourSchema = tableMgr.getLayout(tableName, tx).schema();
                IndexDescriptorBase idb = new IndexDescriptor(tableName, ourSchema, indexName, indexName, IndexType.STATIC_HASH);
                tableScan.close();
                return idb;
            }
        }
        tableScan.close();
        return null;
    }

    /**
     * Returns an Index instance based on the persisted information associated
     * with the index descriptor id.  The Index instance isn't positioned
     * internally on any index record: use Index.beforeFirst() and next() to set
     * the index's internal state correctly.
     *
     * @param tx                supplies the transactional scope for database operations used in
     *                          the implementation, cannot be null.  It's the client's responsibility to
     *                          manage the lifecycle of this transaction.
     * @param indexDescriptorId specifies a previously persisted IndexDescriptor
     * @return Index corresponding to the indexDescriptorId.
     * @throws IllegalArgumentException if no information is associated with the
     *                                  index descriptor id
     * @see #persistIndexDescriptor(TxBase, String, String, IndexType)
     */
    @Override
    public IndexBase instantiate(TxBase tx, int indexDescriptorId) {
        if(tx == null){
            throw new IllegalArgumentException("tx is null");
        }
        IndexDescriptorBase idb = get(tx, indexDescriptorId);
        if(idb == null){
            throw new IllegalArgumentException("index descriptor not found");
        }
        return new Index(tx, idb, buckets);
    }

    /**
     * Deletes all data, catalog metadata, and index metadata associated with
     * the specified table.
     *
     * @param tx        supplies the transactional scope for database operations used in
     *                  the implementation, cannot be null.  It's the client's responsibility to
     *                  manage the lifecycle of this transaction.
     * @param tableName the table whose information is to be deleted.
     */
    @Override
    public void deleteAll(TxBase tx, String tableName) {
        if(tx == null){
            throw new IllegalArgumentException("tx is null");
        }
        Map<String, SchemaBase> ids = new HashMap<>();
        //delete metadata
        TableScanBase tableScan = new TableScan(tx, INDEX_METADATA_FILE, indexMDLayout);
        while(tableScan.next()){
            if(tableScan.getString(TABLE_NAME_FIELD).equals(tableName)){
                SchemaBase ourSchema = get(tx, tableScan.getInt(INDEX_ID_FIELD)).getIndexedTableSchema();
                ids.put(tableScan.getString(INDEX_NAME_FIELD), ourSchema);
                tableScan.delete();
            }
        }
        tableScan.close();
        //delete data
        for(String id: ids.keySet()){
            IndexDescriptorBase indexDescriptor = new IndexDescriptor(tableName, ids.get(id), id, id, IndexType.STATIC_HASH);
            IndexBase index = new Index(tx, indexDescriptor,  buckets);
            index.deleteAll();
            index.close();
        }
        //delete tables metadata
        for(String id: ids.keySet()){
            for (int i = 0; i < buckets; i++) {
                tableMgr.replace(tableName + "_" + id + "_" + i, null, tx);
            }
        }
    }
}
