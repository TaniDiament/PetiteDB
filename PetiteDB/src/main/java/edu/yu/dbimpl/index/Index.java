package edu.yu.dbimpl.index;

import edu.yu.dbimpl.query.Datum;
import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.record.*;
import edu.yu.dbimpl.tx.TxBase;
import java.sql.Types;

import static edu.yu.dbimpl.index.IndexMgr.RID_BLOCK_ID_FIELD;
import static edu.yu.dbimpl.index.IndexMgr.RID_SLOT_FIELD;

/** Specifies the interface for all Index implementations.
 *
 * Students MAY NOT modify this interface in any way.
 *
 * An index is a file of index records such that an (index) record exists in
 * the index file for each record in the "real" table that's being indexed.
 * Each index record contains a RID (the record identifier of the "real record"
 * that's being indexed).  An index record also contains a "value": this is the
 * value of the field being indexed in the "real" record.
 *
 * The Index API resembles a customized TableScan in the sense that clients can
 * position the index at the beginning of the index file and iterate
 * sequentially through its records.  Unlike a TableScan, the client specifies
 * that she is only interested in index records whose "value" field matches a
 * specified "search key".  Iteration only proceeds through records whose
 * "value" field matches the "search key".  When positioned on an index record,
 * clients can read the index record's RID and pass that value to a Scan
 * instance on the "real" table to access the corresponding "real" record.
 * Clients can also use an Index to insert new index records or to delete index
 * reccords.
 *
 * NOTE: this design precludes support of composite indices.
 */
public class Index implements IndexBase{
    private final TxBase tx;
    private final IndexDescriptorBase indexDescriptor;
    private final int buckets;
    private TableScanBase tableScan = null;
    private int type;
    private final LayoutBase layout;
    private DatumBase currentSearchKey = null;

    public Index(TxBase tx, IndexDescriptorBase indexDescriptor, int buckets){
        this.tx = tx;
        this.indexDescriptor = indexDescriptor;
        this.buckets = buckets;
        this.type = indexDescriptor.getIndexedTableSchema().type(indexDescriptor.getFieldName());
        Schema ourSchema = new Schema();
        ourSchema.addField("key", type, 16);
        ourSchema.addIntField(RID_BLOCK_ID_FIELD);
        ourSchema.addIntField(RID_SLOT_FIELD);
        layout = new Layout(ourSchema);
    }

    private String getBucketTable(DatumBase datum){
        int bucket = Math.abs(datum.hashCode() % buckets);
        return indexDescriptor.getTableName() +"_" + indexDescriptor.getFieldName() + "_" + bucket;
    }
    /**
     * Positions the index before the first record (if any) whose "value"
     * matches the value of the specified search key.  Clients must invoke a
     * "next" that returns true in order to read an index record that matches the
     * search semantics.
     * <p>
     * NOTE: "match" semantics are ".equals" semantics
     * <p>
     * NOTE: client navigational use of an index requires that they first invoke
     * beforeFirst().  Otherwise, the index has no idea as to what it should look
     * for (i.e., what it must navigate to).
     *
     * @param searchKey the search key value.
     * @throws IllegalArgumentException if the IndexDescriptor associated with this
     *                                  index implies that the searchKey parameter is incompatible with the index
     *                                  definition
     * @see IndexDescriptorBase
     * @see DatumBase#equals
     */
    @Override
    public void beforeFirst(DatumBase searchKey) {
        if(type == Types.INTEGER && searchKey.getSQLType() == Types.DOUBLE){
            int val = searchKey.asInt();
            searchKey = new Datum(val);
        }else if(type == Types.DOUBLE && searchKey.getSQLType() == Types.INTEGER){
            double val = searchKey.asDouble();
            searchKey = new Datum(val);
        }else{
            if(searchKey.getSQLType() != type){
                throw new IllegalArgumentException("invalid type");
            }
        }
        tableScan = new TableScan(tx, getBucketTable(searchKey), layout);
        tableScan.beforeFirst();
        this.currentSearchKey = searchKey;
    }

    /**
     * Moves the index cursor to the next record whose value matches the value
     * of the search key supplied in the beforeFirst method.
     *
     * @return True if such a record exists, false otherwise
     * @throws IllegalStateException if beforeFirst has not previously supplied a
     *                               search key.
     */
    @Override
    public boolean next() {
        if(tableScan == null){
            throw new IllegalStateException("need to call beforeFirst");
        }
        while(tableScan.next()){
            boolean matches = switch (type) {
                case Types.INTEGER -> tableScan.getInt("key") == currentSearchKey.asInt();
                case Types.DOUBLE -> tableScan.getDouble("key") == currentSearchKey.asDouble();
                case Types.VARCHAR -> tableScan.getString("key").equals(currentSearchKey.asString());
                case Types.BOOLEAN -> tableScan.getBoolean("key") == currentSearchKey.asBoolean();
                default -> false;
            };
            if(matches) return true;
        }
        return false;
    }

    /**
     * Returns the RID value stored in the current index record.
     *
     * @return the current index record's RID.
     * @throws IllegalStateException if the index hasn't been positioned on a
     *                               valid index record
     * @see #next()
     */
    @Override
    public RID getRID() {
        if(tableScan == null){
            throw new IllegalStateException("need to call beforeFirst");
        }
        RID returnRid = null;
        try{
            returnRid = new RID(tableScan.getInt(RID_BLOCK_ID_FIELD), tableScan.getInt(RID_SLOT_FIELD));
        }catch(Exception e){
            throw new IllegalStateException("invalid state");
        }
        return returnRid;
    }

    /**
     * Inserts an index record having the specified value and RID.
     *
     * @param value the "search key" value in the new index record.
     * @param rid   specifies the corresponding record in the "real" table
     * @throws IllegalArgumentException if the IndexDescriptor associated with
     *                                  this index implies that the valued parameter is incompatible with the
     *                                  index definition
     */
    @Override
    public void insert(DatumBase value, RID rid) {
        if(type != value.getSQLType()){
            if(!(type == Types.INTEGER && value.getSQLType() == Types.DOUBLE || type == Types.DOUBLE && value.getSQLType() == Types.INTEGER)){
                throw new IllegalArgumentException("incompatible value");
            }
        }
        String tableName = getBucketTable(value);
        TableScanBase tableScanLocal = new TableScan(tx, tableName, layout);
        tableScanLocal.beforeFirst();
        tableScanLocal.insert();
        switch (type){
            case Types.INTEGER -> tableScanLocal.setInt("key", value.asInt());
            case Types.VARCHAR -> tableScanLocal.setString("key", value.asString());
            case Types.DOUBLE ->  tableScanLocal.setDouble("key", value.asDouble());
            case Types.BOOLEAN ->  tableScanLocal.setBoolean("key", value.asBoolean());
        }
        tableScanLocal.setInt(RID_BLOCK_ID_FIELD, rid.blockNumber());
        tableScanLocal.setInt(RID_SLOT_FIELD, rid.slot());
        tableScanLocal.close();
    }

    /**
     * Deletes the index record having the specified value and RID.
     *
     * @param value the "search key" value of the record to be deleted.
     * @param rid   specifies the corresponding record in the "real" table.
     * @throws IllegalArgumentException if the IndexDescriptor associated with
     *                                  this index implies that the valued parameter is incompatible with the
     *                                  index definition
     */
    @Override
    public void delete(DatumBase value, RID rid) {
        if(type != value.getSQLType()){
            if(!(type == Types.INTEGER && value.getSQLType() == Types.DOUBLE || type == Types.DOUBLE && value.getSQLType() == Types.INTEGER)){
                throw new IllegalArgumentException("incompatible value");
            }
        }
        String tableName = getBucketTable(value);
        TableScanBase tableScanLocal = new TableScan(tx, tableName, layout);
        tableScanLocal.beforeFirst();
        boolean found = false;
        while(tableScanLocal.next() && !found){
            switch (type){
                case Types.INTEGER:
                    if(tableScanLocal.getInt("key") == value.asInt() && tableScanLocal.getInt(RID_BLOCK_ID_FIELD) == rid.blockNumber()
                        && tableScanLocal.getInt(RID_SLOT_FIELD) == rid.slot()){
                        tableScanLocal.delete();
                        found = true;
                    }
                    break;
                case Types.DOUBLE:
                    if(tableScanLocal.getDouble("key") == value.asDouble() && tableScanLocal.getInt(RID_BLOCK_ID_FIELD) == rid.blockNumber()
                            && tableScanLocal.getInt(RID_SLOT_FIELD) == rid.slot()){
                        tableScanLocal.delete();
                        found = true;
                    }
                    break;
                case Types.VARCHAR:
                    if(tableScanLocal.getString("key").equals(value.asString()) && tableScanLocal.getInt(RID_BLOCK_ID_FIELD) == rid.blockNumber()
                            && tableScanLocal.getInt(RID_SLOT_FIELD) == rid.slot()){
                        tableScanLocal.delete();
                        found = true;
                    }
                    break;
                case Types.BOOLEAN:
                    if(tableScanLocal.getBoolean("key") == value.asBoolean() && tableScanLocal.getInt(RID_BLOCK_ID_FIELD) == rid.blockNumber()
                            && tableScanLocal.getInt(RID_SLOT_FIELD) == rid.slot()){
                        tableScanLocal.delete();
                        found = true;
                    }
                    break;
            }
        }
        tableScanLocal.close();
    }

    /**
     * Deletes all index records associated with this index.
     *
     */
    @Override
    public void deleteAll() {
        for (int i = 0; i < buckets; i++){
            close();
            tableScan = new TableScan(tx, indexDescriptor.getTableName() +"_" + indexDescriptor.getFieldName() + "_" + i, layout);
            tableScan.beforeFirst();
            while(tableScan.next()){
                tableScan.delete();
            }
        }
    }

    /**
     * Closes all resources (if any) used by the index.
     */
    @Override
    public void close() {
        if(tableScan != null){
            tableScan.close();
            tableScan = null;
        }
    }
}
