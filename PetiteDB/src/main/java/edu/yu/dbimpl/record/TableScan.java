package edu.yu.dbimpl.record;

import edu.yu.dbimpl.file.BlockId;
import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.query.Datum;
import edu.yu.dbimpl.query.DatumBase;
import edu.yu.dbimpl.tx.TxBase;

import java.sql.Types;

/** Specifies the public API for the TableScan implementation by requiring all
 * TableScan implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * A TableScan is an UpdateScan implementation over records stored in a file.
 *
 * Because the TableScan encapsulates (possibly many) blocks, it is responsible
 * for formatting any new blocks that it appends to the file.
 *
 * Design note: given the PetiteDB assumptions about record layout (per lecture
 * discussion), there should be no need to persist the RID information.  Should
 * you choose to persist it, the implementation must not change the offset or
 * the block state (i.e., such meta-data must be persisted in a way that is
 * transparent to the client).
 *
 * Design note: a given instance of a TableScan need not be thread-safe.
 *
 * Design note: All get/set methods MUST throw an IllegalStateException (NOT
 * IAE) if the TableScan is not positioned on an "in-use" RecordPage slot.
 *
 * Reminder: per interface semantics and per relational database semantics,
 * TableScan.next() on an empty table will return false.
 */
public class TableScan extends TableScanBase {
    private final TxBase tx;
    private final LayoutBase layout;
    private final String fileName;
    private RID currentRID;
    private RecordPageBase currentRecordPage;
    private int currentSlot;
    /**
     * Constructor: if the file for the specified table is currently empty, the
     * Scan will append a block; otherwise, the Scan will be positioned on the
     * first block of the file.
     *
     * @param tx      Defines the transactional scope under which the scan operations
     *                will take place
     * @param tblname Specifies the prefix of the table over which the scan will
     *                be performed.  The implementation can add a suffix to the prefix to
     *                generate the full name of the file that will store the data.
     * @param layout  Defines the logical and physical schema of the
     *                table/relation
     * @see e.g., java.nio.Files#createTempFile for meaning of "prefix" and "suffix".
     * @see #TableScan(TxBase, String, LayoutBase)  TableScan()
     */
    public TableScan(TxBase tx, String tblname, LayoutBase layout) {
        super(tx, tblname, layout);
        this.tx = tx;
        this.layout = layout;
        this.fileName = tblname + "_data.tbl";
        currentRID = new RID(0, 0);
        currentSlot = -1;
        if(tx.size(fileName) <= 0){
            tx.append(fileName);
            currentRecordPage = new RecordPage(tx, new BlockId(fileName, 0), layout);
            currentRecordPage.format();
        }else{
            currentRecordPage = new RecordPage(tx, new BlockId(fileName, 0), layout);
            this.tx.pin(currentRecordPage.block());
        }
    }

    /**
     * Returns the file name (relative to the dbDirectory parameter supplied to
     * the FileMgr) that the implementation used to name the file storing the
     * table's data.
     *
     * @return name of the file used by the implementation to store the table's
     * data.
     * @see File#getName
     */
    @Override
    public String getTableFileName() {//complete
        return fileName;
    }

    /**
     * Modifies the field value in the current record.
     *
     * @param fldname the name of the field
     * @param val     the new value, expressed as a DatumBase
     * @throws IllegalArgumentException if fldname is not part of the schema or
     *                                  if the type of val is incorrect for fldname's type
     */
    @Override
    public void setVal(String fldname, DatumBase val) {//complete
        if(!tx.getBoolean(currentRecordPage.block(), currentSlot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        if(currentSlot < 0){
            throw new IllegalArgumentException("not in valid slot");
        }
        int type = val.getSQLType();
        if(!layout.schema().hasField(fldname) || layout.schema().type(fldname) != type){
            throw new IllegalArgumentException("field " + fldname + " not found or wrong type");
        }
        switch(type){
            case Types.INTEGER:
                currentRecordPage.setInt(currentSlot, fldname, val.asInt());
                break;
            case Types.VARCHAR:
                currentRecordPage.setString(currentSlot, fldname, val.asString());
                break;
            case Types.BOOLEAN:
                currentRecordPage.setBoolean(currentSlot, fldname, val.asBoolean());
                break;
            case Types.DOUBLE:
                currentRecordPage.setDouble(currentSlot, fldname, val.asDouble());
                break;
        }
        tx.unpin(currentRecordPage.block());
    }

    /**
     * Modifies the field value in the current record.
     *
     * @param fldname the name of the field
     * @param val     the new integer value
     * @throws IllegalArgumentException if fldname is not part of the schema or
     *                                  if the type of val is incorrect for fldname's type
     */
    @Override
    public void setInt(String fldname, int val) {
        if(!tx.getBoolean(currentRecordPage.block(), currentSlot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        if(currentSlot < 0){
            throw new IllegalArgumentException("not in valid slot");
        }
        if(!layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.INTEGER){
            throw new IllegalArgumentException("field " + fldname + " not found or wrong type");
        }
        currentRecordPage.setInt(currentSlot, fldname, val);
        tx.unpin(currentRecordPage.block());
    }

    /**
     * Modifies the field value in the current record.
     *
     * @param fldname the name of the field
     * @param val     the new double value
     * @throws IllegalArgumentException if fldname is not part of the schema or
     *                                  if the type of val is incorrect for fldname's type
     */
    @Override
    public void setDouble(String fldname, double val) {//complete
        if(!tx.getBoolean(currentRecordPage.block(), currentSlot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        if(currentSlot < 0){
            throw new IllegalArgumentException("not in valid slot");
        }
        if(!layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.DOUBLE){
            throw new IllegalArgumentException("field " + fldname + " not found or wrong type");
        }
        currentRecordPage.setDouble(currentSlot, fldname, val);
        tx.unpin(currentRecordPage.block());
    }

    /**
     * Modifies the field value in the current record.
     *
     * @param fldname the name of the field
     * @param val     the new boolean value
     * @throws IllegalArgumentException if fldname is not part of the schema or
     *                                  if the type of val is incorrect for fldname's type
     */
    @Override
    public void setBoolean(String fldname, boolean val) {//complete
        if(!tx.getBoolean(currentRecordPage.block(), currentSlot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        if(currentSlot < 0){
            throw new IllegalArgumentException("not in valid slot");
        }
        if(!layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.BOOLEAN){
            throw new IllegalArgumentException("field " + fldname + " not found or wrong type");
        }
        currentRecordPage.setBoolean(currentSlot, fldname, val);
        tx.unpin(currentRecordPage.block());
    }

    /**
     * Modifies the field value in the current record.
     *
     * @param fldname the name of the field
     * @param val     the new string value
     * @throws IllegalArgumentException if fldname is not part of the schema or
     *                                  if the type of val is incorrect for fldname's type
     */
    @Override
    public void setString(String fldname, String val) {//complete
        if(!tx.getBoolean(currentRecordPage.block(), currentSlot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        if(currentSlot < 0){
            throw new IllegalArgumentException("not in valid slot");
        }
        if(!layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.VARCHAR){
            throw new IllegalArgumentException("field " + fldname + " not found or wrong type");
        }
        currentRecordPage.setString(currentSlot, fldname, val);
        tx.unpin(currentRecordPage.block());
    }

    /**
     * Inserts a new record somewhere in the scan after the current record,
     * positioning the scan on that record.  If scanning a physical set of
     * records (cf TableScan), the record's "in-use" flag is set to true.
     */
    @Override
    public void insert() {//complete
        while(true){
            int slot = currentRecordPage.insertAfter(currentSlot);
            tx.unpin(currentRecordPage.block());
            if (slot >= 0) {
                currentSlot = slot;
                currentRID = new RID(currentRecordPage.block().number(), currentSlot);
                return;
            }
            BlockIdBase blk = currentRecordPage.block();
            int nextBlkNum = blk.number() + 1;
            tx.unpin(blk);
            if (nextBlkNum >= tx.size(fileName)) {
                tx.append(fileName);
                BlockIdBase newBlk = new BlockId(fileName, nextBlkNum);
                currentRecordPage = new RecordPage(tx, newBlk, layout);
                currentRecordPage.format();
            } else {
                BlockIdBase nextBlk = new BlockId(fileName, nextBlkNum);
                tx.pin(nextBlk);
                currentRecordPage = new RecordPage(tx, nextBlk, layout);
            }
            // Reset currentSlot to -1 so we search the new block from the beginning
            currentSlot = -1;
        }
    }

    /**
     * Deletes the current record from the scan.  This operation does not
     * advance the cursor.
     */
    @Override
    public void delete() {//complete
        currentRecordPage.delete(currentSlot);
        tx.unpin(currentRecordPage.block());
    }

    /**
     * Returns the id of the current record.
     *
     * @return the id of the current record
     */
    @Override
    public RID getRid() {//complete
        return currentRID;
    }

    /**
     * Positions the scan so that the current record has the specified id.
     *
     * @param rid the id of the desired record
     */
    @Override
    public void moveToRid(RID rid) {//complete
        BlockIdBase block = currentRecordPage.block();
        if(block.number() !=  rid.blockNumber()){
            tx.unpin(block);
            BlockIdBase block2 = new BlockId(fileName, rid.blockNumber());
            tx.pin(block2);
            currentRecordPage = new RecordPage(tx, block2, layout);
        }
        currentRID = rid;
        currentSlot = rid.slot();
    }

    /**
     * Positions the scan before its first record. A subsequent call to next()
     * will return the first "in-use" record.
     */
    @Override
    public void beforeFirst() {//complete
        BlockIdBase block = currentRecordPage.block();
        if(block.number() !=  0){
            tx.unpin(block);
            BlockIdBase block2 = new BlockId(fileName, 0);
            tx.pin(block2);
            currentRecordPage = new RecordPage(tx, block2, layout);
        }
        currentSlot = -1;
        currentRID = new RID(0, 0);
    }

    /**
     * Moves the scan to the next "in-use" record (if one exists)
     *
     * @return true, iff succeeded in moving to the next "in-use" record; false
     * otherwise (no such record exists).
     */
    @Override
    public boolean next() {//complete
        boolean found = false;
        int eof = tx.size(fileName);
        BlockIdBase og = currentRecordPage.block();
        BlockIdBase searchbBlock = currentRecordPage.block();
        RecordPageBase recordPage = currentRecordPage;
        int searchSlot = currentSlot;

        while(!found && searchbBlock.number() < eof){
            int next = recordPage.nextAfter(searchSlot);

            if(next == -1){
                tx.unpin(searchbBlock);
                int nextBlock = searchbBlock.number()+1;
                searchbBlock = new BlockId(fileName, nextBlock);
                recordPage = new RecordPage(this.tx, searchbBlock, layout);
                searchSlot = -1;
            }else{
                currentSlot = next;
                currentRID = new RID(searchbBlock.number(), currentSlot);
                found = true;
                if(searchbBlock.equals(og)){
                    tx.unpin(searchbBlock);
                }else{
                    tx.unpin(og);
                    currentRecordPage = new RecordPage(tx, searchbBlock, layout);
                }
            }
        }
        return  found;
    }

    /**
     * Returns the value of the specified integer field in the current record.
     *
     * @param fldname the name of the field
     * @return the field's integer value in the current record
     * @throws IllegalArgumentException if fldname is not part of the schema or
     *                                  if the type associated with fldname is incompatible with the expected
     *                                  return value.
     */
    @Override
    public int getInt(String fldname) {//complete
        if(currentSlot < 0){
            throw new IllegalArgumentException("not in valid slot");
        }
        if(!tx.getBoolean(currentRecordPage.block(), currentSlot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        if(!layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.INTEGER){
            throw new IllegalArgumentException("field " + fldname + " not found or wrong type");
        }
        int val =  currentRecordPage.getInt(currentSlot, fldname);
        tx.unpin(currentRecordPage.block());
        return val;
    }

    /**
     * Returns the value of the specified boolean field in the current record.
     *
     * @param fldname the name of the field
     * @return the field's boolean value in the current record
     * @throws IllegalArgumentException if fldname is not part of the schema or
     *                                  if the type associated with fldname is incompatible with the expected
     *                                  return value.
     */
    @Override
    public boolean getBoolean(String fldname) {//complete
        if(currentSlot < 0){
            throw new IllegalArgumentException("not in valid slot");
        }
        if(!tx.getBoolean(currentRecordPage.block(), currentSlot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        if(currentSlot < 0){
            throw new IllegalArgumentException("not in valid slot");
        }
        if(!layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.BOOLEAN){
            throw new IllegalArgumentException("field " + fldname + " not found or wrong type");
        }
        boolean val = currentRecordPage.getBoolean(currentSlot, fldname);
        tx.unpin(currentRecordPage.block());
        return val;
    }

    /**
     * Returns the value of the specified double field in the current record.
     *
     * @param fldname the name of the field
     * @return the field's double value in the current record
     * @throws IllegalArgumentException if fldname is not part of the schema or
     *                                  if the type associated with fldname is incompatible with the expected
     *                                  return value.
     */
    @Override
    public double getDouble(String fldname) {//complete
        if(currentSlot < 0){
            throw new IllegalArgumentException("not in valid slot");
        }
        if(!tx.getBoolean(currentRecordPage.block(), currentSlot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        if(!layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.DOUBLE){
            throw new IllegalArgumentException("field " + fldname + " not found or wrong type");
        }
        double val = currentRecordPage.getDouble(currentSlot, fldname);
        tx.unpin(currentRecordPage.block());
        return val;
    }

    /**
     * Returns the value of the specified string field in the current record.
     *
     * @param fldname the name of the field
     * @return the field's string value in the current record
     * @throws IllegalArgumentException if fldname is not part of the schema or
     *                                  if the type associated with fldname is incompatible with the expected
     *                                  return value.
     */
    @Override
    public String getString(String fldname) {//complete
        if(currentSlot < 0){
            throw new IllegalArgumentException("not in valid slot");
        }
        if(!tx.getBoolean(currentRecordPage.block(), currentSlot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        if(!layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.VARCHAR){
            throw new IllegalArgumentException("field " + fldname + " not found or wrong type");
        }
        String val = currentRecordPage.getString(currentSlot, fldname);
        tx.unpin(currentRecordPage.block());
        return val;
    }

    /**
     * Returns the value of the specified field in the current record.  The
     * value is expressed as a DatumBase.
     *
     * @param fldname the name of the field
     * @return the value of that field, expressed as a DatumBase.
     * @throws IllegalArgumentException if fldname is not part of the schema
     */
    @Override
    public DatumBase getVal(String fldname) {//complete
        if(!tx.getBoolean(currentRecordPage.block(), currentSlot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        if(currentSlot < 0){
            throw new IllegalArgumentException("not in valid slot");
        }
        if(!layout.schema().hasField(fldname)){
            throw new IllegalArgumentException("field " + fldname + " not found");
        }
        int type = layout.schema().type(fldname);
        DatumBase d = switch (type) {
            case Types.INTEGER -> new Datum(currentRecordPage.getInt(currentSlot, fldname));
            case Types.DOUBLE -> new Datum(currentRecordPage.getDouble(currentSlot, fldname));
            case Types.VARCHAR -> new Datum(currentRecordPage.getString(currentSlot, fldname));
            case Types.BOOLEAN -> new Datum(currentRecordPage.getBoolean(currentSlot, fldname));
            default -> null;
        };
        tx.unpin(currentRecordPage.block());
        return d;
    }

    /**
     * Returns true iff the scan has the specified field.
     *
     * @param fldname the name of the field
     * @return true iff the scan has that field, false otherwise
     */
    @Override
    public boolean hasField(String fldname) {//complete
        return layout.schema().hasField(fldname);
    }

    /**
     * Returns the type of the specified field.
     * <p>
     * return the type of the wrapped value as a value from the set of constants
     * defined by java.sql.Types
     *
     * @param fldname the name of the field.
     * @throws IllegalArgumentException if fldname is not part of the schema
     */
    @Override
    public int getType(String fldname) {//complete
        if(!layout.schema().hasField(fldname)){
            throw new IllegalArgumentException("field " + fldname + " not found");
        }
        return layout.schema().type(fldname);
    }

    /**
     * Terminate the scan processing (and automatically also close all
     * underlying scans, if any), closing all resources, including unpinning any
     * pinned blocks.
     */
    @Override
    public void close() {
        if(currentRecordPage != null){
            tx.unpin(currentRecordPage.block());
        }
    }
}
