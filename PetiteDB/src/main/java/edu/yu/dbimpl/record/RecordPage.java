package edu.yu.dbimpl.record;

import edu.yu.dbimpl.file.BlockIdBase;
import edu.yu.dbimpl.tx.TxBase;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Specifies the public API for the RecordPage implementation by requiring all
 * RecordPage implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * A RecordPage manages "record-slots" at block granularity.
 *
 * Implementations MUST use the slotted page design for fixed-length fields
 * discussed in lecture and textbook.
 *
 * Design note: the only methods available for manipulating the crucial
 * "in-use" flags are insertAfter() and delete().  Setting (or getting)
 * slot-values without first setting the "in-use" flag will not accomplish what
 * you're trying to accomplish.
 *
 * Related design note: any get() or set() method that's invoked on a valid
 * slot number when the slot isn't "in use" MUST throw an IllegalStateException
 * (NOT IAE).  The same holds true for the delete() API.
 *
 * Design note: because a RecordPage has access to a Layout and Schema, ALL
 * getters and setters MUST throw an IAE if the specified field name's type
 * doesn't correspond to the method signature.  For example: when the client
 * supplies a field name to getInt(fldname) whose type is a boolean, the
 * implementation MUST throw an IAE.  As a special case of this semantic, the
 * implementation throws an IAE if the specified field name is not part of the
 * schema.
 *
 * Usage note: clients are responsible for invoking transaction lifecycle
 * methods (commit/rollback) as the record module have no way of inferring what
 * the client wants.  However, clients delegate responsibility for pinning
 * encapsulated blocks to the RecordPage implementation and responsibility for
 * unpinning to the TableScan implementation.  Specifically: RecordPage
 * getter/setter APIs imply pin semantics and closing a scan implies unpin
 * semantics.
 *
 * Design note: can help to consider the RecordPageBase API as moving parts of
 * the TxBase API "up a level" such that clients can get/set values in terms of
 * field names rather than block locations.
 *
 * Design note: implementors should view THEMSELVES as being the "RecordPage
 * client": meaning, database clients shouldn't be using a RecordPage;
 * TableScan implementations are the intended clients of RecordPage.
 * RecordPage is only exposed as public for pedagogic (and testing) reasons.
 */
public class RecordPage extends RecordPageBase{
    private final BlockIdBase block;
    private final TxBase tx;
    private final LayoutBase layout;
    /**
     * Constructor.
     * <p>
     * IMPORTANT: If the encapsulated block is being used for the first time, the
     * CLIENT is responsible for invoking "format()" before invoking other
     * methods on the RecordPage.  Failure to do so implies that the semantics of
     * subsequent method invocations are undefined.
     *
     * @param tx     Defines the transaction scope in which operations on the block
     *               will take place.  The client passing the tx continues to be responsible
     *               for transaction lifeycle behavior: commit versus rollback.  The
     *               RecordPageBase implementation uses the transaction to "get its work done".
     * @param blk    The block in which the record is stored
     * @param layout Holds the physical and logical record schema
     * @throws IllegalArgumentException if block is too small to hold at least
     *                                  one record.
     */
    public RecordPage(TxBase tx, BlockIdBase blk, LayoutBase layout) {
        super(tx, blk, layout);
        this.block = blk;
        this.tx = tx;
        this.layout = layout;
        if(tx.blockSize() < layout.slotSize()){
            throw new IllegalArgumentException("Block size must be >= slot size");
        }
    }

    /**
     * Return the integer value stored for the specified field of a specified
     * slot.
     *
     * @param slot    specifies location storing the value, must be non-negative.
     * @param fldname the name of the field, must be defined on the page's layout.
     * @return the integer stored in that field
     * @throws IllegalArgumentException if pre-conditions are violated.
     */
    @Override
    public int getInt(int slot, String fldname) {
        if(slot < 0 || !layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.INTEGER){
            throw new IllegalArgumentException("invalid input");
        }
        tx.pin(block);
        if(!tx.getBoolean(block, slot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        int offset = slot * layout.slotSize();
        offset += layout.offset(fldname);
        return tx.getInt(block, offset);
    }

    /**
     * Return the string value stored for the specified field of the specified
     * slot.
     *
     * @param slot    specifies location storing the value, must be non-negative.
     * @param fldname the name of the field, must be defined on the page's layout.
     * @return the string stored in that field
     * @throws IllegalArgumentException if pre-conditions are violated.
     */
    @Override
    public String getString(int slot, String fldname) {
        if(slot < 0 || !layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.VARCHAR){
            throw new IllegalArgumentException("invalid input");
        }
        tx.pin(block);
        if(!tx.getBoolean(block, slot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        int offset = slot * layout.slotSize();
        offset += layout.offset(fldname);
        return tx.getString(block, offset);
    }

    /**
     * Return the boolean value stored for the specified field of a specified
     * slot.
     *
     * @param slot    specifies location storing the value, must be non-negative.
     * @param fldname the name of the field, must be defined on the page's layout.
     * @return the boolean stored in that field
     * @throws IllegalArgumentException if pre-conditions are violated.
     */
    @Override
    public boolean getBoolean(int slot, String fldname) {
        if(slot < 0 || !layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.BOOLEAN){
            throw new IllegalArgumentException("invalid input");
        }
        tx.pin(block);
        if(!tx.getBoolean(block, slot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        int offset = slot * layout.slotSize();
        offset += layout.offset(fldname);
        return tx.getBoolean(block, offset);
    }

    /**
     * Return the double value stored for the specified field of a specified
     * slot.
     *
     * @param slot    specifies location storing the value, must be non-negative.
     * @param fldname the name of the field, must be defined on the page's layout.
     * @return the double stored in that field
     * @throws IllegalArgumentException if pre-conditions are violated.
     */
    @Override
    public double getDouble(int slot, String fldname) {
        if(slot < 0 || !layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.DOUBLE){
            throw new IllegalArgumentException("invalid input");
        }
        tx.pin(block);
        if(!tx.getBoolean(block, slot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        int offset = slot * layout.slotSize();
        offset += layout.offset(fldname);
        return tx.getDouble(block, offset);
    }

    /**
     * Stores an integer at the specified field of the specified slot.
     *
     * @param slot    specifies location storing the value, must be non-negative.
     * @param fldname must be defined on the page's layout
     * @param val     the integer value stored in that field
     * @throws IllegalArgumentException if pre-conditions are violated.
     */
    @Override
    public void setInt(int slot, String fldname, int val) {
        if(slot < 0 || !layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.INTEGER){
            throw new IllegalArgumentException("invalid input");
        }
        tx.pin(block);
        if(!tx.getBoolean(block, slot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        int offset = slot * layout.slotSize();
        offset += layout.offset(fldname);
        tx.setInt(block, offset, val, true);
    }

    /**
     * Stores a string at the specified field of the specified slot.
     *
     * @param slot    specifies location storing the value, must be non-negative.
     * @param fldname must be defined on the page's layout
     * @param val     the string value stored in that field
     * @throws IllegalArgumentException if pre-conditions are violated or if the
     *                                  logical length of the string exceeds the logical length specified in the
     *                                  schema.
     */
    @Override
    public void setString(int slot, String fldname, String val) {
        if(slot < 0 || !layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.VARCHAR || val.length() > layout.schema().length(fldname)){
            throw new IllegalArgumentException("invalid input");
        }
        tx.pin(block);
        if(!tx.getBoolean(block, slot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        int offset = slot * layout.slotSize();
        offset += layout.offset(fldname);
        tx.setString(block, offset, val, true);
    }

    /**
     * Stores a boolean at the specified field of the specified slot.
     *
     * @param slot    specifies location storing the value, must be non-negative.
     * @param fldname must be defined on the page's layout
     * @param val     the boolean value stored in that field
     * @throws IllegalArgumentException if pre-conditions are violated.
     */
    @Override
    public void setBoolean(int slot, String fldname, boolean val) {
        if(slot < 0 || !layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.BOOLEAN){
            throw new IllegalArgumentException("invalid input");
        }
        tx.pin(block);
        if(!tx.getBoolean(block, slot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        int offset = slot * layout.slotSize();
        offset += layout.offset(fldname);
        tx.setBoolean(block, offset, val, true);
    }

    /**
     * Stores a double at the specified field of the specified slot.
     *
     * @param slot    specifies location storing the value, must be non-negative.
     * @param fldname must be defined on the page's layout
     * @param val     the double value stored in that field
     * @throws IllegalArgumentException if pre-conditions are violated.
     */
    @Override
    public void setDouble(int slot, String fldname, double val) {
        if(slot < 0 || !layout.schema().hasField(fldname) || layout.schema().type(fldname) != Types.DOUBLE){
            throw new IllegalArgumentException("invalid input");
        }
        tx.pin(block);
        if(!tx.getBoolean(block, slot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        int offset = slot * layout.slotSize();
        offset += layout.offset(fldname);
        tx.setDouble(block, offset, val, true);
    }

    /**
     * Deletes the specified slot by setting its "in-use" flag to "not in use".
     *
     * @param slot uniquely identifies the record slot.
     * @throws IllegalArgumentException if slot is negative.
     */
    @Override
    public void delete(int slot) {
        if(slot < 0){
            throw new IllegalArgumentException("input must be greater than or equal to 0");
        }
        if(!tx.getBoolean(block, slot*layout.slotSize())){
            throw new IllegalStateException("slot not in use");
        }
        tx.pin(block);
        tx.setBoolean(block, slot*layout.slotSize(), false, true);
    }

    /**
     * Initializes all record slots in the block: i.e., all integers are set to
     * 0, all booleans to false, all doubles to 0.0, all strings to the empty
     * string, and all slots to "empty".
     * <p>
     * These operations should not be logged (from a transactional point of view)
     * because we consider the old values to be meaningless.
     */
    @Override
    public void format() {
        int num = tx.blockSize()/layout.slotSize();
        int offset = 0;
        tx.pin(block);
        List<String> fields = layout.schema().fields();
        Map<String, Integer> types = new HashMap<>();
        for(String field : fields){
            types.put(field, layout.schema().type(field));
        }
        for(int i = 0; i < num; i++){
            tx.setBoolean(block, offset, false, false);
            offset++;
            for(String field : fields){
                int type = types.get(field);
                switch(type){
                    case Types.BOOLEAN:
                        tx.setBoolean(block, offset, false, false);
                        offset++;
                        break;
                    case Types.INTEGER:
                        tx.setInt(block, offset, 0, false);
                        offset+=4;
                        break;
                    case Types.DOUBLE:
                        tx.setDouble(block, offset, 0.0, false);
                        offset+=8;
                        break;
                    case Types.VARCHAR:
                        tx.setString(block, offset, "", false);
                        offset+=4;
                        offset+=layout.schema().length(field);
                        break;
                }
            }

        }
    }

    /**
     * Search the block, starting from the specified slot, for an "in-use" slot.
     *
     * @param slot uniquely identifies the record slot from which the search will
     *             begin.  To search from the beginning of the block, set this parameter to
     *             BEFORE_FIRST_SLOT.
     * @return Returns the location of the first "in-use" slot AFTER the
     * specified slot: if all slots are "empty", returns -1 as a sentinel value.
     * @throws IllegalArgumentException if slot is less than -1.
     */
    @Override
    public int nextAfter(int slot) {
        if(slot < -1){
            throw new IllegalArgumentException("input must be greater than or equal to -1");
        }
        boolean found = false;
        tx.pin(block);
        int size = layout.slotSize();
        int offset = 0;
        if(slot != BEFORE_FIRST_SLOT){
            offset = slot*size + size;
        }
        int returnINT = 0;
        while(!found && offset+size <= tx.blockSize()){
            found = tx.getBoolean(block, offset);
            returnINT = offset;
            offset+=size;
        }
        if(!found){
            return -1;
        }else {
            return returnINT/size;
        }
    }

    /**
     * Search the block, starting from the specified slot, for an "empty" slot.
     *
     * @param slot uniquely identifies the record slot from which the search will
     *             begin.  To search from the beginning of the block, set this parameter to
     *             BEFORE_FIRST_SLOT.
     * @return Returns the location of the first "empty" slot AFTER the specified
     * slot AND sets the state of the slot to "in-use"; if all slots are
     * "in-use", returns -1 as a sentinel value.
     * @throws IllegalArgumentException if slot is less than -1.
     */
    @Override
    public int insertAfter(int slot) {
        if(slot < -1){
            throw new IllegalArgumentException("input must be greater than or equal to -1");
        }
        boolean found = false;
        tx.pin(block);
        int size = layout.slotSize();
        int offset = 0;
        if(slot != BEFORE_FIRST_SLOT){
            offset += (slot*size + size);
        }
        int returnINT = 0;
        while(!found && offset+size <= tx.blockSize()){
            boolean next = tx.getBoolean(block, offset);
            if(!next){
                found = true;
                tx.setBoolean(block, offset, true, true);
                returnINT = offset;
            }else{
                offset+=size;
            }
        }
        if(!found){
            return -1;
        }else {
            return returnINT/size;
        }
    }

    /**
     * Returns the block associated with the RecordPageBase instance.
     *
     * @return the block
     */
    @Override
    public BlockIdBase block() {
        return block;
    }
}
