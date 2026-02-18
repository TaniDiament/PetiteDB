package edu.yu.dbimpl.record;

import java.sql.Types;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
/** A Layout encapsulates and augments the record "logical schema" meta-data
 * (name, type, length) provided by SchemaBase with per-field offset
 * information, and augments this information with "physical schema" meta-data
 * such as "offsets" and "slot size".  In contrast with the logical view
 * presented by the SchemaBase, a Layout presents a physical view.  Thus the
 * "length" of an int field does have meaning for Layout, even though it's
 * undefined for SchemaBase.
 *
 * Because the Layout encapsulates the Schema supplied by the client, the
 * client transfers ownership of the schema, and may no longer mutate the
 * Schema instance.
 *
 * The first bytes of the record MUST BE used to store the Boolean-valued
 * "in-use/empty" flag.  All records fields MUST BE layed out in the order that
 * the client invoked addField.  Aside from this requirement, layout offsets
 * are implementation dependent because field order is implementation
 * dependent.  Only the sum of the offsets ("slotSize") must be the same across
 * implementations.
 *
 * The implementation MUST assign the offset of field #i+1 to be located at the
 * location at which field #1 ends: i.e., a fixed-length layout with no extra
 * padding.
 *
 * NOTE: Layout is conceptually a "value class", with all implications
 * concomitant thereto.
 */
public class Layout extends LayoutBase{
    private final Map<String, Integer> offsets;
    private final SchemaBase schema;
    private final int slotSize;
    /**
     * Constructs a Layout object from a SchemaBase.  This constructor is used
     * when a table is created. It determines the physical offset of each field
     * within the record.
     *
     * @param schema the schema of the table's records
     */
    public Layout(SchemaBase schema) {
        super(schema);
        this.schema = schema;
        offsets = new HashMap<>();
        List<String> fields = schema.fields();
        int off = 1;
        for (String field : fields) {
            offsets.put(field, off);
            int type = schema.type(field);
            if (type == Types.VARCHAR) {
                int length = schema.length(field);
                off += length + 4;
            }else if (type == Types.INTEGER) {
                off += 4;
            }else if (type == Types.DOUBLE) {
                off += 8;
            }else{
                off += 1;
            }
        }
        slotSize = off;
    }

    /** Constructs a Layout object from a SchemaBase, and assumes that the supplied
     * offset and slot size information is correct.  Intended for when the
     * metadata is retrieved from the internal catalog.
     *
     * @param schema the schema of the table's records
     * @param offsets the already-calculated offsets of the fields within a record
     * @param slotSize pre-calculated length of each record slot
     */
    public Layout(SchemaBase schema, Map<String,Integer> offsets, int slotSize) {
        super(schema, offsets, slotSize);
        this.schema = schema;
        this.offsets = offsets;
        this.slotSize = slotSize;
    }

    /**
     * Returns the encapsulated schema.
     *
     * @return the table's record schema
     */
    @Override
    public SchemaBase schema() {
        return schema;
    }

    /**
     * Returns the offset of a specified field within a record
     *
     * @param fldname the name of the field
     * @return the offset of that field within a record
     * @throws IllegalArgumentException if fldname isn't defined for this Layout
     */
    @Override
    public int offset(String fldname) {
        if(!offsets.containsKey(fldname)){
            throw new IllegalArgumentException(fldname + " not found");
        }
        return offsets.get(fldname);
    }

    /**
     * Returns the size of a slot, in bytes.
     *
     * @return the size of a slot
     */
    @Override
    public int slotSize() {
        return slotSize;
    }


    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Layout other = (Layout) obj;
        return slotSize == other.slotSize &&
                offsets.equals(other.offsets) &&
                schema.equals(other.schema);
    }


    @Override
    public int hashCode() {
        int result = schema.hashCode();
        result = 31 * result + offsets.hashCode();
        result = 31 * result + slotSize;
        return result;
    }
}
