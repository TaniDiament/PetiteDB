package edu.yu.dbimpl.record;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Specifies the public API for the Schema implementation by requiring all
 * Schema implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * A Schema represents the "logical" record schema of a table.  A schema
 * contains the name and type of each field of the table, as well as the length
 * of each varchar field.  Schemas have no knowledge of offsets within the
 * record.
 *
 * Design note: because I haven't architected a "delete field" API, it's ok for
 * clients to invoke "addField" (and its cousins) multiple times with the
 * semantics being an override of the previous state.
 *
 * NOTE: strings MUST BE typed as java.sql.Types.VARCHAR, booleans MUST be
 * typed as java.sql.Types.BOOLEAN, doubles as java.sql.Types.DOUBLE, and
 * integers as java.sql.Types.INTEGER.
 *
 * NOTE: Schema is conceptually a "value class", with all implications
 * concomitant thereto.
 */
public class Schema extends SchemaBase{
    private final Map<String, Integer> typeMap;
    private final Map<String, Integer> fieldLength;
    private final List<String> columns;

    /** No-arg constructor.
     */
    public Schema() {
        super();
        this.typeMap = new HashMap<>();
        this.fieldLength = new HashMap<>();
        this.columns = new ArrayList<>();
    }

    /**
     * Adds a field to the schema having a specified name, type, and length.
     * Specifying "length" is very important for the "String" type because only
     * the client has knowledge of the "n" in "varchar(n)".  The server will
     * ignore the length value supplied for all fixed-char field types since it
     * will supply its own (presumabely correct) values.  No point in requiring
     * the client to be aware of the server's implementation choices.
     *
     * @param fldname the name of the field, cannot be null or empty
     * @param type    the type of the field, using the constants in {@link
     *                Types}.
     * @param length  the logical (in contrast to physical) length of a string
     *                field, must be greater than 0.  The implementation must ignore all values
     *                for all types other than VARCHAR.
     * @throws IllegalArgumentException if the specified pre-conditions aren't
     *                                  met
     */
    @Override
    public void addField(String fldname, int type, int length) {
        if(fldname==null || fldname.isEmpty() || (type == Types.VARCHAR && length<=0)){
            throw new IllegalArgumentException("Incorrect parameters");
        }
        if(type != Types.VARCHAR && type != Types.BOOLEAN && type != Types.INTEGER && type != Types.DOUBLE){
            throw new IllegalArgumentException("incorrect type");
        }
        if(!columns.contains(fldname)){
            columns.add(fldname);
        }
        typeMap.put(fldname, type);
        if(type == Types.VARCHAR){
            fieldLength.put(fldname, length);
        }
    }

    /**
     * Adds an integer field to the schema.
     *
     * @param fldname the name of the field, cannot be null or empty
     * @throws IllegalArgumentException if the specified pre-conditions aren't
     *                                  met
     */
    @Override
    public void addIntField(String fldname) {
        if(fldname==null || fldname.isEmpty()){
            throw new IllegalArgumentException("Incorrect parameters");
        }
        if(!columns.contains(fldname)){
            columns.add(fldname);
        }
        typeMap.put(fldname, Types.INTEGER);
    }

    /**
     * Adds a boolean field to the schema.
     *
     * @param fldname the name of the field, cannot be null or empty
     * @throws IllegalArgumentException if the specified pre-conditions aren't
     *                                  met
     */
    @Override
    public void addBooleanField(String fldname) {
        if(fldname==null || fldname.isEmpty()){
            throw new IllegalArgumentException("Incorrect parameters");
        }
        if(!columns.contains(fldname)){
            columns.add(fldname);
        }
        typeMap.put(fldname, Types.BOOLEAN);
    }

    /**
     * Adds a double field to the schema.
     *
     * @param fldname the name of the field, cannot be null or empty
     * @throws IllegalArgumentException if the specified pre-conditions aren't
     *                                  met
     */
    @Override
    public void addDoubleField(String fldname) {
        if(fldname==null || fldname.isEmpty()){
            throw new IllegalArgumentException("Incorrect parameters");
        }
        if(!columns.contains(fldname)){
            columns.add(fldname);
        }
        typeMap.put(fldname, Types.DOUBLE);
    }

    /**
     * Adds a string field to the schema.  The length is the logical length of
     * the field.  For example, if the field is defined as varchar(8), then its
     * length is 8.
     *
     * @param fldname the name of the field, cannot be null or empty
     * @param length  the number of chars in the varchar definition
     * @throws IllegalArgumentException if the specified pre-conditions aren't
     *                                  met
     */
    @Override
    public void addStringField(String fldname, int length) {
        if(fldname==null || fldname.isEmpty() || length<=0){
            throw new IllegalArgumentException("Incorrect parameters");
        }
        if(!columns.contains(fldname)){
            columns.add(fldname);
        }
        typeMap.put(fldname, Types.VARCHAR);
        fieldLength.put(fldname, length);
    }

    /**
     * Adds a field to the schema, retrieving "by name" its type and length
     * information from the specified schema
     *
     * @param fldname the name of the field
     * @param sch     the other schema
     * @throws IllegalArgumentException if the specified pre-conditions aren't
     *                                  met
     */
    @Override
    public void add(String fldname, SchemaBase sch) {
        int type = sch.type(fldname);
        int length = sch.length(fldname);
        this.addField(fldname, type, length);
    }

    /**
     * Adds all fields from the specified schema to the current schema.
     *
     * @param sch the other schema
     * @throws IllegalArgumentException if the specified pre-conditions aren't
     *                                  met
     */
    @Override
    public void addAll(SchemaBase sch) {
        List<String> names = sch.fields();
        for (String fldname : names) {
            int type = sch.type(fldname);
            int length = sch.length(fldname);
            this.addField(fldname, type, length);
        }
    }

    /**
     * Returns the field names in this schema.
     *
     * @return the collection of the schema's field names
     */
    @Override
    public List<String> fields() {
        return List.copyOf(columns);
    }

    /**
     * Returns true iff the specified field is in the schema
     *
     * @param fldname the name of the field
     * @return true iff the field is in the schema
     */
    @Override
    public boolean hasField(String fldname) {
        return columns.contains(fldname);
    }

    /**
     * Returns the type of the specified field, using the
     * constants in {@link Types}.
     *
     * @param fldname the name of the field
     * @return the integer type of the field
     * @throws IllegalArgumentException if the field doesn't exist.
     */
    @Override
    public int type(String fldname) {
        if(fldname==null || fldname.isEmpty() || !typeMap.containsKey(fldname)){
            throw new IllegalArgumentException("not here");
        }
        return typeMap.get(fldname);
    }

    /**
     * Returns the logical length of the specified field.
     *
     * @param fldname the name of the field
     * @return the logical length of the field
     * @throws IllegalArgumentException if the field doesn't exist.
     */
    @Override
    public int length(String fldname) {
        if(fldname==null || fldname.isEmpty() || !typeMap.containsKey(fldname)){
            throw new IllegalArgumentException("not here");
        }
        if(typeMap.get(fldname) == Types.VARCHAR){
            return fieldLength.get(fldname);
        }else {
            switch (typeMap.get(fldname)) {
                case Types.INTEGER:
                    return 4;
                case Types.DOUBLE:
                    return 8;
                case Types.BOOLEAN:
                    return 1;
                default:
                    throw new IllegalArgumentException("Unknown type");
            }
        }
    }

    /**
     * Compares this Schema with another object for equality.
     * Two Schema objects are equal if they have the same columns (in the same order),
     * the same type map, and the same field lengths.
     *
     * @param obj the object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Schema other = (Schema) obj;
        return columns.equals(other.columns) &&
                typeMap.equals(other.typeMap) &&
                fieldLength.equals(other.fieldLength);
    }

    /**
     * Returns the hash code for this Schema.
     * The hash code is computed based on the columns, type map, and field lengths.
     *
     * @return the hash code value for this object
     */
    @Override
    public int hashCode() {
        int result = columns.hashCode();
        result = 31 * result + typeMap.hashCode();
        result = 31 * result + fieldLength.hashCode();
        return result;
    }
}
