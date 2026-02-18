package edu.yu.dbimpl.query;

import edu.yu.dbimpl.file.PageBase;

import java.sql.Types;
import java.util.Arrays;

/** A "value class" encapsulating a single database value (datum): the value
 * can be exactly one of a set of possible Java types.  Although the
 * getters/setters are necessarily specified in terms of Java types, the API
 * requires that a datum be associated with an SQL type.  Specifically: strings
 * MUST BE typed as java.sql.Types.VARCHAR, binary arrays MUST be typed as
 * java.sql.Types.VARBINARY, booleans MUST be typed as java.sql.Types.BOOLEAN,
 * doubles as java.sql.Types.DOUBLE, and integers as java.sql.Types.INTEGER.
 * This approach follows that specified by SchemaBase.
 *
 * NOTE: a DatumBase is conceptually a "value class", with all implications
 * concomitant thereto.
 *
 * Design note: while the Datum constructors are passed Java objects, the asX()
 * methods returns primitive values corresponding to the object parameters.
 * Given constructor parameter Y, the semantics of a asX() method is specified
 * by JDK Y.xValue().  (Strings are convertible to byte arrays via
 * String.getBytes(), using the DBMS Charset), and byte arrays are convertible
 * to Strings via the appropriate String constructor.) The implementation must
 * throw a ClassCastException if the object parameters do not support the
 * implied Y.xValue() method invocation.
 *
 * Design note: the semantics of Datum.equals are "compares this Datum to the
 * specified object. The result is true if and only if the argument is not null
 * and is a Datum object that contains the same (by ".equals" semantics)
 * wrapped value as this Datum instance."  In other words, no "implict type
 * conversion" is done by the DBMS.
 *
 * Design note: a case can be made that the semantics of compareTo should be
 * based on a Datum's primitive value and e.g., allow comparison between 42.0
 * and 42.  That said, having equivalent semantics for .equals and compareTo is
 * so important that Datum.compareTo semantics MUST be based on the object
 * passed to the Datum constructor.  Datum.compareTo MUST throw a
 * ClassCastException if the constructor objects are not of the same type.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * @author Avraham Leff
 */
public class Datum extends DatumBase{
    private Integer ival;
    private String sval;
    private Boolean bval;
    private Double dval;
    private byte[] array;
    private final datumType type;

    private enum datumType{
        INTEGER, STRING, BOOLEAN, DOUBLE, BYTE
    }

    public Datum(Integer ival) {
        super(ival);
        this.ival = ival;
        this.dval = ival.doubleValue();
        this.type = datumType.INTEGER;
    }
    public Datum(String sval) {
        super(sval);
        this.sval = sval;
        this.type = datumType.STRING;
    }
    public Datum(Boolean bval) {
        super(bval);
        this.bval = bval;
        this.type = datumType.BOOLEAN;
    }
    /** Constructor: wrap a Java double.
     */
    public Datum(Double dval) {
        super(dval);
        this.dval = dval;
        this.ival = dval.intValue();
        this.type = datumType.DOUBLE;
    }

    /** Constructor: wrap a binary array
     */
    public Datum(byte[] array) {
        super(array);
        this.array = array;
        this.type = datumType.BYTE;
    }

    /** Returns the value encapsulated by the DatumBase with semantics specified
     * by "design note" above.
     *
     * @throws ClassCastException if the encapsulated value is the wrong type.
     */
    @Override
    public int asInt() {
        if(type != datumType.INTEGER && type != datumType.DOUBLE){
            throw new ClassCastException("Type mismatch");
        }
        return ival;
    }

    @Override
    public boolean asBoolean() {
        if(type != datumType.BOOLEAN){
            throw new ClassCastException("Type mismatch");
        }
        return bval;
    }

    @Override
    public double asDouble() {
        if(type != datumType.INTEGER && type != datumType.DOUBLE){
            throw new ClassCastException("Type mismatch");
        }
        return dval;
    }

    @Override
    public String asString() {
        if(type != datumType.STRING &&  type != datumType.BYTE){
            throw new ClassCastException("Type mismatch");
        }
        if(type == datumType.BYTE){
            return new String(array, PageBase.CHARSET);
        }else{
            return sval;
        }
    }

    @Override
    public byte[] asBinaryArray() {
        if(type != datumType.STRING &&  type != datumType.BYTE){
            throw new ClassCastException("Type mismatch");
        }
        if(type == datumType.BYTE){
            return array;
        }else{
            return sval.getBytes(PageBase.CHARSET);
        }
    }
    /** Return the type of the wrapped value as a value from the set of constants
     * defined by java.sql.Types
     *
     * @return the type of the datum.
     */
    @Override
    public int getSQLType() {
        if(type == datumType.BYTE){
            return Types.VARBINARY;
        }else if(type == datumType.INTEGER){
            return Types.INTEGER;
        }else if(type == datumType.BOOLEAN){
            return Types.BOOLEAN;
        }else if(type == datumType.DOUBLE){
            return Types.DOUBLE;
        }else{
            return Types.VARCHAR;
        }
    }

    @Override
    public int compareTo(DatumBase o) {
        if(o == null){
            throw new NullPointerException("Datum is null");
        }
        Datum d = (Datum)o;
        if(d.type != this.type){
            throw new ClassCastException("Type mismatch");
        }
        return switch (type) {
            case INTEGER -> ival.compareTo(d.ival);
            case BOOLEAN -> bval.compareTo(d.bval);
            case DOUBLE -> dval.compareTo(d.dval);
            case BYTE -> Arrays.compare(array, d.array);
            case STRING -> sval.compareTo(d.sval);
        };
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        Datum other = (Datum) obj;

        if (this.type != other.type) {
            return false;
        }

        switch (this.type) {
            case INTEGER: return this.ival.equals(other.ival);
            case STRING:  return this.sval.equals(other.sval);
            case BOOLEAN: return this.bval.equals(other.bval);
            case DOUBLE:  return this.dval.equals(other.dval);
            case BYTE:    return java.util.Arrays.equals(this.array, other.array);
            default:      return false;
        }
    }

    @Override
    public int hashCode(){
        int hash = 7;
        hash = 31 * hash + type.ordinal();
        switch (type){
            case INTEGER -> hash = 31 * hash + ival.hashCode();
            case STRING -> hash = 31 * hash + sval.hashCode();
            case BOOLEAN -> hash = 31 * hash + bval.hashCode();
            case DOUBLE -> hash = 31 * hash + dval.hashCode();
            case BYTE -> hash = 31 * hash + java.util.Arrays.hashCode(array);
        }
        return hash;
    }

}
