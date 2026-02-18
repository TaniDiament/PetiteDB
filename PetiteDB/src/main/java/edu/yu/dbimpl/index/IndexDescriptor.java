package edu.yu.dbimpl.index;

import edu.yu.dbimpl.record.SchemaBase;
/** Specifies the public API for the IndexDescriptor implementation by
 * requiring all IndexDescriptor implementations to extend this base class.
 *
 * Students MAY NOT modify this class in any way, they must suppport EXACTLY
 * the constructor signatures specified in the base class (and NO OTHER
 * signatures).
 *
 * An IndexDescriptor is a main-memory enapsulation of database information
 * about an index.  An IndexMgr persists IndexDescriptor state (associating
 * instances with an "index id"), and instantiates an Index based on persisted
 * IndexDescriptor state.
 *
 * @author Avraham Leff
 */
public class IndexDescriptor extends IndexDescriptorBase{
    private final String tableName;
    private final SchemaBase indexedTableSchema;
    private final String indexName;
    private final String fieldName;
    private final IndexMgrBase.IndexType indexType;

    /**
     * Constructor creates an instance that encapsulates information about the
     * specified index.
     *
     * @param tableName          the name of the table on which the index is defined
     * @param indexedTableSchema the schema of the table on which the index is defined
     * @param indexName          must uniquely identify the index relative to the
     *                           specified table's scope
     * @param fieldName          the name of the indexed field
     * @param indexType          the index type
     */
    public IndexDescriptor(String tableName, SchemaBase indexedTableSchema, String indexName, String fieldName, IndexMgrBase.IndexType indexType) {
        super(tableName, indexedTableSchema, indexName, fieldName, indexType);
        this.tableName = tableName;
        this.indexedTableSchema = indexedTableSchema;
        this.indexName = indexName;
        this.fieldName = fieldName;
        this.indexType = indexType;
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public SchemaBase getIndexedTableSchema() {
        return indexedTableSchema;
    }

    @Override
    public String getIndexName() {
        return indexName;
    }

    @Override
    public String getFieldName() {
        return fieldName;
    }

    @Override
    public IndexMgrBase.IndexType getIndexType() {
        return indexType;
    }
}
