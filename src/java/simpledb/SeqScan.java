package simpledb;

import java.util.*;
/** Our addition **/
import simpledb.TupleDesc.TDItem;  // we need to import this because of getTupleDesc()

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    /** Our addition **/
    private TransactionId tid;
    private int tableid;
    private String tableAlias;
    private HeapFile h;
    private DbFileIterator it;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return null;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return this.tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        this.tableid = tableid;
        this.tableAlias = tableAlias;
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        // we imported java.io.* for File class, which is needed for creating the HeapFile
        //try {
        this.h = (HeapFile) Database.getCatalog().getDatabaseFile(this.tableid);
        this.it = this.h.iterator(this.tid);
        it.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        String prefix = this.tableAlias + ".";
        int numFields = this.h.getTupleDesc().numFields();
        Type[] fieldTypes = new Type[numFields];
        String[] fieldNames = new String[numFields];
        for(int i=0; i<numFields; i++){
            fieldTypes[i] = this.h.getTupleDesc().getFieldType(i);
            fieldNames[i] = prefix + this.h.getTupleDesc().getFieldName(i);
        }
        return new TupleDesc(fieldTypes, fieldNames);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        return this.it.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        return this.it.next();
    }

    public void close() {
        // some code goes here
        this.it.close();
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        this.it.rewind();
    }
}
