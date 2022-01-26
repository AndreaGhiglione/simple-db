package simpledb;

import java.io.IOException;
import java.util.ArrayList;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    /** Our addition **/
    private TransactionId tid;
    private OpIterator child;
    private int tableId;
    private boolean flagFetch;
    private TupleDesc td;

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     *
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableId
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        this.tid = t;
        this.child = child;
        this.tableId = tableId;
        // we create the tupledesc
        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        String[] fieldAr = new String[1];
        fieldAr[0] = "countInsertedTuples";
        this.td = new TupleDesc(typeAr, fieldAr);
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
        this.flagFetch = false;  // set flag for fetchNext() method
    }

    public void close() {
        // some code goes here
        super.close();
        this.child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        this.child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int counterInserted = 0;
        if(!this.flagFetch){  // check if fetchNext method is called more than once
            while(this.child.hasNext()){
                // we should use BufferPool.insertTuple()
                try {
                    Database.getBufferPool().insertTuple(this.tid, this.tableId, this.child.next());
                }
                // we have to handle the IOException (BufferPool.insertTuple)
                catch (IOException e){
                    throw new DbException("Throw IOException");
                }
                counterInserted = counterInserted + 1;
            }
            this.flagFetch = true;
            // create the tuple
            Tuple t = new Tuple(this.td);
            t.setField(0, new IntField(counterInserted));
            return t;
        }
        else
            return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        OpIterator[] toReturn = new OpIterator[1];
        toReturn[0] = this.child;
        return toReturn;
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
