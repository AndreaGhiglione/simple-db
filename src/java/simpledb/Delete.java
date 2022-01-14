package simpledb;

import java.io.IOException;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    /** Our addition **/
    private TransactionId tid;
    private OpIterator child;
    private TupleDesc td;
    private boolean flagFetch; // same reasoning of Insert.java

    private static final long serialVersionUID = 1L;

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, OpIterator child) {
        // some code goes here
        this.tid = t;
        this.child = child;
        // we create the tupledesc
        Type[] typeAr = new Type[1];
        typeAr[0] = Type.INT_TYPE;
        String[] fieldAr = new String[1];
        fieldAr[0] = "countInsertedTuples";
        this.td = new TupleDesc(typeAr, fieldAr);
        this.flagFetch = false;
    }

    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        this.child.open();
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int countDeleted = 0;
        if(!this.flagFetch){
            while(this.child.hasNext()){
                // we should use BufferPool.insertTuple()
                try {
                    Database.getBufferPool().deleteTuple(this.tid, this.child.next());
                }
                // we have to handle the IOException (BufferPool.insertTuple)
                catch (IOException e){
                    throw new DbException("Throw IOException");
                }
                countDeleted = countDeleted + 1;
            }
            this.flagFetch = true;
            // create the tuple
            Tuple t = new Tuple(this.td);
            t.setField(0, new IntField(countDeleted));
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
