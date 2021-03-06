package simpledb;

import java.util.*;

/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    /** Our addition **/
    private OpIterator child;
    private int afield;
    private int gfield;
    private Aggregator.Op aop;
    private Type typeGroup;
    private Aggregator agg;
    private OpIterator aggIter;


    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     * 
     * 
     * @param child
     *            The OpIterator that is feeding us tuples.
     * @param afield
     *            The column over which we are computing an aggregate.
     * @param gfield
     *            The column over which we are grouping the result, or -1 if
     *            there is no grouping
     * @param aop
     *            The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
	// some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        this.typeGroup = this.child.getTupleDesc().getFieldType(this.afield);
        switch (this.typeGroup){
            case INT_TYPE:
                if(this.gfield != -1)
                    this.agg = new IntegerAggregator(this.gfield, this.child.getTupleDesc().getFieldType(this.gfield), this.afield, this.aop);
                else
                    this.agg = new IntegerAggregator(this.gfield, null, this.afield, this.aop);
                break;
            case STRING_TYPE:
                if(this.gfield != -1)
                    this.agg = new StringAggregator(this.gfield, this.child.getTupleDesc().getFieldType(this.gfield), this.afield, this.aop);
                else
                    this.agg = new StringAggregator(this.gfield, null, this.afield, this.aop);
                break;
        }
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     *         field index in the <b>INPUT</b> tuples. If not, return
     *         {@link simpledb.Aggregator#NO_GROUPING}
     * */
    public int groupField() {
	// some code goes here
        if(this.gfield != -1)
            return this.gfield;
        return Aggregator.NO_GROUPING;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     *         of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     *         null;
     * */
    public String groupFieldName() {
	// some code goes here
        if(this.gfield != -1)
            return this.child.getTupleDesc().getFieldName(this.gfield);
	    return null;
    }

    /**
     * @return the aggregate field
     * */
    public int aggregateField() {
	// some code goes here
        return this.afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     *         tuples
     * */
    public String aggregateFieldName() {
	// some code goes here
        return this.child.getTupleDesc().getFieldName(this.afield);
    }

    /**
     * @return return the aggregate operator
     * */
    public Aggregator.Op aggregateOp() {
	// some code goes here
        return this.aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
	return aop.toString();
    }

    public void open() throws NoSuchElementException, DbException,
	    TransactionAbortedException {
	// some code goes here
        super.open();
        // first we have to merge tuples
        this.child.open();
        while(this.child.hasNext()){
            this.agg.mergeTupleIntoGroup(this.child.next());
        }
        this.aggIter = this.agg.iterator();
        this.aggIter.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
	// some code goes here
        if(this.aggIter.hasNext())
            return this.aggIter.next();
	    return null;
    }

    public void rewind() throws DbException, TransactionAbortedException {
	// some code goes here
        this.aggIter.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * 
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    public TupleDesc getTupleDesc() {
	// some code goes here
        TupleDesc td;
        if(this.gfield != -1){
            Type[] typeAr = new Type[2];
            typeAr[0] = this.child.getTupleDesc().getFieldType(this.gfield);
            typeAr[1] = Type.INT_TYPE;
            String[] fieldAr = new String[2];
            fieldAr[0] = "groupField";
            fieldAr[1] = this.aop.toString() + " " + this.child.getTupleDesc().getFieldName(this.afield);
            td = new TupleDesc(typeAr, fieldAr);
        }
        else{
            Type[] typeAr = new Type[1];
            typeAr[0] = Type.INT_TYPE;
            String[] fieldAr = new String[1];
            fieldAr[0] = this.aop.toString() + " " + this.child.getTupleDesc().getFieldName(this.afield);
            td = new TupleDesc(typeAr, fieldAr);
        }
        return td;
    }

    public void close() {
	// some code goes here
        super.close();
        this.aggIter.close();
    }

    @Override
    public OpIterator[] getChildren() {
	// some code goes here
	    OpIterator[] opIt = new OpIterator[1];
        opIt[0] = this.child;
        return  opIt;
    }

    @Override
    public void setChildren(OpIterator[] children) {
	// some code goes here
        this.child = children[0];
    }
    
}
