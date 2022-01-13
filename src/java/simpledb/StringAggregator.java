package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    /** Our addition **/
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> groupCountMap;  // this map will keep track of number of records per group

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupCountMap = new HashMap<>();
        switch (this.what){
            case COUNT:
                break;
            default:
                throw new IllegalArgumentException("Aggregation operator is not COUNT");
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        String fieldValue = ((StringField) tup.getField(this.afield)).getValue();
        int val;
        Field field = tup.getField(this.gbfield);
        // check if we have to initialize the maps (first tuple) and if grouping or not
        if(this.groupCountMap.size() == 0 && this.gbfield == Aggregator.NO_GROUPING){
            // if there is no grouping we set the key of the maps to null -> we will aggregate over all the tuples
            this.groupCountMap.put(null, 1);
        }

        else if(this.gbfield == Aggregator.NO_GROUPING){
            // this.what is COUNT due to check on the constructor, so
            this.groupCountMap.put(null, this.groupCountMap.get(field) + 1);
        }

        else {
            if (!this.groupCountMap.containsKey(field)) {
                // it means there is no record in the hashmap with such a key
                this.groupCountMap.put(field, 1);
            }
            else
                this.groupCountMap.put(field, this.groupCountMap.get(field) + 1);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc td;
        Tuple t;
        Field f;
        if(this.gbfield == Aggregator.NO_GROUPING){
            Type[] typeAr = new Type[1];
            typeAr[0] = Type.INT_TYPE;
            String[] fieldAr = new String[1];
            fieldAr[0] = "aggregateValue";
            td = new TupleDesc(typeAr, fieldAr);
            // iterate over hashmaps -> create tuples and add them to arraylist of tuples
            f = new IntField(this.groupCountMap.get(null));
            t = new Tuple(td);
            t.setField(0,f);
            tuples.add(t);
            return new TupleIterator(td, tuples);
        }
        else{
            // here in tupledesc we have also the groupValue
            Type[] typeAr = new Type[2];
            typeAr[0] = this.gbfieldtype;
            typeAr[1] = Type.INT_TYPE;
            String[] fieldAr = new String[2];
            fieldAr[0] = "groupValue";
            fieldAr[1] = "aggregateValue";
            td = new TupleDesc(typeAr, fieldAr);
            // iterate over hashmaps -> create tuples and add them to arraylist of tuples
            for (Field group_key : this.groupCountMap.keySet()){
                f = new IntField(this.groupCountMap.get(group_key));
                t = new Tuple(td);
                t.setField(0,group_key);
                t.setField(1,f);
                tuples.add(t);
            }
            return new TupleIterator(td, tuples);
        }
    }

}
