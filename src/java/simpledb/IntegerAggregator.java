package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    /** Our addition **/
    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private HashMap<Field, Integer> groupAggMap;  // this map will keep track of group and aggregate result over that group
    private HashMap<Field, Integer> groupCountMap;  // this map will keep track of number of records per group

    private static final long serialVersionUID = 1L;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        this.groupAggMap = new HashMap<>();
        this.groupCountMap = new HashMap<>();

    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        int fieldValue = ((IntField) tup.getField(this.afield)).getValue();
        int val;
        Field field = tup.getField(this.gbfield);
        // check if we have to initialize the maps (first tuple) and if grouping or not
        if(this.groupCountMap.size() == 0 && this.gbfield == Aggregator.NO_GROUPING){
            // if there is no grouping we set the key of the maps to null -> we will aggregate over all the tuples
            this.groupAggMap.put(null, fieldValue);
            this.groupCountMap.put(null, 1);
        }

        else if(this.gbfield == Aggregator.NO_GROUPING){
                val = this.groupAggMap.get(null);
                switch(this.what){
                    case COUNT:
                        this.groupCountMap.put(null, this.groupCountMap.get(field) + 1);
                        break;
                    case SUM:
                        this.groupAggMap.put(null, val + fieldValue);
                        break;
                    case AVG:
                        this.groupAggMap.put(null, val + fieldValue);
                        this.groupCountMap.put(null, this.groupCountMap.get(field) + 1);
                        break;
                    case MIN:
                        this.groupAggMap.put(null, Math.min(val, fieldValue));
                        break;
                    case MAX:
                        this.groupAggMap.put(null, Math.max(val, fieldValue));
                        break;
                }
        }

        else {
            if (!this.groupAggMap.containsKey(field)) {
                // it means there is no record in the hashmap with such a key
                this.groupAggMap.put(field, fieldValue);
                this.groupCountMap.put(field, 1);
            }
            else {
                val = this.groupAggMap.get(field);
                switch (this.what) {
                    case COUNT:
                        this.groupCountMap.put(field, this.groupCountMap.get(field) + 1);
                        break;
                    case SUM:
                        this.groupAggMap.put(field, val + fieldValue);
                        break;
                    case AVG:
                        this.groupAggMap.put(field, val + fieldValue);
                        this.groupCountMap.put(field, this.groupCountMap.get(field) + 1);
                        break;
                    case MIN:
                        this.groupAggMap.put(field, Math.min(val, fieldValue));
                        break;
                    case MAX:
                        this.groupAggMap.put(field, Math.max(val, fieldValue));
                        break;
                }
            }
        }


    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<>();
        TupleDesc td;
        Tuple t;
        Field f;
        if(this.gbfield == Aggregator.NO_GROUPING){
            // in tupledesc we have aggregateValue and the int type
            Type[] typeAr = new Type[1];
            typeAr[0] = Type.INT_TYPE;
            String[] fieldAr = new String[1];
            fieldAr[0] = "aggregateValue";
            td = new TupleDesc(typeAr, fieldAr);
            // iterate over hashmaps -> create tuples and add them to arraylist of tuples
            switch(this.what){
                case COUNT:
                    f = new IntField(this.groupCountMap.get(null));
                    break;
                case AVG:
                    f = new IntField(this.groupAggMap.get(null) / this.groupCountMap.get(null));
                    break;
                default:
                    f = new IntField(this.groupAggMap.get(null));
                    break;
            }
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
            for (Field group_key : this.groupAggMap.keySet()){
                switch(this.what){
                    case COUNT:
                        f = new IntField(this.groupCountMap.get(group_key));
                        break;
                    case AVG:
                        f = new IntField(this.groupAggMap.get(group_key) / this.groupCountMap.get(group_key));
                        break;
                    default:
                        f = new IntField(this.groupAggMap.get(group_key));
                        break;
                }
                t = new Tuple(td);
                t.setField(0,group_key);
                t.setField(1,f);
                tuples.add(t);
            }
            return new TupleIterator(td, tuples);
        }
    }

}
