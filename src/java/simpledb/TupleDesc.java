package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */

    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldType = t;
            this.fieldName = n;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    private TDItem[] TDArray;
    private int size;

    public TDItem[] getTDArray(){
        return this.TDArray;
    }

    public int getSizeNoBytes(){
        return this.size;
    }


    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */

    public Iterator<TDItem> iterator() {
        Iterator<TDItem> toIter = new Iterator<TDItem>(){
            private int currentIndex = 0;

            public boolean hasNext(){
                if(currentIndex < size){
                    return true;
                }
                else{
                    return false;
                }
            }

            public TDItem next(){
                return TDArray[currentIndex++];
            }


        };
        return toIter;
    }

    private static final long serialVersionUID = 1L;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
       List<TDItem> arrList = new ArrayList<TDItem>(Arrays.asList(TDArray));
       String field;
       for(int i=0; i<typeAr.length; i++) {
           //arrList.add(new TDItem(typeAr[i], fieldAr[i]));
           if (fieldAr[i] == null) {
               field = "";
           } else {
               field = fieldAr[i];
           }

           arrList.add(new TDItem(typeAr[i], field));
       }
       this.TDArray = arrList.toArray(TDArray);
       this.size= TDArray.length;
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        List<TDItem> arrList = new ArrayList<TDItem>(Arrays.asList(TDArray));

        String field="";
        for(int i=0; i<typeAr.length; i++){
            arrList.add(new TDItem(typeAr[i], field));  // null or empty string ?

        }
        this.TDArray = arrList.toArray(TDArray);
        this.size= TDArray.length;
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return this.size;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i < 0 || i > this.size) throw new NoSuchElementException("Index out of range");
        return this.TDArray[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        if (i < 0 || i > this.size) throw new NoSuchElementException("Index out of range");
        return this.TDArray[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        for (int i=0; i<this.TDArray.length; i++){
            if(name.equals(this.TDArray[i].fieldName)){
                return i;
            }
        }
        throw new NoSuchElementException("Name not found");
    }

    /** TODO fieldNameToIndex
     * Do some checks on all fields being null ? On name being null ?
     */

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        int totalSize = 0;
        for(int i = 0; i< TDArray.length; i++){
            totalSize += TDArray[i].fieldType.getLen();  // getLen() from Type.java
        }
        return totalSize;
    }

    /**
     * Our addition
     * **/
    public TupleDesc(TDItem[] td){
        this.TDArray = Arrays.copyOf(td, td.length);
        this.size=td.length;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        /**int mergeSize = td1.size + td2.size;
        String[] mergeFieldsAr = new String[mergeSize];
        Type[] mergeTypeAr = new Type[mergeSize]; .......**/

        TDItem[] TDArray1 = td1.getTDArray();
        TDItem[] TDArray2 = td2.getTDArray();
        TDItem[] tdReturn = new TDItem[TDArray1.length + TDArray2.length];
        System.arraycopy(TDArray1, 0, tdReturn, 0, TDArray1.length);
        System.arraycopy(TDArray2, 0, tdReturn, TDArray1.length, TDArray2.length);
        TupleDesc toReturn = new TupleDesc(tdReturn);

        return toReturn;
    }


    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        TupleDesc otd = (TupleDesc) o;
        boolean flag = false;
        if (otd.getSize() == this.getSize()){
            flag = true;
            for(int i=0; i< otd.size; i++){
                if (!otd.getFieldType(i).equals(this.getFieldType(i))){
                    flag = false;
                    break;
                }
            }
        }
        return flag;

        /**TDItem[] o_arr = (TDItem[]) o;
        boolean flag=true;
        if (this.TDArray.length == o_arr.length){
            for (int i=0; i<TDArray.length; i++){
                if(!TDArray[i].fieldType.equals(o_arr[i].fieldType)){
                    flag=false;
                }
            }
        }
        else{
            flag=false;
        }
        return flag;**/
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(int i =0; i<this.TDArray.length; i++){
            sb.append(TDArray.toString());
        }

        return sb.toString();
    }
}
