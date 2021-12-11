package simpledb;

import java.util.*;

public class HeapFileIterator extends AbstractDbFileIterator{

    //private HeapPage PageStart;
    //private HeapPageId PageStartId;
    private TransactionId tid;
    private HeapPageId HeapPageIdStart;
    private HeapPage HeapPageStart;
    private Iterator<Tuple> tupleIterat;
    private int currPage;
    private int numPages;
    private HeapFile heapFile;


    public HeapFileIterator(HeapFile heapFile, TransactionId tid) {
        this.heapFile = heapFile;
        this.tid = tid;
        this.numPages = this.heapFile.numPages();
    }

    public void open(){
        // we create a pid by using this tableId and pageNumber=0, then we do a getPage in the BufferPool
        this.currPage = 0;
        this.HeapPageIdStart = new HeapPageId(this.heapFile.getId(), this.currPage);
        try{
            this.HeapPageStart = (HeapPage) Database.getBufferPool().getPage(this.tid, HeapPageIdStart, Permissions.READ_ONLY);;
            this.tupleIterat = HeapPageStart.iterator();
        }
        catch (TransactionAbortedException a){
            System.out.println("Transaction Aborted Exception");
        }
        catch (DbException d){
            System.out.println("Too many pages have been requested");
        }


    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        this.currPage = 0;
        this.HeapPageIdStart = new HeapPageId(this.heapFile.getId(), this.currPage);
        try{
            this.HeapPageStart = (HeapPage) Database.getBufferPool().getPage(this.tid, HeapPageIdStart, Permissions.READ_ONLY);
            this.tupleIterat = HeapPageStart.iterator();
        }
        catch (TransactionAbortedException a){
            System.out.println("Transaction Aborted Exception");
        }
        catch (DbException d){
            System.out.println("Too many pages have been requested");
        }
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if(this.tupleIterat == null){
            return null;
        }
        if(this.tupleIterat.hasNext())
            return this.tupleIterat.next();
        else{
            if(currPage + 1 <= this.numPages){
                this.currPage = this.currPage + 1;
                this.HeapPageIdStart = new HeapPageId(this.heapFile.getId(), this.currPage);
                try{
                    this.HeapPageStart = (HeapPage) Database.getBufferPool().getPage(this.tid, HeapPageIdStart, Permissions.READ_ONLY);
                    this.tupleIterat = HeapPageStart.iterator();
                }
                catch (TransactionAbortedException a){
                    System.out.println("Transaction Aborted Exception");
                }
                catch (DbException d){
                    System.out.println("Too many pages have been requested");
                }
                if(this.tupleIterat.hasNext())
                    return this.tupleIterat.next();
                else
                    return null;
            }
            else{
                return null;
            }
        }
    }

    @Override
    public void close() {
        // Ensures that a future call to next() will fail
        super.close();
        this.tupleIterat = null;
        this.currPage = 0;

    }
}
