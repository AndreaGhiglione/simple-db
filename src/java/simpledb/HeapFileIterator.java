package simpledb;

import java.util.*;

public class HeapFileIterator extends AbstractDbFileIterator{

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

    /**
     * Open the iterator, must be called before readNext.
     */
    public void open() throws DbException, TransactionAbortedException {
       this.currPage = -1;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        close();
        open();
    }

    @Override
    protected Tuple readNext() throws DbException, TransactionAbortedException {
        if(this.tupleIterat != null && !this.tupleIterat.hasNext()){
            this.tupleIterat = null;
        }
        while(this.tupleIterat == null && this.currPage < this.heapFile.numPages() - 1) {
            this.currPage = this.currPage + 1;
            this.HeapPageIdStart = new HeapPageId(this.heapFile.getId(), this.currPage);
            try {
                this.HeapPageStart = (HeapPage) Database.getBufferPool()
                        .getPage(this.tid, HeapPageIdStart, Permissions.READ_ONLY);
                this.tupleIterat = HeapPageStart.iterator();
            } catch (TransactionAbortedException a) {
                System.out.println("Transaction Aborted Exception");
            } catch (DbException d) {
                System.out.println("Too many pages have been requested");
            }
            if (!this.tupleIterat.hasNext())
                this.tupleIterat = null;
        }
        if(this.tupleIterat == null) return null;
        return this.tupleIterat.next();
    }

    @Override
    public void close() {
        // Ensures that a future call to next() will fail
        super.close();
        this.tupleIterat = null;
        this.currPage = Integer.MAX_VALUE;

    }
}