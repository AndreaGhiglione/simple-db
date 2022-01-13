package simpledb;

import javax.xml.crypto.Data;
import java.awt.*;
import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    /** Our addition **/
    private File f;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.f.getAbsoluteFile().hashCode();  // this is the tableId (also the HeapFileId since there is 1 HeapFile per Table)
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        // we calculate offset by multiplying the page size by the page number
        int offset = BufferPool.getPageSize() * pid.getPageNumber();
        byte[] pageContent = new byte[BufferPool.getPageSize()];
        Page pageToReturn = null;
        try {
            RandomAccessFile fileToRead = new RandomAccessFile(this.f, "r");
            fileToRead.seek(offset);
            fileToRead.read(pageContent);
            fileToRead.close();
            pageToReturn = new HeapPage((HeapPageId) pid, pageContent);
        }
        catch(FileNotFoundException f){
            System.out.println("File not found");
        }
        catch(IOException i){
            System.out.println("Wrong offset");
        }

        return pageToReturn;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        int offset = BufferPool.getPageSize() * this.numPages();
        RandomAccessFile fileToWrite = new RandomAccessFile(this.f, "rw");
        fileToWrite.seek(offset);
        fileToWrite.write(page.getPageData());
        fileToWrite.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(this.f.length() * 1.0/ BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        PageId pid;
        HeapPage hp = null;
        ArrayList<Page> toReturn = new ArrayList<>();
        // first, we search for a heapPage with empty slot(s)
        for(int i=0; i<this.numPages(); i++){
            pid = new HeapPageId(getId(), i);
            HeapPage curr_hp = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
            if(curr_hp.getNumEmptySlots() > 0) {
                hp = curr_hp;
                break; // heapPage found
            }
        }
        if(hp == null){
            // it means we didn't find an heapPage with empty slot(s)
            HeapPageId new_hpId = new HeapPageId(getId(), this.numPages());  // heapfileId -> tableId and a new page number
            HeapPage new_hp = new HeapPage(new_hpId, HeapPage.createEmptyPageData());
            new_hp.insertTuple(t);
            // we have to append the new heapPage physically on disk
            int offset = BufferPool.getPageSize() * this.numPages();
            RandomAccessFile fileToWrite = new RandomAccessFile(this.f, "rw");
            fileToWrite.seek(offset);
            fileToWrite.write(new_hp.getPageData());
            fileToWrite.close();
            toReturn.add(new_hp);
            return toReturn;
        }
        else{
            // we found an heapPage with some empty slots
            hp.insertTuple(t);
            toReturn.add(hp);
            return toReturn;
        }
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        // we retrieve heapPage from the getPage method as we did in insertTuple()
        HeapPage hp = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        hp.deleteTuple(t);
        ArrayList<Page> toReturn = new ArrayList<>();
        toReturn.add(hp);
        return toReturn;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        // take the tableId of this HeapFile (getId) , go to BufferPool and iterate over PageIds; if a pageId has a TableId (getTableId) == getId then we take that page to
        // start iterating

        return new HeapFileIterator(this, tid);

    }

}

