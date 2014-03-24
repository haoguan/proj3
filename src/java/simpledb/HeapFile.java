package simpledb;

import java.io.*;
import java.util.*;
import java.nio.*;
import java.nio.channels.FileChannel;

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

    private File file;
    private int fid;
    private TupleDesc td;

    private HashMap<Integer, Boolean> freePages;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        fid = f.getAbsoluteFile().hashCode();
        this.td = td;
        this.freePages = new HashMap<Integer, Boolean>();
        for (int i = 0; i < numPages(); i++) {
            freePages.put(i, false);
        }
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return fid;
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        // throw new UnsupportedOperationException("implement this");
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int offset = pid.pageNumber()*BufferPool.PAGE_SIZE;
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel fc = raf.getChannel();
            ByteBuffer buffer = ByteBuffer.allocate(BufferPool.PAGE_SIZE);
            fc.read(buffer, offset);
            HeapPage newPage = new HeapPage((HeapPageId) pid, buffer.array());
            raf.close();
            return newPage;
        } catch (Exception e) {
            System.out.println("READ PAGE ERROR");
            System.out.println(e); // MAYBE CHANGE LATER
        } 
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for proj1
        int pageNum = page.getId().pageNumber();
        int offset = pageNum*BufferPool.PAGE_SIZE;
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "rw");
            FileChannel fc = raf.getChannel();
            ByteBuffer buffer = ByteBuffer.wrap(page.getPageData());
            fc.write(buffer, offset);
        } catch (IOException e) {
            System.out.println("WRITE PAGE ERROR");
            System.out.println(e); // MAYBE CHANGE LATER
        }
        if (((HeapPage)page).getNumEmptySlots() > 0) {
            freePages.put(pageNum, true);
        } else {
            freePages.put(pageNum, false);
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) ((long)file.length() / BufferPool.PAGE_SIZE);
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        RecordId rid = t.getRecordId();
        HeapPageId pid = new HeapPageId(fid, this.numPages());
        HeapPage page = new HeapPage(pid, HeapPage.createEmptyPageData());
        boolean foundFree = false;
        for (int i = 0; i < this.numPages(); i++) {
            if (freePages.get(i) == true) {
                pid = new HeapPageId(fid, i);
                page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                foundFree = true;
                break;
            } 
        }
        int pageNum = pid.pageNumber();

        page.insertTuple(t);
        page.markDirty(true,  tid);

        if (!foundFree)
            this.writePage(page);
        if (page.getNumEmptySlots() > 0) {
            freePages.put(pageNum, true);
        } else {
            freePages.put(pageNum, false);
        }
        
        ArrayList<Page> changed = new ArrayList<Page>();
        changed.add(page);
        return changed;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        // not necessary for proj1
        RecordId rid = t.getRecordId();
        PageId pid = rid.getPageId();
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
        int pageNum = pid.pageNumber();
                
        page.deleteTuple(t);
        page.markDirty(true,  tid);
        if (((HeapPage)page).getNumEmptySlots() > 0) {
            freePages.put(pageNum, true);
        } else {
            freePages.put(pageNum, false);
        }
        return page;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        int numPages = numPages();
        return new HeapFileIterator(tid, fid, numPages);
    }

}

