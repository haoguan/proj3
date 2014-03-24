package simpledb;
import java.io.Serializable;
import java.util.*;

/**
 * HeapFileIterator iterates through a
 * implement.
 */
public class HeapFileIterator implements DbFileIterator {

    boolean open = false;
    TransactionId tid;
    int tableId;
    PageId pid;
    int numPages;
    int pgNo = 0; //counter for fetching from bufferpool.
    HeapPage curPage;
    Iterator<Tuple> curPageIter = null;

    public HeapFileIterator(TransactionId tid, int tableId, int numPages) {
        this.tid = tid;
        this.tableId = tableId;
        this.numPages = numPages;
    }    

    /**
     * Opens the iterator
     * @throws DbException when there are problems opening/accessing the database.
     */
    public void open() throws DbException, TransactionAbortedException {
        //preparation.
        open = true;
        generateNextPage();
    }

    /** @return true if there are more tuples available. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        if (!open) 
          return false; //not yet opened.
        if (curPageIter != null && curPageIter.hasNext()) {
            return true;
        }
        while (curPageIter != null && !curPageIter.hasNext() && pgNo < numPages) {
            generateNextPage();
        }
        if (curPageIter != null && curPageIter.hasNext()) {
            return true;
        }
        return false;
    }

    /**
     * Gets the next tuple from the operator (typically implementing by reading
     * from a child operator or an access method).
     *
     * @return The next tuple in the iterator.
     * @throws NoSuchElementException if there are no more tuples
     */
    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
        if (open == false) {
            throw new NoSuchElementException();
        }
        if (curPageIter != null && curPageIter.hasNext()) {
            return curPageIter.next();
        }
        //update iterator
        boolean status = hasNext();
        if (status) {
          return curPageIter.next();
        }
        throw new NoSuchElementException();
    }

    /**
     * Resets the iterator to the start.
     * @throws DbException When rewind is unsupported.
     */
    public void rewind() throws DbException, TransactionAbortedException {
      close();
      open();
    }

    /**
     * Closes the iterator.
     */
    public void close() {
      pgNo = 0;
      open = false;
    }

    private void generateNextPage() throws DbException, TransactionAbortedException{
        pid = new HeapPageId(tableId, pgNo++);
        //fetch page - should not be null if getPage() works.
        curPage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY);
        //generate iterator
        if (curPage == null)
          throw new DbException("invalid page fetched from bufferpool");
        curPageIter = curPage.iterator(); 
    }
}
