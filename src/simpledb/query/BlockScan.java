package simpledb.query;

import simpledb.file.BlockId;
import simpledb.record.Layout;
import simpledb.record.RecordPage;
import simpledb.tx.Transaction;

import java.util.ArrayList;
import java.util.List;

import static java.sql.Types.INTEGER;

public class BlockScan implements Scan {
    public List<RecordPage> pages;
    private Transaction tx;
    private Layout layout;
    private String filename;
    private int currentslot;
    private int currentblock;
    private int last;

    public BlockScan(Transaction tx, String tblname, Layout layout) {
        this.tx = tx;
        this.layout = layout;
        filename = tblname + ".tbl";
    }

    // return how many is loaded
    public int loadNext(int n) {
        close();
        pages = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            if (last + i >= tx.size(filename)) {
                last += i;
                return i;
            }
            BlockId blk = new BlockId(filename, last + i);
            RecordPage rp = new RecordPage(tx, blk, layout);
            pages.add(rp);
        }
        last += n;
        currentblock = 0;
        return n;
    }

    @Override
    public void beforeFirst() {
        currentslot = -1;
        currentblock = 0;
    }

    @Override
    public boolean next() {
        currentslot = pages.get(currentblock).nextAfter(currentslot);
        while (currentslot < 0) {
            if (currentblock == pages.size() - 1)
                return false;
            currentblock++;
            currentslot = pages.get(currentblock).nextAfter(currentslot);
        }
        return true;
    }

    @Override
    public int getInt(String fldname) {
        return pages.get(currentblock).getInt(currentslot, fldname);
    }

    @Override
    public String getString(String fldname) {
        return pages.get(currentblock).getString(currentslot, fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        if (layout.schema().type(fldname) == INTEGER)
            return new Constant(getInt(fldname));
        else
            return new Constant(getString(fldname));
    }

    @Override
    public boolean hasField(String fldname) {
        return layout.schema().hasField(fldname);
    }

    @Override
    public void close() {
        if (pages == null)
            return;
        for (RecordPage rp : pages) {
            tx.unpin(rp.block());
        }
        pages.clear();
        currentblock = 0;
        currentslot = -1;
    }
}
