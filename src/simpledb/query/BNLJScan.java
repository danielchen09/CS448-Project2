package simpledb.query;

import simpledb.server.SimpleDB;

public class BNLJScan implements Scan {
    private int bb = SimpleDB.BUFFER_SIZE - 2;
    private BlockScan r, s;
    private Predicate pred;

    public BNLJScan(BlockScan r, BlockScan s, Predicate pred) {
        this.r = r;
        r.loadNext(bb);
        this.s = s;
        s.loadNext(1);
        beforeFirst();
        this.pred = pred;
    }


    @Override
    public void beforeFirst() {
        r.beforeFirst();
        r.next();
        s.beforeFirst();
    }

    @Override
    public boolean next() {
        while (true) {
            // innermost loop
            while (s.next()) {
                if (pred.isSatisfied(this)) {
                    return true;
                }
            }
            // Bs exhausted, load next in Br
            if (!r.next()) {
                // Br exhausted, load next in s
                if (s.loadNext(1) == 0) {
                    // s exhausted, load next in r
                    if (r.loadNext(bb) == 0) {
                        // r exhausted, done
                        return false;
                    }
                    // next r is loaded, reset s
                    s.close();
                    s.loadNext(1);
                }
                // next s is loaded, reset Br
                r.beforeFirst();
                r.next();
            }
            // next Br is loaded, reset Bs
            s.beforeFirst();
        }
    }

    @Override
    public int getInt(String fldname) {
        if (r.hasField(fldname))
            return r.getInt(fldname);
        else
            return s.getInt(fldname);
    }

    @Override
    public String getString(String fldname) {
        if (r.hasField(fldname))
            return r.getString(fldname);
        else
            return s.getString(fldname);
    }

    @Override
    public Constant getVal(String fldname) {
        if (r.hasField(fldname))
            return r.getVal(fldname);
        else
            return s.getVal(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return r.hasField(fldname) || s.hasField(fldname);
    }

    @Override
    public void close() {
        r.close();
        s.close();
    }
}
