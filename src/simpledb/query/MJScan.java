package simpledb.query;

public class MJScan implements Scan {
    private Scan r, s;
    private String ar, as;

    public MJScan(Scan r, Scan s, Predicate pred) {
        this.r = r;
        r.beforeFirst();
        this.s = s;
        s.beforeFirst();
    }

    @Override
    public void beforeFirst() {

    }

    @Override
    public boolean next() {
        return false;
    }

    @Override
    public int getInt(String fldname) {
        return 0;
    }

    @Override
    public String getString(String fldname) {
        return null;
    }

    @Override
    public Constant getVal(String fldname) {
        return null;
    }

    @Override
    public boolean hasField(String fldname) {
        return false;
    }

    @Override
    public void close() {

    }
}
