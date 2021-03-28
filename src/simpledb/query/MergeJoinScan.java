package simpledb.query;

public class MergeJoinScan implements Scan {
    private Scan r, s;
    private String fieldr;
    private String fields;

    public MergeJoinScan(Scan r, Scan s, String fieldr, String fields) {
        this.r = r;
        this.s = s;
        this.fieldr = fieldr;
        this.fields = fields;
        beforeFirst();
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
            if (!s.next())
                return false;
            while (r.getVal(fieldr).compareTo(s.getVal(fields)) < 0) {
                if (!r.next())
                    return false;
            }
            if (r.getVal(fieldr).compareTo(s.getVal(fields)) == 0) {
                return true;
            }
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
        return false;
    }

    @Override
    public void close() {
        r.close();
        s.close();
    }
}
