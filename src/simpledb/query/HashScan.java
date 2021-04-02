package simpledb.query;

import simpledb.record.RecordPage;

import java.util.Hashtable;
import java.util.List;

public class HashScan implements Scan {
    private Scan r, s;
    private List<RecordPage> buckets;
    private String fieldr, fields;

    public HashScan(Scan r, Scan s, String fieldr, String fields) {
        this.r = r;
        this.s = s;
        this.fieldr = fieldr;
        this.fields = fields;
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
