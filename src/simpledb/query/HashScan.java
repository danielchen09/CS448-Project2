package simpledb.query;

import simpledb.file.BlockId;
import simpledb.plan.HashPlan;
import simpledb.record.Layout;
import simpledb.record.RecordPage;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;
import static java.sql.Types.INTEGER;

import java.awt.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

public class HashScan implements Scan {
    class HashBucket extends TableScan {
        private Layout layout;
        public HashBucket(Transaction tx, String fname, Layout layout) {
            super(tx, fname, layout);
            this.layout = layout;
        }

        public void add(Scan s) {
            insert();
            for (String fldname : this.layout.schema().fields()) {
                setVal(fldname, s.getVal(fldname));
            }
        }
    }

    private String fieldr, fields;
    private HashBucket[] hr;
    private HashBucket[] hs;
    private HashMap<Constant, List<HashMap<String, Constant>>> inMemHash;
    private HashMap<String, Constant> currentTuple;
    private int i = 0;

    private Layout layout;

    public HashScan(Transaction tx, Layout layout, String fname, Scan r, Scan s, String fieldr, String fields) {
        this.fieldr = fieldr;
        this.fields = fields;
        this.layout = layout;

        hr = new HashBucket[HashPlan.SIZE];
        hs = new HashBucket[HashPlan.SIZE];
        for (int j = 0; j < HashPlan.SIZE; j++) {
            hr[j] = new HashBucket(tx, fname + "-r-" + j, projectLayout(r));
            hs[j] = new HashBucket(tx, fname + "-s-" + j, projectLayout(s));
        }
        while (r.next()) {
            hr[hash(r.getVal(fieldr).hashCode())].add(r);
        }
        while (s.next()) {
            hs[hash(s.getVal(fields).hashCode())].add(s);
        }
        hashInMem(hs[0]);
        beforeFirst();
    }

    public Layout projectLayout(Scan s) {
        Schema schema = new Schema();
        for (String fldname : layout.schema().fields()) {
            if (s.hasField(fldname))
                schema.addField(fldname, layout.schema().type(fldname), layout.schema().length(fldname));
        }
        return new Layout(schema);
    }

    public void hashInMem(Scan s) {
        s.beforeFirst();
        inMemHash = new HashMap<>();
        Layout sl = projectLayout(s);
        while (s.next()) {
            Constant key = s.getVal(fields);
            if (!inMemHash.containsKey(key))
                inMemHash.put(key, new ArrayList<>());
            HashMap<String, Constant> tuple = new HashMap<>();
            for (String fldname : sl.schema().fields()) {
                tuple.put(fldname, s.getVal(fldname));
            }
            inMemHash.get(key).add(tuple);
        }
    }

    public int hash(int hashcode) {
        return hashcode & (int) Math.log(HashPlan.SIZE);
    }

    @Override
    public void beforeFirst() {
        i = 0;
        hs[0].beforeFirst();
        hr[0].beforeFirst();
    }

    @Override
    public boolean next() {
        while (true) {
            while (hr[i].next()) {
                Constant key = hr[i].getVal(fieldr);
                if (inMemHash.containsKey(key)) {
                    if (inMemHash.get(key).size() > 0) {
                        currentTuple = inMemHash.get(key).remove(0);
                        return true;
                    }
                }
            }
            if (++i >= HashPlan.SIZE)
                return false;
            hashInMem(hs[i]);
            hs[i].beforeFirst();
            hr[i].beforeFirst();
        }
    }

    @Override
    public int getInt(String fldname) {
        if (hr[i].hasField(fldname))
            return hr[i].getInt(fldname);
        else
            return currentTuple.get(fldname).asInt();
    }

    @Override
    public String getString(String fldname) {
        if (hr[i].hasField(fldname))
            return hr[i].getString(fldname);
        else
            return currentTuple.get(fldname).asString();
    }

    @Override
    public Constant getVal(String fldname) {
        if (hr[i].hasField(fldname))
            return hr[i].getVal(fldname);
        else
            return currentTuple.get(fldname);
    }

    @Override
    public boolean hasField(String fldname) {
        return hr[i].hasField(fldname) || hs[i].hasField(fldname);
    }

    @Override
    public void close() {
        for (int j = 0; j < HashPlan.SIZE; j++) {
            hr[j].close();
            hs[j].close();
        }
    }
}
