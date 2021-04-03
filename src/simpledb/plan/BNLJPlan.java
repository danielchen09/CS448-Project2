package simpledb.plan;

import simpledb.metadata.MetadataMgr;
import simpledb.metadata.StatInfo;
import simpledb.query.*;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class BNLJPlan implements Plan {
    private Plan p1, p2;
    private Schema schema;
    private Predicate pred;
    private Transaction tx;
    private Layout layout;

    public BNLJPlan(Transaction tx, Layout layout, Plan p1, Plan p2, Predicate pred) {
        this.tx = tx;
        this.layout = layout;
        if (p1.blocksAccessed() <= p2.blocksAccessed()) {
            this.p1 = p1;
            this.p2 = p2;
        } else {
            this.p1 = p2;
            this.p2 = p1;
        }
        this.schema = layout.schema();
        this.pred = pred.selectSubPred(schema);
    }

    @Override
    public Scan open() {
        return new BNLJScan(tx, layout, p1.open(), p2.open(), pred);
    }

    @Override
    public int blocksAccessed() {
        return p1.blocksAccessed() * p2.blocksAccessed() + p1.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return 0;
    }

    @Override
    public int distinctValues(String fldname) {
        return 0;
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
