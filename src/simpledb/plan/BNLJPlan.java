package simpledb.plan;

import simpledb.metadata.MetadataMgr;
import simpledb.metadata.StatInfo;
import simpledb.query.*;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class BNLJPlan implements Plan {
    private Plan p1, p2;
    private Schema schema = new Schema();
    private Predicate pred;

    public BNLJPlan(Plan p1, Plan p2, Predicate pred) {
        this.p1 = p1;
        this.p2 = p2;
        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
        this.pred = pred.selectSubPred(schema);
    }

    @Override
    public Scan open() {
        return new BNLJScan((BlockScan) p1.open(), (BlockScan) p2.open(), pred);
    }

    @Override
    public int blocksAccessed() {
        return 0;
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
