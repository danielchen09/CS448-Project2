package simpledb.plan;

import simpledb.materialize.SortPlan;
import simpledb.query.Constant;
import simpledb.query.MergeJoinScan;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

import java.util.Arrays;

public class MergeJoinPlan implements Plan {
    private Plan p1, p2;
    private Schema schema = new Schema();
    private Predicate pred;
    private String fieldr;
    private String fields;

    public MergeJoinPlan(Transaction tx, Plan p1, Plan p2, Predicate pred) {
        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
        this.pred = pred.selectSubPred(schema);
        this.fieldr = this.pred.getTerms().get(0).getExpression(p1.schema()).asFieldName();
        this.fields = this.pred.getTerms().get(0).getExpression(p2.schema()).asFieldName();

        this.p1 = new SortPlan(tx, p1, Arrays.asList(fieldr));
        this.p2 = new SortPlan(tx, p2, Arrays.asList(fields));
    }

    @Override
    public Scan open() {
        return new MergeJoinScan(p1.open(), p2.open(), this.fieldr, this.fields);
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
