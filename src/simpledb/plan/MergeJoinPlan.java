package simpledb.plan;

import simpledb.materialize.SortPlan;
import simpledb.query.*;
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

        Term matchTerm = findMatchTerm(p1, p2);
        this.fieldr = matchTerm.getExpression(p1.schema()).asFieldName();
        this.fields = matchTerm.getExpression(p2.schema()).asFieldName();

        this.p1 = new SortPlan(tx, p1, Arrays.asList(fieldr));
        this.p2 = new SortPlan(tx, p2, Arrays.asList(fields));
    }

    private Term findMatchTerm(Plan p1, Plan p2) {
        for (Term t : this.pred.getTerms()) {
            if (t.getExpression(p1.schema()) != null && t.getExpression(p2.schema()) != null) {
                return t;
            }
        }
        return null;
    }

    @Override
    public Scan open() {
        return new MergeJoinScan(p1.open(), p2.open(), this.fieldr, this.fields);
    }

    @Override
    public int blocksAccessed() {
        return p1.blocksAccessed() + p2.blocksAccessed();
    }

    @Override
    public int recordsOutput() {
        return p1.recordsOutput() * p2.recordsOutput() / Math.max(p1.distinctValues(this.fieldr), p2.distinctValues(this.fields));
    }

    @Override
    public int distinctValues(String fldname) {
        return fldname.equals(this.fieldr) ? p1.distinctValues(fldname) : p2.distinctValues(fldname);
    }

    @Override
    public Schema schema() {
        return schema;
    }
}
