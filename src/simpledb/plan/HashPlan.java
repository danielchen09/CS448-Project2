package simpledb.plan;

import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.Term;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashPlan implements Plan {
    private Plan p1, p2;
    private Schema schema = new Schema();
    private Predicate pred;
    private String fieldr;
    private String fields;

    public HashPlan(Transaction tx, Plan p1, Plan p2, Predicate pred) {
        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
        this.pred = pred.selectSubPred(schema);

        Term matchTerm = findMatchTerm(p1, p2);
        this.fieldr = matchTerm.getExpression(p1.schema()).asFieldName();
        this.fields = matchTerm.getExpression(p2.schema()).asFieldName();

        this.p1 = p1;
        this.p2 = p2;
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
        return null;
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
        return null;
    }
}
