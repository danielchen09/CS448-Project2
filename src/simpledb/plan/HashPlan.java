package simpledb.plan;

import simpledb.query.HashScan;
import simpledb.query.Predicate;
import simpledb.query.Scan;
import simpledb.query.Term;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;

public class HashPlan implements Plan {
    public static int SIZE = 4;
    private Plan p1, p2;
    private Schema schema = new Schema();
    private Predicate pred;
    private String fieldr;
    private String fields;

    private Transaction tx;

    public static int count = 0;

    public HashPlan(Transaction tx, Plan p1, Plan p2, Predicate pred) {
        this.tx = tx;

        schema.addAll(p1.schema());
        schema.addAll(p2.schema());
        this.pred = pred.selectSubPred(schema);


        if (p1.blocksAccessed() <= p2.blocksAccessed()) {
            this.p1 = p1;
            this.p2 = p2;
        } else {
            this.p1 = p2;
            this.p2 = p1;
        }

        Term matchTerm = findMatchTerm(this.p1, this.p2);
        this.fieldr = matchTerm.getExpression(this.p1.schema()).asFieldName();
        this.fields = matchTerm.getExpression(this.p2.schema()).asFieldName();
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
        return new HashScan(tx, new Layout(schema), "hj-" + (count++), p1.open(), p2.open(), fieldr, fields);
    }

    @Override
    public int blocksAccessed() {
        return 3 * (p1.blocksAccessed() + p2.blocksAccessed())+ 4 * SIZE ;
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
