package simpledb.plan;

import simpledb.query.Scan;
import simpledb.record.Schema;

public class MJPlan implements Plan {
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
