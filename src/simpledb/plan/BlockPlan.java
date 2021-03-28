package simpledb.plan;

import simpledb.metadata.MetadataMgr;
import simpledb.metadata.StatInfo;
import simpledb.query.BlockScan;
import simpledb.query.Scan;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.record.TableScan;
import simpledb.tx.Transaction;

public class BlockPlan implements Plan {
    private String tblname;
    private Transaction tx;
    private Layout layout;
    private StatInfo si;

    public BlockPlan(Transaction tx, String tblname, MetadataMgr md) {
        this.tblname = tblname;
        this.tx = tx;
        layout = md.getLayout(tblname, tx);
        si = md.getStatInfo(tblname, layout, tx);
    }

    @Override
    public Scan open() {
        return new BlockScan(tx, tblname, layout);
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
        return layout.schema();
    }
}
