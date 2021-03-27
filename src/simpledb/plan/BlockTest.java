package simpledb.plan;

import simpledb.query.BlockScan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

import java.io.File;

public class BlockTest {
    public static void deleteDir(File f) {
        if (f.isDirectory()) {
            for (File ff : f.listFiles()) {
                deleteDir(ff);
            }
        }
        f.delete();
    }

    public static void main(String[] args) {
        String fname = "blocktest";
        File ff = new File(fname);
        deleteDir(ff);

        SimpleDB db = new SimpleDB(fname);
        Transaction tx = db.newTx();
        Planner planner = db.planner();

        String cmd = "create table T1(A int, B varchar(9))";
        planner.executeUpdate(cmd, tx);
        int n = 20;
        System.out.println("Inserting " + n + " records into T1.");
        for (int i=0; i<n; i++) {
            int a = i;
            String b = "bbb"+a;
            cmd = "insert into T1(A,B) values(" + a + ", '"+ b + "')";
            planner.executeUpdate(cmd, tx);
            System.out.println(cmd);
        }

        String qry = "select A,B from T1";
        Plan p = planner.createQueryPlan(qry, tx);
        BlockScan s = (BlockScan) p.open();
        s.loadNext(1);
        while (s.next())
            System.out.println(s.getInt("a") + s.getString("b"));
        s.close();
        tx.commit();
    }
}
