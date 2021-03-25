package simpledb.plan;

import simpledb.query.BlockScan;
import simpledb.query.Scan;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;

public class BlockTest {
    public static void main(String[] args) {
        SimpleDB db = new SimpleDB("plannertest3");
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
        Scan s = p.open();
        while (s.next())
            System.out.println(s.getString("a") + s.getString("b"));
        s.close();
        tx.commit();
    }
}
