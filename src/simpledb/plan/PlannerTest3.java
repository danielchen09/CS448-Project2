package simpledb.plan;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.query.Scan;

import java.io.File;
import java.io.IOException;

public class PlannerTest3 {
   private static final String DNAME_BASE = "plannertest3";
   private static Planner planner;
   private static Transaction tx;

   private static final int N = 20;

   private static long timer;

   private static int tableCount;
   private static int colCount;

   public static SimpleDB db;

   public static void deleteDir(File f) throws IOException {
      if (f.isDirectory()) {
         for (File ff : f.listFiles()) {
            deleteDir(ff);
         }
      }
      if (!f.delete()) {
         throw new IOException("Could not delete " + f.getAbsolutePath());
      }
   }

   public static void insert(int start, int end, int step) {
      String tbl = "T" + (++tableCount);
      char col1 = (char)('A' + (colCount++));
      char col2 = (char)('A' + (colCount++));

      String cmd = String.format("create table %s(%c int, %c varchar(9))", tbl, col1, col2);
      planner.executeUpdate(cmd, tx);
      if (step > 0) {
         for (int i = start; i < end; i += step) {
            int a = i;
            String b = "" + col2 + a;
            cmd = String.format("insert into %s(%c, %c) values(%s, '%s')", tbl, col1, col2, a, b);
//            System.out.println(cmd);
            planner.executeUpdate(cmd, tx);
         }
      } else if (step < 0) {
         for (int i = start; i >= end; i -= step) {
            int a = i;
            String b = "" + col2 + a;
            cmd = String.format("insert into %s(%c, %c) values(%s, %s)", tbl, col1, col2, a, b);
//            System.out.println(cmd);
            planner.executeUpdate(cmd, tx);
         }
      }
   }

   public static long runTest(String dname, QueryPlannerTest.JoinPlan plan) {
      tableCount = 0;
      colCount = 0;

      db = new SimpleDB(dname);
      tx = db.newTx();
      planner = db.planner();

      insert(0, 5, 1);
      insert(0, 5, 1);

      timer = System.currentTimeMillis();

      String qry = "select B,D from T1,T2 where A=C";
      Plan p = planner.createQueryPlan(qry, tx, plan);
      Scan s = p.open();
      while (s.next())
         System.out.println(s.getString("b") + " " + s.getString("d"));
      s.close();

      tx.commit();
      long time = System.currentTimeMillis() - timer;
      return time;
   }

   public static void t1() throws IOException {
      for (QueryPlannerTest.JoinPlan jp : QueryPlannerTest.JoinPlan.values()) {
         String dname = DNAME_BASE + "-" + jp;
         try {
            System.out.printf("running %s, directory: %s\n", jp, dname);
            long time = runTest(dname, jp);
            System.out.println(jp + " " + time);
            db.fileMgr().closeAll();
         } catch (Exception ex) {
            ex.printStackTrace();
         } finally {
            File ff = new File(dname);
            deleteDir(ff);
         }
      }
   }

   public static void main(String[] args) throws IOException {
      t1();
   }
}
