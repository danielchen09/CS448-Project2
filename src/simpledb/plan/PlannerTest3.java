package simpledb.plan;

import simpledb.file.BlockId;
import simpledb.record.Layout;
import simpledb.record.RecordPage;
import simpledb.record.Schema;
import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.query.Scan;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.function.Consumer;

public class PlannerTest3 {
   // constants
   private static final String TEST_DIR = "jointests/";
   private static final String DNAME_BASE = "plannertest3";

   // globals
   private static Planner planner;
   private static Transaction tx;
   public static SimpleDB db;

   // useful variable for size
   private static final int N = 20;

   // timer
   private static long timer;

   // for creating tables automatically
   private static int tableCount;
   private static int colCount;


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

   public static List<Integer> generateRange(int start, int end, int step) {
      List<Integer> result = new ArrayList<>();
      if (step > 0) {
         for (int i = start; i < end; i += step) {
            result.add(i);
         }
      } else {
         for (int i = start; i >= end; i += step) {
            result.add(i);
         }
      }
      return result;
   }

   public static void insert(List<Integer> data) {
      String tbl = "T" + (++tableCount);
      char col1 = (char)('A' + (colCount++));
      char col2 = (char)('A' + (colCount++));

      String cmd = String.format("create table %s(%c int, %c varchar(9))", tbl, col1, col2);
      planner.executeUpdate(cmd, tx);
      for (int i : data) {
         int a = i;
         String b = "" + col2 + a;
         cmd = String.format("insert into %s(%c, %c) values(%s, '%s')", tbl, col1, col2, a, b);
//            System.out.println(cmd);
         planner.executeUpdate(cmd, tx);
      }
   }

   public static long test1(String dname, QueryPlannerTest.JoinPlan plan) throws Exception {
      String qry = "select B,D,F from T1,T2,T3 where A=C and C=E";
      return runTest(dname, plan, () -> {
         insert(generateRange(0, 100, 1));
         insert(generateRange(0, 100, 1));
         insert(generateRange(0, 100, 1));
         return null;
      }, qry);
   }

   public static long runTest(String dname, QueryPlannerTest.JoinPlan plan, Callable<Void> datagen, String qry) throws Exception {
      tableCount = 0;
      colCount = 0;

      db = new SimpleDB(dname);
      tx = db.newTx();
      planner = db.planner();

      datagen.call();

      timer = System.currentTimeMillis();

      Plan p = planner.createQueryPlan(qry, tx, plan);
      Scan s = p.open();
      while (s.next());
      s.close();

      tx.commit();
      long time = System.currentTimeMillis() - timer;
      return time;
   }

   public static void t1() throws IOException {
      PrintWriter pw = new PrintWriter(new File(TEST_DIR + "test1.txt"));
      for (int i = 0; i < 30; i++) {
         for (QueryPlannerTest.JoinPlan jp : QueryPlannerTest.JoinPlan.values()) {
            String dname = DNAME_BASE + "-" + jp;
            try {
               System.out.printf("\nrunning %s, directory: %s\n", jp, dname);
               long time = test1(dname, jp);
               pw.println(jp + " " + time);
               db.fileMgr().closeAll();
            } catch (Exception ex) {
               ex.printStackTrace();
            } finally {
               File ff = new File(dname);
               deleteDir(ff);
            }
         }
      }
   }

   public static void main(String[] args) throws IOException {
      t1();
   }
}
