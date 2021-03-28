package simpledb.plan;

import simpledb.server.SimpleDB;
import simpledb.tx.Transaction;
import simpledb.query.Scan;

import java.io.File;

public class PlannerTest3 {
   public static void deleteDir(File f) {
      if (f.isDirectory()) {
         for (File ff : f.listFiles()) {
            deleteDir(ff);
         }
      }
      f.delete();
   }

   public static void main(String[] args) {
      String fname = "plannertest3";
      File ff = new File(fname);
      deleteDir(ff);

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

      cmd = "create table T2(C int, D varchar(9))";
      planner.executeUpdate(cmd, tx);
      System.out.println("Inserting " + n + " records into T2.");
      for (int i=0; i<n; i++) {
         int c = n-i-1;
         String d = "ddd" + c;
         cmd = "insert into T2(C,D) values(" + c + ", '"+ d + "')";
         planner.executeUpdate(cmd, tx);
         System.out.println(cmd);
      }
//
//      cmd = "create table T3(E int, F varchar(9))";
//      planner.executeUpdate(cmd, tx);
//      System.out.println("Inserting " + n + " records into T3.");
//      for (int i=0; i<n; i++) {
//         int e = i;
//         String f = "fff"+e;
//         cmd = "insert into T3(E,F) values(" + e + ", '"+ f + "')";
//         planner.executeUpdate(cmd, tx);
//         System.out.println(cmd);
//      }

      String qry = "select B,D from T1,T2 where A=C";
      Plan p = planner.createQueryPlan(qry, tx);
      Scan s = p.open();
      while (s.next())
         System.out.println(s.getString("b") + " " + s.getString("d")); 
      s.close();
      
//
//      qry = "select D,F from T2,T3 where C=E";
//      p = planner.createQueryPlan(qry, tx);
//      s = p.open();
//      while (s.next())
//    	  System.out.println(s.getString("d") + " " + s.getString("f"));
//      s.close();

      tx.commit();
      
      
   }
}
