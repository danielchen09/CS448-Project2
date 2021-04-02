package simpledb.query;

import simpledb.server.SimpleDB;

import java.util.Enumeration;
import java.util.Hashtable;

public class HashScan implements Scan {
   private BlockScan s,r;
   private Predicate pred;
   private Hashtable h = new Hashtable();
   private String JoinAttrs;

   public HashScan(BlockScan s, BlockScan r, Predicate pred){
      // s: build input, r: probe input
      this.s = s;
      this.r = r;
      beforeFirst();
      this.pred = pred;
      //Compute JoinAttrs in Predicate.java
      JoinAttrs = pred.findJoinAttribute(pred, s, r);
      //use values in join attribute column in the table as the key, record as the value
      //BUILD HASH
      while(s.next()){
         h.put(s.getVal(JoinAttrs), s);
         s.next();
      }
   }

   public void beforeFirst() {
      r.beforeFirst();
      r.next();
      s.beforeFirst();
   }

   public boolean next() {
      //attribute matching
      Enumeration<Integer> keys = h.keys();
      //probe r
      while(r.next()){
         //iterate hash table keys
         while(keys.hasMoreElements()){
            if(h.get(keys.nextElement()) == r.getVal(JoinAttrs)){ //match
               r.next();
               return true;
            }
            return false;
         }
      }
      return false;
   }


   public int getInt(String fldname) {
      if (r.hasField(fldname))
         return r.getInt(fldname);
      else
         return s.getInt(fldname);
   }

   public String getString(String fldname) {
      if (r.hasField(fldname))
         return r.getString(fldname);
      else
         return s.getString(fldname);
   }

   public Constant getVal(String fldname) {
      if (r.hasField(fldname))
         return r.getVal(fldname);
      else
         return s.getVal(fldname);
   }

   public boolean hasField(String fldname) {
      return r.hasField(fldname) || s.hasField(fldname);
   }

   public void close() {
      r.close();
      s.close();
      h.clear();
   }
}
