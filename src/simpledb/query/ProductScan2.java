package simpledb.query;

/**
 * Write your own version of join algorithm here
 */
public class ProductScan2 implements Scan {
   private Scan s1, s2;


   public ProductScan2(Scan s1, Scan s2) {
	  this.s1 = s1;
	  this.s2 = s2;
      System.out.println("Write your implementation");
   }


   public void beforeFirst() {
	  System.out.println("Write your implementation");

   }


   public boolean next() {
      return true;
   }


   public int getInt(String fldname) {
      return 0;
   }

   public String getString(String fldname) {
      return "Write your own implementation";
   }

   public Constant getVal(String fldname) {
	  if (s1.hasField(fldname))
	     return s1.getVal(fldname);
	  else
	     return s2.getVal(fldname);   
   }


   public boolean hasField(String fldname) {
      return s1.hasField(fldname) || s2.hasField(fldname);
   }

   public void close() {
      s1.close();
      s2.close();
   }
}
