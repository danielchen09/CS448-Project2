package simpledb.plan;

import java.util.*;
import simpledb.tx.Transaction;
import simpledb.metadata.*;
import simpledb.parse.*;

/**
 * The simplest, most naive query planner possible.
 * @author Edward Sciore
 */
public class BasicQueryPlanner implements QueryPlanner {
    private MetadataMgr mdm;

    public BasicQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    /**
     * Creates a query plan as follows.  It first takes
     * the product of all tables and views; it then selects on the predicate;
     * and finally it projects on the field list.
     */
    public Plan _createPlan(QueryData data, Transaction tx) {
        //Step 1: Create a plan for each mentioned table or view.
        List<Plan> plans = new ArrayList<>();
        for (String tblname : data.tables()) {
            String viewdef = mdm.getViewDef(tblname, tx);
            if (viewdef != null) { // Recursively plan the view.
                Parser parser = new Parser(viewdef);
                QueryData viewdata = parser.query();
                plans.add(createPlan(viewdata, tx));
            }
            else
                plans.add(new TablePlan(tx, tblname, mdm));
        }

        //Step 2: Create the product of all table plans
        Plan p = plans.remove(0);
        for (Plan nextplan : plans) {
            p = new OptimizedProductPlan(p, nextplan);
            System.out.println(p.blocksAccessed());
        }


        //Step 3: Add a selection plan for the predicate
        p = new SelectPlan(p, data.pred());

        //Step 4: Project on the field names
        p = new ProjectPlan(p, data.fields());
        return p;
    }

    public Plan createPlan(QueryData data, Transaction tx) {
        return new ProjectPlan(new SelectPlan(new TablePlan(tx, data.tables().iterator().next(), mdm), data.pred()), data.fields());
    }
}
