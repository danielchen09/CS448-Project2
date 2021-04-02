package simpledb.plan;

import java.util.*;
import java.util.stream.Collectors;

import simpledb.materialize.SortPlan;
import simpledb.query.Predicate;
import simpledb.record.Layout;
import simpledb.record.Schema;
import simpledb.tx.Transaction;
import simpledb.metadata.*;
import simpledb.parse.*;

import javax.management.Query;

/**
 * The simplest, most naive query planner possible.
 * @author Edward Sciore
 */
public class BasicQueryPlanner implements QueryPlannerTest {
    private MetadataMgr mdm;

    public BasicQueryPlanner(MetadataMgr mdm) {
        this.mdm = mdm;
    }

    /**
     * Creates a query plan as follows.  It first takes
     * the product of all tables and views; it then selects on the predicate;
     * and finally it projects on the field list.
     */
    public Plan createPlan(QueryData data, Transaction tx) {
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

    public Plan createPlan(QueryData data, Transaction tx, JoinPlan plan) {
        switch (plan) {
            case CROSS_JOIN: {
                System.out.println("running cross join");
                return createPlan(data, tx);
            }
            case BLOCK_NESTED_LOOP_JOIN: {
                System.out.println("running block nested loop join");
                return createPlanBNLJ(data, tx);
            }
            case MERGE_JOIN: {
                System.out.println("running merge join");
                return createPlanMJ(data, tx);
            }
            case HASH_JOIN: {
                System.out.println("running hash join");
                return createPlanHJ(data, tx);

            }
            default: {
                System.out.println("running cross join");
                return createPlan(data, tx);
            }
        }
    }

    public Plan createPlanHJ(QueryData data, Transaction tx) {
        List<Plan> plans = new ArrayList<>();
        for (String tblname : data.tables()) {
            plans.add(new TablePlan(tx, tblname, mdm));
        }

        Plan p = plans.remove(0);
        for (Plan nextplan : plans) {
            p = new HashPlan(tx, p, nextplan, data.pred());
        }

        p = new ProjectPlan(p, data.fields());
        return p;
    }

    public Plan createPlanMJ(QueryData data, Transaction tx) {
        List<Plan> plans = new ArrayList<>();
        for (String tblname : data.tables()) {
            plans.add(new TablePlan(tx, tblname, mdm));
        }

        Plan p = plans.remove(0);
        for (Plan nextplan : plans) {
            p = new MergeJoinPlan(tx, p, nextplan, data.pred());
        }

        p = new ProjectPlan(p, data.fields());
        return p;
    }

    public Plan createPlanBNLJ(QueryData data, Transaction tx) {
        List<Plan> plans = new ArrayList<>();
        for (String tblname : data.tables()) {
            plans.add(new TablePlan(tx, tblname, mdm));
        }

        Plan p = plans.remove(0);
        Iterator<String> it = data.tables().iterator();
        Layout layout = mdm.getLayout(it.next(), tx);
        for (int i = 0; i < data.tables().size() - 1; i++) {
            layout = combineLayout(layout, mdm.getLayout(it.next(), tx));
            p = new BNLJPlan(tx, layout, p, plans.get(i), data.pred());
        }

        return new ProjectPlan(p, data.fields());
    }

    public Layout combineLayout(Layout l1, Layout l2) {
        Schema schema = new Schema();
        schema.addAll(l1.schema());
        schema.addAll(l2.schema());

        return new Layout(schema);
    }
}
