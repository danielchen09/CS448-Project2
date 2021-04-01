package simpledb.plan;

import simpledb.parse.QueryData;
import simpledb.tx.Transaction;

public interface QueryPlannerTest extends QueryPlanner {
    public enum JoinPlan {
        CROSS_JOIN,
        MERGE_JOIN,
        BLOCK_NESTED_LOOP_JOIN,
        HASH_JOIN
    }

    public Plan createPlan(QueryData data, Transaction tx, JoinPlan plan);
}
