package main;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.ImpermanentDatabaseRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.stream.Stream;

/**
 * Unit test for simple HGC.
 */
public class HGCTest {

    private static Logger log = LoggerFactory.getLogger(HGCTest.class);

    private static final String TEST_GRAPH =
            "CREATE " +
                    "(root:ROOT:ACTIVE:TISSUE {schema:[1.0, 1.0]}), " +
                    "(c1:ACTIVE:CELL {schema:[1.0, 1.0]}), " +
                    "(c2:ACTIVE:CELL {schema:[1.0, 1.0]}), " +
                    "(c3:ACTIVE:CELL {schema:[1.0, 0.0]}), " +
                    "(c4:ACTIVE:CELL {schema:[0.0, 1.0]}), " +
                    "(root)-[:CONTAINS]->(c1), " +
                    "(root)-[:CONTAINS]->(c2), " +
                    "(root)-[:CONTAINS]->(c3), " +
                    "(root)-[:CONTAINS]->(c4)";

    private static final String TISSUE_GRAPH =
            "CREATE " +
                    "(root:ROOT:ACTIVE:TISSUE {schema:[1.0, 1.0]}), " +
                    "(c1:ACTIVE:CELL {schema:[1.0, 1.0]}), " +
                    "(c2:ACTIVE:CELL {schema:[1.0, 1.0]}), " +
                    "(g1:PASSIVE:GENE {schema:[1.0, 1.0]}), " +
                    "(g2:PASSIVE:GENE {schema:[1.0, 1.0]}), " +
                    "(g3:PASSIVE:GENE {schema:[1.0, 1.0]}), " +
                    "(g4:PASSIVE:GENE {schema:[1.0, 1.0]}), " +
                    "(root)-[:CONTAINS]->(c1), " +
                    "(root)-[:CONTAINS]->(c2), " +
                    "(c1)-[:CONTAINS]->(g1), " +
                    "(c1)-[:CONTAINS]->(g2), " +
                    "(c1)-[:CONTAINS]->(g3), " +
                    "(c1)-[:CONTAINS]->(g4)";

    @ClassRule
    public static final ImpermanentDatabaseRule DB = new ImpermanentDatabaseRule();

    private HGC hgc;

    @Before
    public void setup() throws Exception {
        DB.execute("MATCH (n) DETACH DELETE n");
        hgc = new HGC.Builder(DB)
                .withGraph(TISSUE_GRAPH)
                .withGraph(DB -> {})
                .withNodeFunctions(TestFunctions.class)
                .build();
    }

    @Test
    public void computeTest() throws Exception {
        hgc
                .setPreIterationMonitor(i -> log.info("Iteration {}", i))
                .computeAll();
    }

    @Test
    public void tissueTest() throws Exception {
        hgc
                .withRootFilter(new double[]{1.0, 1.0})
                .setPreIterationMonitor(i -> {
                    DB.execute("match (t:TISSUE) return count(t)")
                            .columnAs("count(t)")
                            .stream()
                            .map(o -> i + ": " + o + " tissues")
                            .forEach(log::info);
                    DB.execute("match (c:CELL) return count(c)")
                            .columnAs("count(c)")
                            .stream()
                            .map(o -> i + ": " + o + " cells")
                            .forEach(log::info);
                    DB.execute("match (g:GENE) return count(g)")
                            .columnAs("count(g)")
                            .stream()
                            .map(o -> i + ": " + o + " genes")
                            .forEach(log::info);
                })
                .computeAll(10);
    }

    @Test
    public void cloneTest() throws Exception {
        try (Transaction tx = DB.beginTx()) {
            hgc.findSNodes(HGC.labels.ROOT)
                    .forEach(sNode ->
                            log.info("{}", sNode.deepPrettyPrint()));

            hgc.findSNodes(TestFunctions.labels.CELL)
                    .forEach(SNode::deepClone);

            hgc.findSNodes(HGC.labels.ROOT)
                    .forEach(sNode -> log.info("After: {}", sNode.deepPrettyPrint()));

//            hgc.getAllSNodes()
//                    .forEach(sNode -> log.info("ALL: {}", sNode.deepPrettyPrint()));
            tx.success();
        }
    }

    @Test
    public void reductionTest() throws Exception {
//        Stream.of(1, 2, 3)
//                .reduce(new Integer(0),,)
    }
}
