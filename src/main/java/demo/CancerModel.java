package demo;

import main.Functions;
import main.HGC;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Supplier;

public class CancerModel {

    private static Logger log = LoggerFactory.getLogger(CancerModel.class);

    public enum labels implements Label {TISSUE, CELL, CHROMOSOME, GENE, DIV, APOPT, SEG}

    private HGC hgc;

    public static void main(String... args) throws IllegalAccessException, InstantiationException {
        new CancerModel().go();
    }

    public void go() throws InstantiationException, IllegalAccessException {
        File dbFile = new File("temp.db");

        GraphDatabaseService DB = new GraphDatabaseFactory().newEmbeddedDatabase(dbFile);
        DB.execute("match (n) detach delete n");

        hgc = new HGC.Builder(DB)
                .withGraph(graphBuilder)
                .withNodeFunctions(CellFunctions.class)
                .withRootLabel(labels.TISSUE)
//                .maxIterations(100)
                .build();

        hgc
//                .setPreIterationMonitor(i ->
//                        log.info("iteration {}:\n{}",
//                                i, DB.execute("match (n) return distinct count(labels(n)), labels(n)").resultAsString()))
                .setPreIterationMonitor(i -> log.info("{}", i))
                .setPostExecutionMonitor(i ->
                        log.info("\n{}", DB.execute("match (n) return distinct count(labels(n)), labels(n)").resultAsString()))
                .computeAll(100);

        DB.shutdown();
    }


    public static class CellFunctions extends Functions {

        public HGC.NodeFunction divide = sNode -> {

            if (sNode.hasLabel(labels.CELL)) {
                sNode.deepClone();
            }

            if (sNode.hasLabel(labels.CHROMOSOME)) return new double[]{1.1, 1.0};

            return sNode.getDefaultSchema();
        };

        public HGC.NodeFunction die = sNode -> {
            if (sNode.hasLabel(labels.CELL)) {
                sNode.setInactive();
            }
            if (sNode.hasLabel(labels.CHROMOSOME)) return new double[]{1.1, 1.0};
            return sNode.getDefaultSchema();
        };
    }

    public HGC.GraphBuilder graphBuilder = DB -> {

        // VARIABLES
        int nCells = 100;
        long rndSeed = 123;

        ThreadLocalRandom rand = ThreadLocalRandom.current();
//        rand.setSeed(rndSeed);

        // SUPPLIERS
        Supplier<Node> divGene = () -> {
            Node node = DB.createNode(labels.DIV, labels.GENE, HGC.labels.ACTIVE);
            node.setProperty(HGC.SCHEMA_PROPERTY, new double[]{1.0, 1.0});
            return node;
        };
        Supplier<Node> apoptGene = () -> {
            Node node = DB.createNode(labels.APOPT, labels.GENE, HGC.labels.ACTIVE);
            node.setProperty(HGC.SCHEMA_PROPERTY, new double[]{1.0, 1.0});
            return node;
        };
        Supplier<Node> segGene = () -> {
            Node node = DB.createNode(labels.SEG, labels.GENE, HGC.labels.ACTIVE);
            node.setProperty(HGC.SCHEMA_PROPERTY, new double[]{1.0, 1.0});
            return node;
        };
        Supplier<Node> chromosome = () -> {
            Node node = DB.createNode(labels.CHROMOSOME, HGC.labels.ACTIVE);
            node.setProperty(HGC.SCHEMA_PROPERTY, new double[]{1.0, 1.0});
            return node;
        };

        // TISSUE
        Node tissue = DB.createNode(labels.TISSUE, HGC.labels.ACTIVE);
        tissue.setProperty(HGC.SCHEMA_PROPERTY, new double[]{1.0, 1.0});

        // CELLS
        Node[] cells = new Node[nCells];
        for (int i = 0; i < cells.length; i++) {
            cells[i] = DB.createNode(labels.CELL, HGC.labels.ACTIVE);
            cells[i].setProperty(HGC.SCHEMA_PROPERTY, new double[]{1.0, 1.0});
            tissue.createRelationshipTo(cells[i], HGC.rels.CONTAINS);

            int dist = rand.nextInt(3);

            // CHROMOSOME PAIRS
            Node[] chrs = new Node[4];
            for (int j = 0; j < chrs.length; j++) {
                chrs[j] = chromosome.get();
                cells[i].createRelationshipTo(chrs[j], HGC.rels.CONTAINS);
            }

            switch (dist) {
                case 0:
                    // distribution A
                    chrs[0].createRelationshipTo(divGene.get(), HGC.rels.CONTAINS);
                    chrs[0].createRelationshipTo(apoptGene.get(), HGC.rels.CONTAINS);
                    chrs[1].createRelationshipTo(divGene.get(), HGC.rels.CONTAINS);
                    chrs[1].createRelationshipTo(apoptGene.get(), HGC.rels.CONTAINS);
                    chrs[2].createRelationshipTo(segGene.get(), HGC.rels.CONTAINS);
                    chrs[3].createRelationshipTo(segGene.get(), HGC.rels.CONTAINS);
                    break;
                case 1:
                    // distribution B
                    chrs[0].createRelationshipTo(apoptGene.get(), HGC.rels.CONTAINS);
                    chrs[1].createRelationshipTo(apoptGene.get(), HGC.rels.CONTAINS);
                    chrs[2].createRelationshipTo(divGene.get(), HGC.rels.CONTAINS);
                    chrs[2].createRelationshipTo(segGene.get(), HGC.rels.CONTAINS);
                    chrs[3].createRelationshipTo(divGene.get(), HGC.rels.CONTAINS);
                    chrs[3].createRelationshipTo(segGene.get(), HGC.rels.CONTAINS);
                    break;
                case 2:
                    // distribution C
                    chrs[0].createRelationshipTo(divGene.get(), HGC.rels.CONTAINS);
                    chrs[1].createRelationshipTo(divGene.get(), HGC.rels.CONTAINS);
                    chrs[2].createRelationshipTo(apoptGene.get(), HGC.rels.CONTAINS);
                    chrs[2].createRelationshipTo(segGene.get(), HGC.rels.CONTAINS);
                    chrs[3].createRelationshipTo(apoptGene.get(), HGC.rels.CONTAINS);
                    chrs[3].createRelationshipTo(segGene.get(), HGC.rels.CONTAINS);
                    break;
            }
        }
    };
}
