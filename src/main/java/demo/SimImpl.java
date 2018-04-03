package demo;

import ihdn.*;
import org.neo4j.graphdb.Direction;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;

import java.util.concurrent.ThreadLocalRandom;

public class SimImpl implements Simulation {

    private ThreadLocalRandom rand = ThreadLocalRandom.current();

    private int cellCount = 100;
    private int capacity = 200;
    private String cypherQuery;

    public enum labels implements Label {
        TISSUE, CELL, CELL_COPY, GENE, CHROMOSOME, DIV_GENE, APOPT_GENE, SEG_GENE
    }

    public enum relTypes implements RelationshipType {
        FROM, WAS
    }

    private static String cypherGraph = "CREATE\n" +
            "(t:TISSUE:ROOT),\n" +
            "(c1:CELL),\n" +
            "(c2:CELL),\n" +
            "(c3:CELL),\n" +
            "(ch1:CHROMOSOME:CH1),\n" +
            "(ch2:CHROMOSOME:CH2),\n" +
            "(ch3:CHROMOSOME:CH3),\n" +
            "(ch4:CHROMOSOME:CH4),\n" +
            "(ch5:CHROMOSOME:CH5),\n" +
            "(ch6:CHROMOSOME:CH6),\n" +
            "(a1:GENE:APOPT_GENE),\n" +
            "(a2:GENE:APOPT_GENE),\n" +
            "(a3:GENE:APOPT_GENE),\n" +
            "(d1:GENE:DIV_GENE),\n" +
            "(d2:GENE:DIV_GENE),\n" +
            "(d3:GENE:DIV_GENE),\n" +
            "(s1:GENE:SEG_GENE),\n" +
            "(s2:GENE:SEG_GENE),\n" +
            "(s3:GENE:SEG_GENE),\n" +
            "(t)-[:CONTAINS]->(c1),\n" +
            "(t)-[:CONTAINS]->(c2),\n" +
            "(t)-[:CONTAINS]->(c3),\n" +
            "(c1)-[:CONTAINS]->(ch3),\n" +
            "(c1)-[:CONTAINS]->(ch4),\n" +
            "(c2)-[:CONTAINS]->(ch1),\n" +
            "(c2)-[:CONTAINS]->(ch2),\n" +
            "(c3)-[:CONTAINS]->(ch5),\n" +
            "(c3)-[:CONTAINS]->(ch6),\n" +
            "(ch1)-[:CONTAINS]->(d3),\n" +
            "(ch2)-[:CONTAINS]->(a3),\n" +
            "(ch2)-[:CONTAINS]->(s3),\n" +
            "(ch3)-[:CONTAINS]->(d1),\n" +
            "(ch3)-[:CONTAINS]->(a1),\n" +
            "(ch4)-[:CONTAINS]->(s1),\n" +
            "(ch5)-[:CONTAINS]->(s2),\n" +
            "(ch5)-[:CONTAINS]->(d2),\n" +
            "(ch6)-[:CONTAINS]->(a2)";

    private static String cypherGraphA = "CREATE\n" +
            "(t:TISSUE:ROOT),\n" +
            "(c1:CELL),\n" +
            "(ch3:CHROMOSOME:CH3),\n" +
            "(ch4:CHROMOSOME:CH4),\n" +
            "(a1:GENE:APOPT_GENE),\n" +
            "(d1:GENE:DIV_GENE),\n" +
            "(s1:GENE:SEG_GENE),\n" +
            "(t)-[:CONTAINS]->(c1),\n" +
            "(c1)-[:CONTAINS]->(ch3),\n" +
            "(c1)-[:CONTAINS]->(ch4),\n" +
            "(ch3)-[:CONTAINS]->(d1),\n" +
            "(ch3)-[:CONTAINS]->(a1),\n" +
            "(ch4)-[:CONTAINS]->(s1)";

    private static String cypherGraphB = "CREATE\n" +
            "(t:TISSUE:ROOT),\n" +
            "(c2:CELL),\n" +
            "(ch1:CHROMOSOME:CH1),\n" +
            "(ch2:CHROMOSOME:CH2),\n" +
            "(a3:GENE:APOPT_GENE),\n" +
            "(d3:GENE:DIV_GENE),\n" +
            "(s3:GENE:SEG_GENE),\n" +
            "(t)-[:CONTAINS]->(c2),\n" +
            "(c2)-[:CONTAINS]->(ch1),\n" +
            "(c2)-[:CONTAINS]->(ch2),\n" +
            "(ch1)-[:CONTAINS]->(d3),\n" +
            "(ch2)-[:CONTAINS]->(a3),\n" +
            "(ch2)-[:CONTAINS]->(s3)";

    private static String cypherGraphC = "CREATE\n" +
            "(t:TISSUE:ROOT),\n" +
            "(c3:CELL),\n" +
            "(ch5:CHROMOSOME:CH5),\n" +
            "(ch6:CHROMOSOME:CH6),\n" +
            "(a2:GENE:APOPT_GENE),\n" +
            "(d2:GENE:DIV_GENE),\n" +
            "(s2:GENE:SEG_GENE),\n" +
            "(t)-[:CONTAINS]->(c3),\n" +
            "(c3)-[:CONTAINS]->(ch5),\n" +
            "(c3)-[:CONTAINS]->(ch6),\n" +
            "(ch5)-[:CONTAINS]->(s2),\n" +
            "(ch5)-[:CONTAINS]->(d2),\n" +
            "(ch6)-[:CONTAINS]->(a2)";

    public static GraphBuilder graphBuilder = ihdn -> {
        // set cell properties
        ihdn.getIHDNNodes(labels.CELL)
                .forEach(cell -> {
                    cell.setProperty("start", 0);
                    cell.setProperty("nDivs", 0);
                    cell.setProperty(Properties.VOTE_FUNCTION, "cellVote");
                });

        // set filters to [0, 0] for all nodes below level of cell
        ihdn.getIHDNNodes(labels.CHROMOSOME)
                .forEach(hgNode -> hgNode.setProperty(Properties.FILTER, new double[]{0, 0, 0}));
        ihdn.getIHDNNodes(labels.GENE)
                .forEach(hgNode -> hgNode.setProperty(Properties.FILTER, new double[]{0, 0, 0}));

        // duplicate each chromosome
        ihdn.getIHDNNodes(labels.CHROMOSOME).forEach(IHDNNode::deepClone);

        // set votes for each gene type
        ihdn.getIHDNNodes(labels.DIV_GENE).forEach(
                hgNode -> hgNode.setProperty(Properties.VOTE, new double[]{1, 0, 0}));

        ihdn.getIHDNNodes(labels.APOPT_GENE).forEach(
                hgNode -> hgNode.setProperty(Properties.VOTE, new double[]{0, 1, 0}));

        // duplicate the cell 99 times
        for (int i = 0; i < 99; i ++)
            ihdn.getIHDNNodes(labels.CELL).findFirst().ifPresent(IHDNNode::deepClone);
    };

    @IHDNFunctionDefinition
    public IHDNFunction copy = (ihdn, ihdnNode) -> {
        if (ihdnNode.hasLabel(labels.CELL)) {
            int nDivs = ((int) ihdnNode.getProperty("nDivs"));

            ihdnNode.setProperty("nDivs", nDivs + 1);
            IHDNNode cellCopy = ihdnNode.deepClone();
            cellCopy.createRelationshipTo(ihdnNode, relTypes.FROM);
            cellCopy.setProperty("start", ihdn.getCurrentIteration());

            int numSegGenes = ihdnNode.getAllChildNodes()
                    .mapToInt(chromosomeNode ->
                            (int) chromosomeNode.getChildNodesWithLabel(labels.SEG_GENE).count())
                    .sum();

            if (rand.nextInt(50) < Math.max(0, 4 - numSegGenes)) {
                // save copy before missegregation
                IHDNNode newClone = ihdnNode.deepClone();
                newClone.createRelationshipTo(ihdnNode, relTypes.WAS);
                cellCopy.getSingleRelationship(relTypes.FROM, Direction.OUTGOING).delete();
                cellCopy.createRelationshipTo(ihdnNode, relTypes.WAS);
                ihdnNode.addLabel(IHDNLabels.INACTIVE);
                ihdnNode.removeLabel(labels.CELL);
                ihdnNode.addLabel(labels.CELL_COPY);
                ihdnNode.setProperty("missegregationAt", ihdn.getCurrentIteration());
                missegregate(new IHDNNode[]{newClone, cellCopy});
            }

            cellCount++;
        }
    };

    private boolean overCapacity;
    private int lastUpdated;

    @IHDNFunctionDefinition
    public IHDNFunction die = (ihdn, ihdnNode) -> {
        if (ihdnNode.hasLabel(labels.CELL) && overCapacity) {
            ihdnNode.setInactive();
            ihdnNode.setProperty("inactiveAt", ihdn.getCurrentIteration());
            cellCount--;
        }
    };

    @IHDNFunctionDefinition
    public IHDNFunction pass = (ihdn, ihdnNode) -> {
    };

    private void missegregate(IHDNNode[] cells) {
        // select cell to lose copy of gene
        int i = rand.nextInt(2);
        IHDNNode lessCell = cells[i];
        IHDNNode extraCell = cells[(i + 1) % 2];

        IHDNNode[] chromosomes = lessCell
                .getAllChildNodes()
                .toArray(IHDNNode[]::new);

        if (chromosomes.length == 0) return;
        IHDNNode chrToMove = chromosomes[rand.nextInt(chromosomes.length)];

        Relationship parentRel = chrToMove.getSingleRelationship(IHDNRelTypes.CONTAINS, Direction.INCOMING);
        parentRel.delete();

        // put gene in extraCell matching chromosome
        extraCell.createRelationshipTo(chrToMove, IHDNRelTypes.CONTAINS);
    }

    @VoteFunctionDefinition
    public VoteFunction cellVote = (ihdn, ihdnNode, votesFromChildren) -> {

        int currentIteration = ihdn.getCurrentIteration();

        if (currentIteration > lastUpdated) {
            lastUpdated = currentIteration;
            overCapacity = cellCount > capacity;
        }

        double[] votes = VoteFunction.DEFAULT.apply(ihdn, ihdnNode, votesFromChildren);
        double sum = votes[0] + votes[1];
        votes[2] = sum;

        return votes;
    };

    public static void main(String... args) {
        String[] strings = new String[]{cypherGraphA, cypherGraphB, cypherGraphC};
        for (int c = 0; c < strings.length; c++) {
            String cypherQuery = strings[c];
            System.out.printf("Configuration: %d\n", c);
            for (int t = 0; t < 20; t++) {
                System.out.printf("Trial: %d\n", t);
                IHDN IHDN = new IHDN.IHDNBuilder()
                        .withNewDB("graph-c" + c + "-t" + t + ".db")
                        .withCypherStatement(cypherQuery)
                        .withGraphBuilder(graphBuilder)
                        .setIterationMonitor((iteration, HGC) -> {
                            long count = HGC.getActiveIHDNNodes(labels.CELL).filter(cell -> !cell.hasLabel(IHDNLabels.INACTIVE)).count();
                            System.out.printf("%d: %d cells.\n", iteration, count);
                            String result = HGC.execute("match (c:CELL)-[:CONTAINS]->(ch:CHROMOSOME) where not (c:INACTIVE) " +
                                    "with c, collect(ch) as chrs " +
                                    "with c," +
                                    "size(filter(x in chrs where (x:CH1))) as ch1, " +
                                    "size(filter(x in chrs where (x:CH2))) as ch2, " +
                                    "size(filter(x in chrs where (x:CH3))) as ch3, " +
                                    "size(filter(x in chrs where (x:CH4))) as ch4, " +
                                    "size(filter(x in chrs where (x:CH5))) as ch5, " +
                                    "size(filter(x in chrs where (x:CH6))) as ch6 " +
                                    "return distinct count(c) as cells, ch1, ch2, ch3, ch4, ch5, ch6 " +
                                    "order by cells desc").resultAsString();
                            System.out.println(result);
                            return count > 7000;
                        })
                        .withSimulation(new SimImpl())
                        .createIHDN();

                IHDN.computeAll(100, 1);
            }
        }
    }

}
