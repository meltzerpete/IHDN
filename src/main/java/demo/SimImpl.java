package demo;

import hgc.*;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;

import java.util.Arrays;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

public class SimImpl implements Simulation {

    public enum labels implements Label {
        TISSUE, CELL, GENE, CHROMOSOME, DIV_GENE, APOPT_GENE, SEG_GENE
    }

    public enum relTypes implements RelationshipType {
        FROM
    }

    public static String cypherGraph = "CREATE \n" +
            "  (:TISSUE:ROOT), \n" +
            "  (`0` :CELL ) ,\n" +
            "  (`1` :CELL ) ,\n" +
            "  (`2` :CELL ) ,\n" +
            "  (`3` :CHROMOSOME:C1 {type:1} ) ,\n" +
            "  (`4` :CHROMOSOME:C2 {type:2} ) ,\n" +
            "  (`6` :DIV_GENE ) ,\n" +
            "  (`7` :APOPT_GENE ) ,\n" +
            "  (`8` :SEG_GENE ) ,\n" +
            "  (`9` :APOPT_GENE ) ,\n" +
            "  (`10` :SEG_GENE ) ,\n" +
            "  (`11` :DIV_GENE ) ,\n" +
            "  (`12` :DIV_GENE ) ,\n" +
            "  (`13` :APOPT_GENE ) ,\n" +
            "  (`14` :SEG_GENE ) ,\n" +
            "  (`21` :CHROMOSOME:C3 {type:3} ) ,\n" +
            "  (`22` :CHROMOSOME:C4 {type:4} ) ,\n" +
            "  (`24` :CHROMOSOME:C5 {type:5} ) ,\n" +
            "  (`26` :CHROMOSOME:C6 {type:6} ) ,\n" +
            "  (`0`)-[:`RELATED_TO` ]->(`21`),\n" +
            "  (`0`)-[:`RELATED_TO` ]->(`22`),\n" +
            "  (`2`)-[:`RELATED_TO` ]->(`24`),\n" +
            "  (`2`)-[:`RELATED_TO` ]->(`26`),\n" +
            "  (`1`)-[:`RELATED_TO` ]->(`3`),\n" +
            "  (`1`)-[:`RELATED_TO` ]->(`4`),\n" +
            "  (`21`)-[:`RELATED_TO` ]->(`6`),\n" +
            "  (`21`)-[:`RELATED_TO` ]->(`7`),\n" +
            "  (`22`)-[:`RELATED_TO` ]->(`8`),\n" +
            "  (`26`)-[:`RELATED_TO` ]->(`9`),\n" +
            "  (`24`)-[:`RELATED_TO` ]->(`10`),\n" +
            "  (`24`)-[:`RELATED_TO` ]->(`11`),\n" +
            "  (`3`)-[:`RELATED_TO` ]->(`12`),\n" +
            "  (`4`)-[:`RELATED_TO` ]->(`13`),\n" +
            "  (`4`)-[:`RELATED_TO` ]->(`14`)";

    public static GraphBuilder<Double> graphBuilder = HGC -> {
        // Connect tissue to cells
        HGC.getHGNodes(labels.TISSUE)
                .forEach(tissueNode -> HGC.getHGNodes(labels.CELL)
                        .forEach(cellNode -> tissueNode.createRelationshipTo(cellNode, HGRelTypes.CONTAINS)));

        // set filters to [0, 0] for all nodes below level of cell
        HGC.getHGNodes(labels.CHROMOSOME)
                .forEach(hgNode -> hgNode.setProperty(Properties.FILTER, new double[]{0, 0}));
        HGC.getHGNodes(labels.GENE)
                .forEach(hgNode -> hgNode.setProperty(Properties.FILTER, new double[]{0, 0}));

        // duplicate each chromosome
        HGC.getHGNodes(labels.CHROMOSOME).forEach(HGNode::deepClone);

        // set vote function for chromosomes
        HGC.getHGNodes(labels.CELL).forEach(cellNode ->
                cellNode.setProperty("voteFunction", "cellVote"));

        // set votes for each gene type
        HGC.getHGNodes(labels.DIV_GENE).forEach(
                hgNode -> hgNode.setProperty(Properties.VOTE, new double[]{1, 0}));

        HGC.getHGNodes(labels.APOPT_GENE).forEach(
                hgNode -> hgNode.setProperty(Properties.VOTE, new double[]{0, 1}));

        // fill tissue -> 100 cells of random distribution of each gene distribution
        ThreadLocalRandom random = ThreadLocalRandom.current();
        HGNode[] hgNodes = HGC.getHGNodes(labels.CELL).toArray(HGNode[]::new);
        for (int i = 0; i < 97; i++)
            hgNodes[random.nextInt(hgNodes.length)].deepClone();
    };

    @HGFunctionDefinition
    public HGFunction<Double> copy = (HGC, hgNode, resultFromChildren) -> {
        if (hgNode.hasLabel(labels.CELL)) {
            HGNode cellCopy = hgNode.deepClone();
            hgNode.createRelationshipTo(cellCopy, relTypes.FROM);

            int numSegGenes = hgNode.getAllChildNodes()
                    .mapToInt(chromosomeNode ->
                            (int) chromosomeNode.getChildNodesWithLabel(labels.SEG_GENE).count())
                    .sum();


        }

        return null;
    };

    @HGFunctionDefinition
    public HGFunction<Double> die = (HGC, hgNode, resultFromChildren) -> {
        if (hgNode.hasLabel(labels.CELL)) {
            hgNode.setInactive();
        }
        return null;
    };

    @VoteFunctionDefinition
    public VoteFunction cellVote = (HGC, hgNode, votesFromChildren) -> {
        // can intercept votes here
//        if (HGC.getActiveHGNodes(labels.CELL).count() > 200) {
//            return new double[]{0, 1};
//        } else
        double[] vote = VoteFunction.DEFAULT.apply(HGC, hgNode, votesFromChildren);
//        System.out.println("Cell recieved votes: " + Arrays.toString(vote));
        return vote;
    };

    public static void main(String... args) {
        HGC<Double> hgc = new HGC.HGCBuilder<Double>()
                .withNewDB("temp.db")
                .withCypherStatement(cypherGraph)
                .withGraphBuilder(graphBuilder)
                .setIterationMonitor((iteration, HGC) -> {

                    System.out.println(
                            iteration + ": " +
                                    HGC.getActiveHGNodes(labels.CELL).count() +
                                    " cells.");

                    HGC.getHGNodes(labels.CHROMOSOME)
                            .filter(hgNode -> hgNode.getAllParentNodes().allMatch(HGNode::isActive))
                            .collect(Collectors.groupingBy(HGNode::getLabels))
                            .forEach((labels, hgNodes) ->
                                    System.out.println(labels + ": " + hgNodes.size()));

                })
                .withSimulation(new SimImpl())
                .createHGC();

        hgc.computeAll(100);
    }

}
