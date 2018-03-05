package main;

import org.neo4j.graphdb.Label;

public class TestFunctions extends Functions {

    enum labels implements Label { TISSUE, CELL, GENE }

    public HGC.NodeFunction divide = sNode -> {
        if (sNode.hasLabel(labels.CELL)) {
            sNode.deepClone();
        }
        return new double[]{1.0, 0.0};
    };

    public HGC.NodeFunction die = sNode -> {

        if (sNode.hasLabel(labels.CELL)) {
            sNode.recursiveDelete();
        }
        return new double[]{1.0, 0.0};
    };
}
