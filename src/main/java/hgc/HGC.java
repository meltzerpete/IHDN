package hgc;

import javafx.util.Pair;
import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

public class HGC<T> {

    private final static Logger log = LoggerFactory.getLogger(HGC.class);
    private final GraphDatabaseService DB;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final int iterationsPerMonitor;
    private final IterationMonitor iterationMonitor;
    private final double[] rootFilter;
    private final HGFunction<T>[] hgFunctions;
    private final Map<HGFunction<T>, String> hgFunctionNames;
    private final Map<String, VoteFunction> voteFunctions;


    private HGC(
            GraphDatabaseService DB,
            int iterationsPerMonitor,
            IterationMonitor iterationMonitor,
            double[] rootFilter,
            HGFunction<T>[] hgFunctions,
            Map<HGFunction<T>, String> hgFunctionNames,
            Map<String, VoteFunction> voteFunctions) {
        this.DB = DB;
        this.iterationsPerMonitor = iterationsPerMonitor;
        this.iterationMonitor = iterationMonitor;
        this.rootFilter = rootFilter;
        this.hgFunctions = hgFunctions;
        this.hgFunctionNames = hgFunctionNames;
        this.voteFunctions = voteFunctions;
    }

    public void computeAll(int maxIterations) {
        computeAll(maxIterations, 100);
    }

    private int iteration;

    public void computeAll(int maxIterations, int batchSize) {
        iteration = 0;
        int remaining = maxIterations;

        try (Transaction tx = DB.beginTx()) {
            if (iterationMonitor != null) iterationMonitor.accept(iteration, this);
            tx.success();
        }

        while (remaining > 0) {
            batchSize = Math.min(batchSize, remaining);
            remaining -= batchSize;

            try (Transaction tx = DB.beginTx()) {
                for (int i = 0; i < batchSize; i++) {
                    // compute here

                    DB.findNodes(HGLabels.ROOT).stream().forEach(node -> compute(new HGNode(this, node), rootFilter));
                    iteration++;
                    if (iterationMonitor != null && iteration % iterationsPerMonitor == 0)
                        iterationMonitor.accept(iteration, this);
                }
                tx.success();
            }
        }
    }

    private Pair<T, double[]> compute(HGNode hgNode, double[] parentFilter) {
        if (!hgNode.isActive()) return null;

        // combine filter
        double[] combined_filter = new double[hgFunctions.length];
        double[] node_filter = hgNode.getFilter();

        for (int i = 0; i < combined_filter.length; i++)
            combined_filter[i] = parentFilter[i] * node_filter[i];

        // compute children and collect votes
        Pair<Stream<T>, Stream<double[]>> resultFromChildren = hgNode.getAllChildNodes()
                .filter(hgNode1 -> !hgNode1.isDeleted())
                .map(hgNode1 -> compute(hgNode1, combined_filter))
                .filter(Objects::nonNull)
                .map(tPair -> new Pair<>(Stream.of(tPair.getKey()), Stream.of(tPair.getValue())))
                .reduce(new Pair<>(Stream.empty(), Stream.empty()),
                        (acc, next) -> {
                            // combine results
                            Stream<T> resultStream = Stream.concat(acc.getKey(), next.getKey());
                            // combine votes
                            Stream<double[]> votesStream = Stream.concat(acc.getValue(), next.getValue());

                            return new Pair<>(resultStream, votesStream);
                        });

        // vote function
        double[] vote = new double[hgFunctions.length];
        VoteFunction voteFunction = hgNode.getVoteFunction();
        if (voteFunction != null)
            vote = voteFunction.apply(this, hgNode, resultFromChildren.getValue());

        for (int i = 0; i < combined_filter.length; i++)
            combined_filter[i] *= vote[i];

        // function to perform
        T functionResult = null;

        HGFunction<T> function = getHGFunction(combined_filter);
        if (function == null) {
            log.debug("No function to perform for node {}.", hgNode.getId());
        } else {
            log.debug("Performing function {} on node {}.", hgFunctionNames.get(function), hgNode.getId());
            functionResult = function.apply(this, hgNode, resultFromChildren.getKey());
        }

        // node may now have deleted itself -> if so return null?
        if (hgNode.isDeleted()) return null;

        return new Pair<>(functionResult, vote);
    }

    Map<String, VoteFunction> getVoteFunctions() {
        return this.voteFunctions;
    }

    private HGFunction<T> getHGFunction(double[] filter) {
        double sum = Arrays.stream(filter).sum();
        if (sum == 0) {
            // no possible function to perform
            return null;
        }
        double limit = random.nextDouble(sum);
        for (int i = 0; i < filter.length; i++) {
            limit -= filter[i];
            if (limit < 0) return hgFunctions[i];
        }
        throw new RuntimeException("Function selection overran.");
    }

    int getNumFunctions() {
        return this.hgFunctions.length;
    }

    public Result execute(String s) throws QueryExecutionException {
        return DB.execute(s);
    }

    GraphDatabaseService getDB() {
        return this.DB;
    }

    public Stream<HGNode> getHGNodes(Label label) {
        return DB.findNodes(label).stream().map(node -> new HGNode(this, node));
    }

    public Stream<HGNode> getActiveHGNodes(Label label) {
        return DB.findNodes(label).stream()
                .filter(node -> !node.hasLabel(HGLabels.INACTIVE))
                .map(node -> new HGNode(this, node));
    }

    public int getCurrentIteration() {
        return iteration;
    }
    public static class HGCBuilder<T> {

        private GraphDatabaseService db;
        private int iterationsPerMonitor;
        private IterationMonitor<T> iterationMonitor;
        private double[] rootFilter;
        private HGFunction<T>[] hgFunctions;
        private Map<HGFunction<T>, String> hgFunctionNames;
        private Map<String, VoteFunction> voteFunctions;
        private String cypherStatement;
        private GraphBuilder<T> graphBuilder;
        private double[] defaultVote;

        public HGCBuilder<T> withExistingDB(String fileName) {
            if (this.db != null) throw new RuntimeException("Must choose one from withExistingDB() and withNewDB()");

            File file = new File(fileName);
            if (!file.exists())
                throw new RuntimeException("Database file does not exist.");

            this.db = new GraphDatabaseFactory().newEmbeddedDatabase(file);
            return this;
        }

        public HGCBuilder<T> withNewDB(String fileName) {
            if (this.db != null) throw new RuntimeException("Must choose one from withExistingDB() and withNewDB()");

            File file = new File(fileName);
            if (file.exists())
                throw new RuntimeException("File already exists.");

            this.db = new GraphDatabaseFactory().newEmbeddedDatabase(file);
            return this;
        }

        public HGCBuilder<T> setIterationsPerMonitor(int iterationsPerMonitor) {
            this.iterationsPerMonitor = iterationsPerMonitor;
            return this;
        }

        public HGCBuilder<T> setIterationMonitor(IterationMonitor<T> iterationMonitor) {
            this.iterationMonitor = iterationMonitor;
            return this;
        }

        public HGCBuilder<T> setRootFilter(double[] rootFilter) {
            this.rootFilter = rootFilter;
            return this;
        }

        private Simulation simulation;

        public HGCBuilder<T> withSimulation(Simulation simulation) {
            this.simulation = simulation;
            return this;
        }

        public HGCBuilder<T> withGraphBuilder(GraphBuilder<T> graphBuilder) {
            this.graphBuilder = graphBuilder;
            return this;
        }

        public HGCBuilder<T> withCypherStatement(String cypherStatement) {
            if (cypherStatement != null) {
                cypherStatement = cypherStatement.replaceAll("\\)-\\[:[^\\]]*\\]->\\(", ")-[:CONTAINS]->(");
                cypherStatement = cypherStatement.replaceAll("\\)<-\\[:[^\\]]*\\]-\\(", ")<-[:CONTAINS]-(");
                cypherStatement = cypherStatement.replaceAll("\\)->\\(", ")-[:CONTAINS]->(");
                cypherStatement = cypherStatement.replaceAll("\\)<-\\(", ")<-[:CONTAINS]-(");
                log.debug("Cypher graph builder statement replaced with:\n{}", cypherStatement);
            }
            this.cypherStatement = cypherStatement;
            return this;
        }

        public HGC<T> createHGC() {
            try (Transaction tx = db.beginTx()) {
                if (simulation == null) throw new RuntimeException("No simulation provided.");

                // set up array of HGFunctions
                List<HGFunction> hgFunctionList = new LinkedList<>();
                hgFunctionNames = new HashMap<>();
                voteFunctions = new HashMap<>();

                try {
                    for (Field field : simulation.getClass().getDeclaredFields()) {
                        if (field.getAnnotation(HGFunctionDefinition.class) != null) {
                            if (field.getType().isAssignableFrom(HGFunction.class)) {
                                HGFunction<T> thgFunction = (HGFunction<T>) field.get(simulation);
                                hgFunctionList.add(thgFunction);
                                hgFunctionNames.put(thgFunction, field.getName());
                            }
                        } else if (field.getAnnotation(VoteFunctionDefinition.class) != null) {
                            if (field.getType().isAssignableFrom(VoteFunction.class)) {
                                this.voteFunctions.put(field.getName(), ((VoteFunction) field.get(simulation)));
                            }
                        }
                    }
                } catch (IllegalAccessException e) {
                    throw new RuntimeException(e);
                }

                if (hgFunctionList.isEmpty()) throw new RuntimeException("No HGFunctions provided.");
                this.hgFunctions = hgFunctionList.toArray(new HGFunction[0]);

                // default iterationsPerMonitor
                if (iterationsPerMonitor == 0) iterationsPerMonitor = 1;
                // default root filter
                if (rootFilter == null) {
                    rootFilter = new double[hgFunctions.length];
                    Arrays.fill(rootFilter, 1.0);
                }

                // set up Map for voteFunctions
                if (cypherStatement != null) db.execute(cypherStatement);
                HGC<T> thgc = new HGC<>(db, iterationsPerMonitor, iterationMonitor, rootFilter, hgFunctions, hgFunctionNames, voteFunctions);
                if (graphBuilder != null) {
                    graphBuilder.accept(thgc);
                }

                // default vote
                defaultVote = new double[hgFunctions.length];
                Arrays.fill(defaultVote, 0.0);

                db.getAllNodes().stream()
                        .filter(node -> !node.hasProperty(Properties.VOTE))
                        .forEach(node -> node.setProperty(Properties.VOTE, defaultVote));

                if (db.findNodes(HGLabels.ROOT).stream().count() == 0)
                    throw new RuntimeException("No ROOT nodes found.");


                tx.success();
                return thgc;
            }
        }
    }
}

