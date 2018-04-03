package ihdn;

import org.neo4j.graphdb.*;
import org.neo4j.graphdb.factory.GraphDatabaseFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Stream;

public class IHDN {

    private final static Logger log = LoggerFactory.getLogger(IHDN.class);
    private final GraphDatabaseService DB;
    private final ThreadLocalRandom random = ThreadLocalRandom.current();
    private final int iterationsPerMonitor;
    private final IterationMonitor iterationMonitor;
    private final double[] rootFilter;
    private final IHDNFunction[] ihdnFunctions;
    private final Map<IHDNFunction, String> hgFunctionNames;
    private final Map<String, VoteFunction> voteFunctions;


    private IHDN(
            GraphDatabaseService DB,
            int iterationsPerMonitor,
            IterationMonitor iterationMonitor,
            double[] rootFilter,
            IHDNFunction[] ihdnFunctions,
            Map<IHDNFunction, String> hgFunctionNames,
            Map<String, VoteFunction> voteFunctions) {
        this.DB = DB;
        this.iterationsPerMonitor = iterationsPerMonitor;
        this.iterationMonitor = iterationMonitor;
        this.rootFilter = rootFilter;
        this.ihdnFunctions = ihdnFunctions;
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
            if (iterationMonitor != null) iterationMonitor.apply(iteration, this);
            tx.success();
        }

        boolean finished = false;

        while (remaining > 0) {
            batchSize = Math.min(batchSize, remaining);
            remaining -= batchSize;

            try (Transaction tx = DB.beginTx()) {
                for (int i = 0; i < batchSize; i++) {
                    // compute here

                    DB.findNodes(IHDNLabels.ROOT).stream().forEach(node -> compute(new IHDNNode(this, node), rootFilter));
                    iteration++;
                    if (iterationMonitor != null && iteration % iterationsPerMonitor == 0) {
                        finished = iterationMonitor.apply(iteration, this);
                        if (finished) break;
                    }
                }
                tx.success();
                if (finished) break;
            }
        }
    }

    private double[] compute(IHDNNode ihdnNode, double[] parentFilter) {
        if (!ihdnNode.isActive()) return null;

        // combine filter
        double[] combined_filter = new double[ihdnFunctions.length];
        double[] node_filter = ihdnNode.getFilter();

        for (int i = 0; i < combined_filter.length; i++)
            combined_filter[i] = parentFilter[i] * node_filter[i];

        // compute children and collect votes
        Stream<double[]> childVotes = ihdnNode.getAllChildNodes()
                .filter(hgNode1 -> !hgNode1.isDeleted())
                .map(hgNode1 -> compute(hgNode1, combined_filter))
                .filter(Objects::nonNull);

        // vote function
        double[] vote = new double[ihdnFunctions.length];
        VoteFunction voteFunction = ihdnNode.getVoteFunction();
        vote = voteFunction.apply(this, ihdnNode, childVotes);

        for (int i = 0; i < combined_filter.length; i++)
            combined_filter[i] *= vote[i];

        // function to perform
        IHDNFunction function = getIHDNFunction(combined_filter);
        if (function == null) {
            log.debug("No function to perform for node {}.", ihdnNode.getId());
        } else {
            log.debug("Performing function {} on node {}.", hgFunctionNames.get(function), ihdnNode.getId());
            function.accept(this, ihdnNode);
        }

        // node may now have deleted itself -> if so return null?
        if (ihdnNode.isDeleted()) return null;

        return vote;
    }

    Map<String, VoteFunction> getVoteFunctions() {
        return this.voteFunctions;
    }

    private IHDNFunction getIHDNFunction(double[] filter) {
        double sum = Arrays.stream(filter).sum();
        if (sum == 0) {
            // no possible function to perform
            return null;
        }
        double limit = random.nextDouble(sum);
        for (int i = 0; i < filter.length; i++) {
            limit -= filter[i];
            if (limit < 0) return ihdnFunctions[i];
        }
        throw new RuntimeException("Function selection overran.");
    }

    int getNumFunctions() {
        return this.ihdnFunctions.length;
    }

    public Result execute(String s) throws QueryExecutionException {
        return DB.execute(s);
    }

    GraphDatabaseService getDB() {
        return this.DB;
    }

    public Stream<IHDNNode> getIHDNNodes(Label label) {
        return DB.findNodes(label).stream().map(node -> new IHDNNode(this, node));
    }

    public Stream<IHDNNode> getActiveIHDNNodes(Label label) {
        return DB.findNodes(label).stream()
                .filter(node -> !node.hasLabel(IHDNLabels.INACTIVE))
                .map(node -> new IHDNNode(this, node));
    }

    public int getCurrentIteration() {
        return iteration;
    }

    public static class IHDNBuilder {

        private GraphDatabaseService db;
        private int iterationsPerMonitor;
        private IterationMonitor iterationMonitor;
        private double[] rootFilter;
        private IHDNFunction[] ihdnFunctions;
        private Map<IHDNFunction, String> hgFunctionNames;
        private Map<String, VoteFunction> voteFunctions;
        private String cypherStatement;
        private GraphBuilder graphBuilder;
        private double[] defaultVote;

        public IHDNBuilder withExistingDB(String fileName) {
            if (this.db != null) throw new RuntimeException("Must choose one from withExistingDB() and withNewDB()");

            File file = new File(fileName);
            if (!file.exists())
                throw new RuntimeException("Database file does not exist.");

            this.db = new GraphDatabaseFactory().newEmbeddedDatabase(file);
            return this;
        }

        public IHDNBuilder withNewDB(String fileName) {
            if (this.db != null) throw new RuntimeException("Must choose one from withExistingDB() and withNewDB()");

            File file = new File(fileName);
            if (file.exists())
                throw new RuntimeException("File already exists.");

            this.db = new GraphDatabaseFactory().newEmbeddedDatabase(file);
            return this;
        }

        public IHDNBuilder setIterationsPerMonitor(int iterationsPerMonitor) {
            this.iterationsPerMonitor = iterationsPerMonitor;
            return this;
        }

        public IHDNBuilder setIterationMonitor(IterationMonitor iterationMonitor) {
            this.iterationMonitor = iterationMonitor;
            return this;
        }

        public IHDNBuilder setRootFilter(double[] rootFilter) {
            this.rootFilter = rootFilter;
            return this;
        }

        private Simulation simulation;

        public IHDNBuilder withSimulation(Simulation simulation) {
            this.simulation = simulation;
            return this;
        }

        public IHDNBuilder withGraphBuilder(GraphBuilder graphBuilder) {
            this.graphBuilder = graphBuilder;
            return this;
        }

        public IHDNBuilder withCypherStatement(String cypherStatement) {
            this.cypherStatement = cypherStatement;
            return this;
        }

        public IHDN createIHDN() {
            try (Transaction tx = db.beginTx()) {
                if (simulation == null) throw new RuntimeException("No simulation provided.");

                // set up array of HGFunctions
                List<IHDNFunction> ihdnFunctionList = new LinkedList<>();
                hgFunctionNames = new HashMap<>();
                voteFunctions = new HashMap<>();

                try {
                    for (Field field : simulation.getClass().getDeclaredFields()) {
                        if (field.getAnnotation(IHDNFunctionDefinition.class) != null) {
                            if (field.getType().isAssignableFrom(IHDNFunction.class)) {
                                IHDNFunction function = (IHDNFunction) field.get(simulation);
                                ihdnFunctionList.add(function);
                                hgFunctionNames.put(function, field.getName());
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

                if (ihdnFunctionList.isEmpty()) throw new RuntimeException("No HGFunctions provided.");
                this.ihdnFunctions = ihdnFunctionList.toArray(new IHDNFunction[0]);

                // default iterationsPerMonitor
                if (iterationsPerMonitor == 0) iterationsPerMonitor = 1;
                // default root filter
                if (rootFilter == null) {
                    rootFilter = new double[ihdnFunctions.length];
                    Arrays.fill(rootFilter, 1.0);
                }

                // set up Map for voteFunctions
                if (cypherStatement != null) db.execute(cypherStatement);
                IHDN ihdn = new IHDN(db, iterationsPerMonitor, iterationMonitor, rootFilter, ihdnFunctions, hgFunctionNames, voteFunctions);
                if (graphBuilder != null) {
                    graphBuilder.accept(ihdn);
                }

                // default vote
                defaultVote = new double[ihdnFunctions.length];
                Arrays.fill(defaultVote, 0.0);

                db.getAllNodes().stream()
                        .filter(node -> !node.hasProperty(Properties.VOTE))
                        .forEach(node -> node.setProperty(Properties.VOTE, defaultVote));

                if (db.findNodes(IHDNLabels.ROOT).stream().count() == 0)
                    throw new RuntimeException("No ROOT nodes found.");


                tx.success();
                return ihdn;
            }
        }
    }
}

