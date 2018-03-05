/*
 * Copyright 2018 pete meltzer
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *	   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package main;

import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Hello world!
 */
public class HGC {

    private static HGC hgc;

    private static final Logger log = LoggerFactory.getLogger(HGC.class);
    static final ThreadLocalRandom random = ThreadLocalRandom.current();

    final Map<NodeFunction, Integer> functionMap;

    private final GraphDatabaseService DB;
    private final long maxIterations;
    private final NodeFunction[] nodeFunctions;
    private final String[] functionNames;
    private final Label rootLabel;
    private double[] rootFilter;

    public void computeAll() {
        computeAll(maxIterations);
    }

    public void computeAll(long maxIterations) {
        long millis = System.currentTimeMillis();
//        log.info("Beginning simulation...\n" +
//                        "maxIterations: {}\n" +
//                        "rootFilter: {}\n" +
//                        "rootLabel: {}\n" +
//                        "functions: {}\n",
//                maxIterations, Arrays.toString(rootFilter), rootLabel.name(), Arrays.toString(functionNames));

        log.info("Beginning simulation...");
        log.info("maxIterations: {}", maxIterations);
        log.info("rootFilter: {}", Arrays.toString(rootFilter));
        log.info("rootLabel: {}", rootLabel);
        log.info("functions: {}", Arrays.toString(functionNames));

        long iteration = 0;
        long remaining = maxIterations;
        while (remaining > 0) {
            long batchSize = 10;
            batchSize = Math.min(batchSize, remaining);
            remaining -= batchSize;

            try (Transaction tx = DB.beginTx()) {
                for (long i = 0; i < batchSize; i++) {
                    if (preIterationMonitor != null) preIterationMonitor.accept(iteration);
                    hgc.findSNodes(rootLabel)
                            .forEach(sNode -> sNode.compute(rootFilter, nodeFunctions));
                    if (postIterationMonitor != null) postIterationMonitor.accept(iteration);
                    iteration++;
                }
                tx.success();
            }
        }

        log.info("Completed {} iterations in {} millis", iteration, System.currentTimeMillis() - millis);

        try (Transaction tx = DB.beginTx()) {
            if (postExecutionMonitor != null) postExecutionMonitor.accept(iteration);
            tx.success();
        }
    }

    private Consumer<Long> preIterationMonitor;

    public HGC setPreIterationMonitor(Consumer<Long> iterationMonitor) {
        this.preIterationMonitor = iterationMonitor;
        return this;
    }

    private Consumer<Long> postIterationMonitor;

    public HGC setPostIterationMonitor(Consumer<Long> postIterationMonitor) {
        this.postIterationMonitor = postIterationMonitor;
        return this;
    }

    private Consumer<Long> postExecutionMonitor;

    public HGC setPostExecutionMonitor(Consumer<Long> postExecutionMonitor) {
        this.postExecutionMonitor = postExecutionMonitor;
        return this;
    }

    public static HGC get() {
        if (HGC.hgc == null)
            throw new RuntimeException("HGC must be instantiated first using the HGC.Builder class");
        return HGC.hgc;
    }

    String getFunctionName(int i) {
        return functionNames[i];
    }

    // delegation
    public SNode createSNode() {
        return new SNode(DB.createNode());
    }

    public SNode createSNode(Label... labels) {
        return new SNode(DB.createNode(labels));
    }

    public SNode getSNodeById(long l) {
        return new SNode(DB.getNodeById(l));
    }

    public Stream<SNode> getAllSNodes() {
        return DB.getAllNodes().stream()
                .map(SNode::new);
    }

    public Stream<SNode> findSNodes(Label label) {
        return DB.findNodes(label).stream()
                .map(SNode::new);
    }

    public HGC withRootFilter(double[] filter) {
        if (filter != null) {
            this.rootFilter = rootFilter;
        }
        return this;
    }

    public static class Builder {

        private static final Logger log = LoggerFactory.getLogger(Builder.class);

        // defaults
        private Label rootLabel = labels.ROOT;
        private long maxIterations = 1000L;
        private double[] rootFilter;    // default {1.0, 1.0, ...}

        private GraphDatabaseService DB;
        private List<NodeFunction> nodeFunctionsList = new LinkedList<>();

        private Class functionsClass;

        private String graphString;

        public Builder(GraphDatabaseService DB) {
            this.DB = DB;
        }

        public Builder withGraph(String cypherQuery) {
            this.graphString = cypherQuery;
            return this;
        }

        private GraphBuilder graphBuilder;

        public Builder withGraph(GraphBuilder graphBuilder) {
            this.graphBuilder = graphBuilder;
            return this;
        }

        public Builder maxIterations(long maxIterations) {
            this.maxIterations = maxIterations;
            return this;
        }

        public Builder withRootLabel(Label label) {
            this.rootLabel = label;
            return this;
        }

        public Builder withNodeFunctions(Class<? extends Functions> functionsClass) {
            this.functionsClass = functionsClass;
            return this;
        }

        Builder withRootFilter(double[] filter) {
            this.rootFilter = filter;
            return this;
        }

        public HGC build() throws IllegalAccessException, InstantiationException {

            // load graph
            if (graphString == null && graphBuilder == null) {
                log.warn("No graph given - using empty/existing database.");
            } else {
                if (graphString != null && graphBuilder != null) {
                    log.warn("graphString and graphBuilder both given" +
                            " -- using graphString first, then graphBuilder");
                }
                if (graphString != null) DB.execute(graphString).close();
                if (graphBuilder != null) {
                    try (Transaction tx = DB.beginTx()) {
                        graphBuilder.accept(DB);
                        tx.success();
                    }
                }
            }

            // register all functions
            List<String> functionNames = new LinkedList<>();
            final Functions functions;
            try {
                functions = (Functions) functionsClass.newInstance();
            } catch (InstantiationException | IllegalAccessException e) {
                log.error("Failed to load class {}", functionsClass);
                throw e;
            }
            Field[] declaredFields = functionsClass.getDeclaredFields();
            for (Field declaredField : declaredFields) {
                if (NodeFunction.class.isAssignableFrom(declaredField.getType())) {
                    try {
                        functionNames.add(declaredField.getName());
                        nodeFunctionsList.add((NodeFunction) declaredField.get(functions));
                    } catch (IllegalAccessException e) {
                        log.error("Failed to load NodeFunctions from {}", functionsClass);
                        throw e;
                    }
                }
            }

            NodeFunction[] nodeFunctions = this.nodeFunctionsList.toArray(new NodeFunction[0]);

            Map<NodeFunction, Integer> functionMap = new Hashtable<>();
            for (int i = 0; i < nodeFunctionsList.size(); i++) functionMap.put(nodeFunctionsList.get(i), i);

            // set default root filter
            if (rootFilter == null) {
                rootFilter = new double[nodeFunctions.length];
                Arrays.fill(rootFilter, 1.0);
            }

            return new HGC(
                    DB,
                    maxIterations,
                    rootLabel,
                    nodeFunctions,
                    functionNames.toArray(new String[0]),
                    functionMap,
                    rootFilter,
                    hgc -> functions.hgc = hgc);
        }
    }

    private HGC(
            GraphDatabaseService DB,
            long maxIterations,
            Label rootLabel,
            NodeFunction[] nodeFunctions,
            String[] functionNames,
            Map<NodeFunction, Integer> functionMap,
            double[] rootFilter,
            Consumer<HGC> registerFunctionClass) {
        HGC.hgc = this;
        this.DB = DB;
        this.maxIterations = maxIterations;
        this.rootLabel = rootLabel;
        this.nodeFunctions = nodeFunctions;
        this.functionNames = functionNames;
        this.functionMap = functionMap;
        this.rootFilter = rootFilter;
        registerFunctionClass.accept(this);
    }

    public enum labels implements Label {
        ROOT, ACTIVE
    }

    public enum rels implements RelationshipType {
        CONTAINS
    }

    public static final String SCHEMA_PROPERTY = "schema";

    public interface GraphBuilder extends Consumer<GraphDatabaseService> {
        @Override
        void accept(GraphDatabaseService DB);
    }

    public interface NodeFunction extends Function<SNode, double[]> {
        @Override
        double[] apply(SNode sNode);
    }
}
