package main;

import org.neo4j.graphdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class SNode implements Node {

    private static final Logger log = LoggerFactory.getLogger(SNode.class);

    private Node node;

    public SNode(Node node) {
        this.node = node;
    }

    public double[] getSchema() {
        return (double[]) getProperty(HGC.SCHEMA_PROPERTY);
    }

    public double[] getDefaultSchema() {
        double[] schema = new double[getSchema().length];
        Arrays.fill(schema, 1.0);
        return schema;
    }

    public SNode setActive() {
        if (!hasLabel(HGC.labels.ACTIVE)) addLabel(HGC.labels.ACTIVE);
        return this;
    }

    public SNode setInactive() {
        if (hasLabel(HGC.labels.ACTIVE)) removeLabel(HGC.labels.ACTIVE);
        return this;
    }

    public double[] compute(final double[] filter, final HGC.NodeFunction[] nodeFunctions) {

        double[] schema = getSchema();
        if (schema.length != filter.length) {
            log.error("No. of functions defined in schema does not match size of filter.\n" +
                    "-- functions: {}, filter: {}", Arrays.toString(schema), Arrays.toString(filter));
            throw new RuntimeException("No. of functions defined in schema does not match size of filter.");
        }

        // use Hadamard to filter the schema
        for (int i = 0; i < filter.length; i++) schema[i] *= filter[i];

        double sum = Arrays.stream(schema).sum();

        if (sum == 0) {
            // no possible functions
            double[] returnFilter = new double[filter.length];
            Arrays.fill(returnFilter, 1.0);
            return returnFilter;
        }

        double[] childrenReturnedFilter = computeChildren(schema, nodeFunctions);

        for (int i = 0; i < schema.length; i++)
            schema[i] *= childrenReturnedFilter[i];

        sum = Arrays.stream(schema).sum();
        double limit = HGC.random.nextDouble(sum);

        for (int i = 0; i < schema.length; i++) {
            limit -= schema[i];
            if (limit < 0) {
                log.debug("Computing function {} on SNode {} - [{}]",
                        HGC.get().getFunctionName(i),
                        getId(),
                        ((List<Label>) getLabels()).stream()
                                .map(Label::name)
                                .reduce((s, s1) -> String.join(",", s, s1))
                                .orElse(""));
                schema = nodeFunctions[i].apply(this);
                break;
            }
        }

        try {
            setProperty(HGC.SCHEMA_PROPERTY, schema);
        } catch (NotFoundException ex) {
            // do nothing - this node has been deleted
        }
        return schema;
    }

    public double[] computeChildren(final double[] filter, final HGC.NodeFunction[] nodeFunctions) {
        double[] identity = new double[filter.length];
        Arrays.fill(identity, 1.0);

        return getChildNodesWithLabel(HGC.labels.ACTIVE)
                .map(childNode -> childNode.compute(filter, nodeFunctions))
                .reduce(identity, (acc, next) -> {
                    for (int i = 0; i < acc.length; i++)
                        acc[i] *= next[i];
                    return acc;
                });
    }

    public Stream<SNode> getAllChildNodes() {
        return ((ResourceIterator<Relationship>)
                getRelationships(HGC.rels.CONTAINS, Direction.OUTGOING).iterator()).stream()
                .map(Relationship::getEndNode)
                .map(SNode::new);
    }

    public Stream<SNode> getChildNodesWithLabel(Label label) {
        return getAllChildNodes().filter(sNode -> sNode.hasLabel(label));
    }

    public Stream<SNode> getAllParentNodes() {
        return ((ResourceIterator<Relationship>)
                getRelationships(HGC.rels.CONTAINS, Direction.INCOMING).iterator()).stream()
                .map(Relationship::getStartNode)
                .map(SNode::new);
    }

    public SNode shallowClone() {
        SNode sNode = HGC.get().createSNode();
        for (Label label : getLabels()) sNode.addLabel(label);

        for (Map.Entry<String, Object> entry : getAllProperties().entrySet())
            sNode.setProperty(entry.getKey(), entry.getValue());

        getAllParentNodes()
                .forEach(parent -> parent.createRelationshipTo(sNode, HGC.rels.CONTAINS));

        return new SNode(sNode);
    }

    public SNode deepClone() {
        SNode sNode = this.shallowClone();
        getAllChildNodes()
                .map(SNode::deepClone)
                .peek(child ->
                        ((ResourceIterator<Relationship>)
                                getRelationships(HGC.rels.CONTAINS, Direction.OUTGOING).iterator()).stream()
                                .filter(rel -> rel.getEndNode().equals(child))
                                .forEach(Relationship::delete))
                .forEach(child -> sNode.createRelationshipTo(child, HGC.rels.CONTAINS));

        return new SNode(sNode);
    }

    public String prettyPrint() {
        return String.format("(%d) -> [%s]",
                getId(),
                getAllChildNodes()
                        .map(SNode::getId)
                        .map(Object::toString)
                        .reduce((s, s2) -> String.join(",", s, s2))
                        .orElse(""));
    }

    public String deepPrettyPrint() {
        return String.format("(%d) -> [%s]",
                getId(),
                getAllChildNodes()
                        .map(SNode::deepPrettyPrint)
                        .map(Object::toString)
                        .reduce((s, s2) -> String.join(",", s, s2))
                        .orElse(""));
    }

    // delegation
    @Override
    public long getId() {
        return node.getId();
    }

    @Override
    public void delete() {
        ((ResourceIterator<Relationship>) node.getRelationships().iterator()).stream()
                .forEach(Relationship::delete);
        node.delete();
    }

    public void recursiveDelete() {
        getAllChildNodes().forEach(SNode::recursiveDelete);
        delete();
    }

    @Override
    public Iterable<Relationship> getRelationships() {
        return node.getRelationships();
    }

    @Override
    public boolean hasRelationship() {
        return node.hasRelationship();
    }

    @Override
    public Iterable<Relationship> getRelationships(RelationshipType... relationshipTypes) {
        return node.getRelationships(relationshipTypes);
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction direction, RelationshipType... relationshipTypes) {
        return node.getRelationships(direction, relationshipTypes);
    }

    @Override
    public boolean hasRelationship(RelationshipType... relationshipTypes) {
        return node.hasRelationship(relationshipTypes);
    }

    @Override
    public boolean hasRelationship(Direction direction, RelationshipType... relationshipTypes) {
        return node.hasRelationship(direction, relationshipTypes);
    }

    @Override
    public Iterable<Relationship> getRelationships(Direction direction) {
        return node.getRelationships(direction);
    }

    @Override
    public boolean hasRelationship(Direction direction) {
        return node.hasRelationship(direction);
    }

    @Override
    public Iterable<Relationship> getRelationships(RelationshipType relationshipType, Direction direction) {
        return node.getRelationships(relationshipType, direction);
    }

    @Override
    public boolean hasRelationship(RelationshipType relationshipType, Direction direction) {
        return node.hasRelationship(relationshipType, direction);
    }

    @Override
    public Relationship getSingleRelationship(RelationshipType relationshipType, Direction direction) {
        return node.getSingleRelationship(relationshipType, direction);
    }

    @Override
    public Relationship createRelationshipTo(Node node, RelationshipType relationshipType) {
        return this.node.createRelationshipTo(node, relationshipType);
    }

    @Override
    public Iterable<RelationshipType> getRelationshipTypes() {
        return node.getRelationshipTypes();
    }

    @Override
    public int getDegree() {
        return node.getDegree();
    }

    @Override
    public int getDegree(RelationshipType relationshipType) {
        return node.getDegree(relationshipType);
    }

    @Override
    public int getDegree(Direction direction) {
        return node.getDegree(direction);
    }

    @Override
    public int getDegree(RelationshipType relationshipType, Direction direction) {
        return node.getDegree(relationshipType, direction);
    }

    @Override
    public void addLabel(Label label) {
        node.addLabel(label);
    }

    @Override
    public void removeLabel(Label label) {
        node.removeLabel(label);
    }

    @Override
    public boolean hasLabel(Label label) {
        return node.hasLabel(label);
    }

    @Override
    public Iterable<Label> getLabels() {
        return node.getLabels();
    }

    @Override
    public GraphDatabaseService getGraphDatabase() {
        return node.getGraphDatabase();
    }

    @Override
    public boolean hasProperty(String s) {
        return node.hasProperty(s);
    }

    @Override
    public Object getProperty(String s) {
        return node.getProperty(s);
    }

    @Override
    public Object getProperty(String s, Object o) {
        return node.getProperty(s, o);
    }

    @Override
    public void setProperty(String s, Object o) {
        node.setProperty(s, o);
    }

    @Override
    public Object removeProperty(String s) {
        return node.removeProperty(s);
    }

    @Override
    public Iterable<String> getPropertyKeys() {
        return node.getPropertyKeys();
    }

    @Override
    public Map<String, Object> getProperties(String... strings) {
        return node.getProperties(strings);
    }

    @Override
    public Map<String, Object> getAllProperties() {
        return node.getAllProperties();
    }
}
