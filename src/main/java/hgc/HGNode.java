package hgc;

import org.neo4j.graphdb.*;
import org.omg.PortableInterceptor.INACTIVE;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Stream;

public class HGNode implements Node {

    private final Logger log = LoggerFactory.getLogger(HGNode.class);

    private final Node node;
    private final HGC HGC;
    private boolean isDeleted;

    HGNode(HGC HGC, Node node) {
        this.HGC = HGC;
        this.node = node;
    }

    public HGNode(HGC HGC) {
        this.HGC = HGC;
        this.node = HGC.getDB().createNode();
    }

    public boolean isDeleted() {
        return this.isDeleted;
    }

    public HGNode(HGC HGC, Label... labels) {
        this.HGC = HGC;
        this.node = HGC.getDB().createNode(labels);
    }

    public void setInactive() {
        node.setProperty(Properties.TIME_INACTIVE, HGC.getCurrentIteration());
        node.addLabel(HGLabels.INACTIVE);
    }

    public boolean isActive() {
        return !hasLabel(HGLabels.INACTIVE);
    }

    public VoteFunction getVoteFunction() {
        if (node.hasProperty(Properties.VOTE_FUNCTION)) {
            String voteFunctionName = (String) node.getProperty(Properties.VOTE_FUNCTION);
            if (HGC.getVoteFunctions().containsKey(voteFunctionName))
                return (VoteFunction) HGC.getVoteFunctions().get(voteFunctionName);
            else
                throw new RuntimeException("Vote function " + voteFunctionName + " not found.");
        } else {
            return VoteFunction.DEFAULT;
        }
    }

    public double[] getFilter() {
        double[] filter;
        if (node.hasProperty(Properties.FILTER))
            filter = (double[]) node.getProperty(Properties.FILTER);
        else {
            filter = new double[HGC.getNumFunctions()];
            Arrays.fill(filter, 1.0);
        }
        return filter;
    }

    public double[] getVote() {
        return (double[]) getProperty(Properties.VOTE);
    }

    public Stream<HGNode> getAllChildNodes() {
        return ((ResourceIterator<Relationship>)
                getRelationships(HGRelTypes.CONTAINS, Direction.OUTGOING).iterator()).stream()
                .map(Relationship::getEndNode)
                .map(node1 -> new HGNode(HGC, node1));
    }

    public Stream<HGNode> getChildNodesWithLabel(Label label) {
        return getAllChildNodes().filter(hgNode -> hgNode.hasLabel(label));
    }

    public Stream<HGNode> getAllParentNodes() {
        return ((ResourceIterator<Relationship>)
                getRelationships(HGRelTypes.CONTAINS, Direction.INCOMING).iterator()).stream()
                .map(Relationship::getStartNode)
                .map(node1 -> new HGNode(HGC, node1));
    }

    public HGNode shallowClone() {
        HGNode hgNode = new HGNode(HGC);
        for (Label label : getLabels()) hgNode.addLabel(label);

        for (Map.Entry<String, Object> entry : getAllProperties().entrySet())
            hgNode.setProperty(entry.getKey(), entry.getValue());

        getAllParentNodes()
                .forEach(parent -> parent.createRelationshipTo(hgNode, HGRelTypes.CONTAINS));

        return hgNode;
    }

    public HGNode deepClone() {
        HGNode hgNode = this.shallowClone();
        getAllChildNodes()
                .map(HGNode::deepClone)
                .peek(child ->
                        ((ResourceIterator<Relationship>)
                                getRelationships(main.HGC.rels.CONTAINS, Direction.OUTGOING).iterator()).stream()
                                .filter(rel -> rel.getEndNode().equals(child))
                                .forEach(Relationship::delete))
                .forEach(child -> hgNode.createRelationshipTo(child, HGRelTypes.CONTAINS));

        return hgNode;
    }

    public String prettyPrint() {
        return String.format("(%d) -> [%s]",
                getId(),
                getAllChildNodes()
                        .map(HGNode::getId)
                        .map(Object::toString)
                        .reduce((s, s2) -> String.join(",", s, s2))
                        .orElse(""));
    }

    public String deepPrettyPrint() {
        return String.format("(%d) -> [%s]",
                getId(),
                getAllChildNodes()
                        .map(HGNode::deepPrettyPrint)
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
        this.isDeleted = true;
    }

    public void recursiveDelete() {
        getAllChildNodes().forEach(HGNode::recursiveDelete);
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
