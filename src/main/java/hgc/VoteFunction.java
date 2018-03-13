package hgc;

import java.util.stream.Stream;

public interface VoteFunction {

    double[] apply(HGC HGC, HGNode hgNode, Stream<double[]> votesFromChildren);

    VoteFunction DEFAULT = (HGC, hgNode, votesFromChildren) -> {
        double[] vote = hgNode.getVote();

        double[] combinedVotesFromChildren = votesFromChildren.reduce(new double[vote.length],
                (acc, next) -> {
                    double[] combinedVote = new double[vote.length];
                    for (int i = 0; i < combinedVote.length; i++)
                        combinedVote[i] += next[i];
                    return combinedVote;
                });

        for (int i = 0; i < vote.length; i++) vote[i] += combinedVotesFromChildren[i];

        return vote;
    };
}