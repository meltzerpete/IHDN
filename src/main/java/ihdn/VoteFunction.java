package ihdn;

import java.util.stream.Stream;

public interface VoteFunction {

    double[] apply(IHDN IHDN, IHDNNode ihdnNode, Stream<double[]> votesFromChildren);

    VoteFunction DEFAULT = (IHDN, ihdnNode, votesFromChildren) -> {
        double[] vote = ihdnNode.getVote();

        double[] combinedVotesFromChildren = votesFromChildren.reduce(new double[vote.length],
                (acc, next) -> {
//                    double[] combinedVote = new double[vote.length];
                    for (int i = 0; i < acc.length; i++)
                        acc[i] += next[i];
                    return acc;
                });

        for (int i = 0; i < vote.length; i++) vote[i] += combinedVotesFromChildren[i];

        return vote;
    };
}