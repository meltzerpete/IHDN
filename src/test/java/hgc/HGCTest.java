package hgc;

import javafx.util.Pair;
import org.junit.Test;

import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.Assert.*;

public class HGCTest {

    @Test
    public void testReduce() throws Exception {
        // compute children
//        Pair<Stream<T>, double[]> resultFromChildren = getAllChildNodes(node)
//                .map(node1 -> compute(node1, combined_filter));
        Pair<Stream<Double>, double[]> reduce = Stream.of(
                new Pair<>(2.0, new double[]{1.0, 0.0}),
                new Pair<>(3.0, new double[]{2.0, 0.3}))
                .map(tPair -> new Pair<>(Stream.of(tPair.getKey()), tPair.getValue()))
                .reduce(new Pair<>(Stream.empty(), new double[2]),
                        (acc, next) -> {
                            // combine results
                            Stream<Double> resultStream = Stream.concat(acc.getKey(), next.getKey());
                            // combine votes
                            double[] accFilter = acc.getValue();
                            double[] nextFilter = next.getValue();
                            for (int i = 0; i < accFilter.length; i++) accFilter[i] += nextFilter[i];

                            return new Pair<>(resultStream, accFilter);
                        });

        Stream<Double> key = reduce.getKey();
        double[] value = reduce.getValue();

        key.forEach(System.out::println);
        System.out.println(Arrays.toString(value));
    }

}