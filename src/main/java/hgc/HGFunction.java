package hgc;

import java.util.stream.Stream;

public interface HGFunction<T> {

    T apply(HGC<T> HGC, HGNode hgNode, Stream<T> resultFromChildren);
}
