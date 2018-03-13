package hgc;

import java.util.function.Consumer;

public interface GraphBuilder<T> extends Consumer<HGC<T>> {

    @Override
    void accept(HGC<T> HGC);
}
