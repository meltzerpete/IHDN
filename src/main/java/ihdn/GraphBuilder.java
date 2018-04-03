package ihdn;

import java.util.function.Consumer;

public interface GraphBuilder extends Consumer<IHDN> {

    @Override
    void accept(IHDN IHDN);
}
