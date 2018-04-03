package ihdn;

import java.util.function.BiConsumer;

public interface IHDNFunction extends BiConsumer<IHDN, IHDNNode> {

    @Override
    void accept(IHDN IHDN, IHDNNode ihdnNode);
}
