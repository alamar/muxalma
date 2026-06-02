package pvt.muxalma.fanservice;

import java.util.LinkedList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Lifecycle {
    private static final Logger log = LoggerFactory.getLogger(Lifecycle.class);

    private final List<Runnable> stopMethods = new LinkedList<>();

    public void addStopMethod(Runnable m) {
        stopMethods.add(m);
    }

    public void stop() {
        for (Runnable stopMethods : stopMethods) {
            try {
                stopMethods.run();
            } catch (Throwable th) {
                log.error("While stopping", th);
            }
        }
        stopMethods.clear();
    }
}
