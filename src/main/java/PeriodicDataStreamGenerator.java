import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

// It just generates Flink DataStream periodically (interval can be configured)
public class PeriodicDataStreamGenerator implements ParallelSourceFunction<Tuple1<Long>> {

    private final long intervalInMillis;
    private final long countToStop;

    private volatile boolean isRunning = true;

    PeriodicDataStreamGenerator(long intervalInMillis, long count) {
        this.intervalInMillis = intervalInMillis;
        countToStop = count;
    }

    @Override
    public void run(SourceContext<Tuple1<Long>> sourceContext) throws Exception {
        long count = 0;
        while (isRunning) {
            count++;
            if (count != countToStop) {
                long now = System.currentTimeMillis();
                sourceContext.collect(new Tuple1<>(now));
            }
            Thread.sleep(intervalInMillis);
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}
