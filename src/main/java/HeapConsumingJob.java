import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.Date;


public class HeapConsumingJob {

    // Default Params
    private static int interval = 1;
    private static int size = 1024 * 1024;
    private static long count = 0;

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params);

        if (params.has("interval")) {
            interval = Integer.valueOf(params.get("interval"));
        }
        if (params.has("size")) {
            size = Integer.valueOf(params.get("size"));
        }
        if (params.has("count")) {
            count = Long.valueOf(params.get("count"));
        }
        env.addSource(new PeriodicDataStreamGenerator(interval * 1000, count)).setParallelism(1)
                .keyBy(0)
                .flatMap(new heapHolder(size));

        env.execute("Heap Consuming Job");
    }

    public static final class heapHolder extends RichFlatMapFunction<Tuple1<Long>, String> {

        private final int size;

        private transient ListState<Byte[]> byteArraysState;

        heapHolder(int size) {
            this.size = size;
        }

        @Override
        public void flatMap(Tuple1<Long> in, Collector<String> out) throws Exception {
            byteArraysState.add(new Byte[size]);

            Runtime runtime = Runtime.getRuntime();

            long max = runtime.maxMemory();
            long free = runtime.freeMemory();
            long used = max - free;
            double percentage = (double) used / max * 100;

            String formattedDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());

            System.out.println(String.format("%s - Heap Usage:%.02f%% (Max:%d Free:%d Used:%d)", formattedDate, percentage, max, free, used));
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ListStateDescriptor<Byte[]> byteArraysDescriptor = new ListStateDescriptor<>(
                    "byteArrays",
                    TypeInformation.of(new TypeHint<Byte[]>(){})
            );
            byteArraysState = getRuntimeContext().getListState(byteArraysDescriptor);
        }
    }
}
