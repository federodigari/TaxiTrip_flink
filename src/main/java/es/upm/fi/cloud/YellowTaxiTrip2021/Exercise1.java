package es.upm.fi.cloud.YellowTaxiTrip2021;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

public class Exercise1 {

    //formatter to read timestamps as date
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static void main(String[] args) throws Exception {

        final ParameterTool params = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setGlobalJobParameters(params);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        env.readTextFile(params.get("input"))

                //read tuples
                .map(in -> {
                    String[] fieldArray = in.split(",");
                    return new Tuple4<>(
                            Long.parseLong(fieldArray[0]),
                            fieldArray[1],
                            Long.parseLong(fieldArray[3]),
                            Double.parseDouble(fieldArray[16]));
                })
                .returns(Types.TUPLE(Types.LONG, Types.STRING, Types.LONG, Types.DOUBLE))
                //assign watermarks
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<Long, String, Long, Double>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<Long, String, Long, Double> element) {
                        return LocalDateTime.parse(element.f1, formatter).toInstant(ZoneOffset.UTC).toEpochMilli();
                    }
                })
                //keyBy vendorID
                .keyBy(0)
                // 1 hour windowsize - Tumbling window
                .window(TumblingEventTimeWindows.of(Time.hours(1)))
                //perform operations
                .apply(new Operations())
                .writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        env.execute("Exercise1");

    }
    //function for rounding double values to 2 after comma
    private static double roundDouble(double d, int places) {
        BigDecimal bigDecimal = new BigDecimal(Double.toString(d));
        bigDecimal = bigDecimal.setScale(places, RoundingMode.HALF_UP);
        return bigDecimal.doubleValue();
    }

    public static class Operations implements WindowFunction<Tuple4<Long, String, Long, Double>,
            Tuple4<Long, Long, Long, Double>, Tuple, TimeWindow> {

        public void apply(Tuple tuple, TimeWindow timeWindow,
                          Iterable<Tuple4<Long, String, Long, Double>> input,
                          Collector<Tuple4<Long, Long, Long, Double>> out) {
            Iterator<Tuple4<Long, String, Long, Double>> iterator = input.iterator();
            Tuple4<Long, String, Long, Double> first = iterator.next();

            Long VendorID = 0L;
            Long number_trips = 0L;
            Long number_passagers = 0L;
            Double total = 0.0;

            if (first != null) {
                VendorID = first.f0;
                number_trips = 1L;
                number_passagers = first.f2;
                total = first.f3;

            }
            //accumulated number of trips, number of passenger, amount of money
            while (iterator.hasNext()){
                Tuple4<Long, String, Long, Double> next=iterator.next();
                number_trips+=1;
                number_passagers+= next.f2;
                total+= next.f3;
            }
            out.collect(new Tuple4<>(VendorID, number_trips, number_passagers, roundDouble(total,2)));
        }
    }
}
