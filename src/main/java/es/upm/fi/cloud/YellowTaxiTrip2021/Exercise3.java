package es.upm.fi.cloud.YellowTaxiTrip2021;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.streaming.api.windowing.triggers.PurgingTrigger;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

public class Exercise3 {

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
                    return new Tuple3<>(
                            Long.parseLong(fieldArray[0]),
                            Double.parseDouble(fieldArray[4]),
                            stringDatetoSeconds(fieldArray[1], fieldArray[2]));

                })
                .returns(Types.TUPLE(Types.LONG, Types.DOUBLE, Types.LONG))
                //keyBy vendorID
                .keyBy(0)
                //use a GlobalWindows
                .window(GlobalWindows.create())
                //Set a CountTrigger each 10 elements
                .trigger(PurgingTrigger.of(CountTrigger.of(10)))
                //apply operations
                .apply(new Operations3()).returns(Types.TUPLE(Types.LONG, Types.DOUBLE, Types.LONG))
                .writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        env.execute("Exercise3");

    }
    //function for calculating time difference
    private static Long stringDatetoSeconds(String s, String s1) {
        Long startSeconds = LocalDateTime.parse(s, formatter).toEpochSecond(ZoneOffset.UTC);
        Long finishSeconds = LocalDateTime.parse(s1, formatter).toEpochSecond(ZoneOffset.UTC);
        return finishSeconds - startSeconds;
    }

    //function for rounding double values to 2 after comma
    private static double roundDouble(double d, int places) {
        BigDecimal bigDecimal = new BigDecimal(Double.toString(d));
        bigDecimal = bigDecimal.setScale(places, RoundingMode.HALF_UP);
        return bigDecimal.doubleValue();
    }

    public static class Operations3 implements WindowFunction<Tuple3<Long, Double, Long>,
            Tuple3<Long, Double, Long>, Tuple, GlobalWindow> {
        public void apply(Tuple tuple, GlobalWindow globalWindow,
                          Iterable<Tuple3<Long,Double,Long>> input,
                          Collector<Tuple3<Long, Double, Long>> out) {
            Iterator <Tuple3<Long, Double, Long>>iterator= input.iterator();
            Tuple3<Long, Double, Long> first=iterator.next();

            Long VendorID = 0L;
            Double average_distance=0.0;
            Long average_time=0L;

            Long time=0L;
            Double trip_distance=0.0;
            Long number_trips = 0L;

            if (first != null) {
                VendorID = first.f0;
                number_trips = 1L;
                trip_distance=first.f1;
                time=first.f2;
                average_distance=trip_distance/number_trips;
                average_time= (time/number_trips);

            }
            //calculate avg distance, avg time
            while (iterator.hasNext()) {
                Tuple3<Long, Double, Long> next=iterator.next();
                number_trips += 1;
                trip_distance+= next.f1;
                time+=next.f2;
                average_distance=trip_distance/number_trips;
                average_time= (time/number_trips);
            }
            out.collect((new Tuple3<>(VendorID,roundDouble(average_distance,2),average_time)));
        }
    }
}


