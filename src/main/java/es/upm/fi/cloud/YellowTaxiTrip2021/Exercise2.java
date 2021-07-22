package es.upm.fi.cloud.YellowTaxiTrip2021;


import org.apache.flink.api.common.typeinfo.Types;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;

import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;


import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;

public class Exercise2 {

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
                            fieldArray[1],
                            fieldArray[1],
                            Double.parseDouble(fieldArray[14]),//tolls
                            Double.parseDouble(fieldArray[16]));//total amount
                })
                .returns(Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE, Types.DOUBLE))
                //assign watermarks
                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<String, String, Double, Double>>() {
                    @Override
                    public long extractAscendingTimestamp(Tuple4<String, String, Double, Double> element) {
                        return LocalDateTime.parse(element.f0, formatter).toInstant(ZoneOffset.UTC).toEpochMilli();
                    }
                })
                //8 hour windowsize - timeWindowAll since there are not keyBy
                .timeWindowAll(Time.hours(8))
                //perform operations
                .apply(new Operations2())
                .writeAsCsv(params.get("output"), FileSystem.WriteMode.OVERWRITE);
        env.execute("Exercise2");

    }
    //function for rounding double values to 2 after comma
    private static double roundDouble(double d, int places) {
        BigDecimal bigDecimal = new BigDecimal(Double.toString(d));
        bigDecimal = bigDecimal.setScale(places, RoundingMode.HALF_UP);
        return bigDecimal.doubleValue();
    }

    public static class Operations2 implements AllWindowFunction<Tuple4<String, String, Double, Double>,
            Tuple5<String, String, Long, Double, Double>,TimeWindow>{

        public void apply(TimeWindow timeWindow,
                          Iterable<Tuple4<String, String, Double, Double>> input,
                          Collector<Tuple5<String, String, Long, Double, Double>> out) {
            {

                Iterator<Tuple4<String, String, Double, Double>> iterator = input.iterator();
                Tuple4<String, String, Double, Double> first = iterator.next();

                String pickup_datetime_first = "";
                String pickup_datetime_last = "";
                Long number_trips = 0L;
                Double total_tolls = 0.0;
                Double total = 0.0;

                if (first != null) {
                    pickup_datetime_first = first.f0;
                    pickup_datetime_last = first.f0;
                    number_trips = 1L;
                    total_tolls = first.f2;
                    total = first.f3;
                }

                //save last trip time, accumulated number of trips, tolls amount, total amount
                while (iterator.hasNext()) {
                    Tuple4<String, String, Double, Double> next = iterator.next();
                    pickup_datetime_last = next.f1;
                    number_trips += 1;
                    total_tolls += next.f2;
                    total += next.f3;
                }
                out.collect((new Tuple5<>(pickup_datetime_first, pickup_datetime_last, number_trips, roundDouble(total_tolls,2), roundDouble(total,2))));
            }
        }
    }
}
