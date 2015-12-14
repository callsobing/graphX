package functions;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by yat on 2015/12/7.
 */
public class TransferRs2ToLongKey implements PairFunction<Tuple2<String, Tuple2<Tuple2<Long,Tuple2<Float,String>>, String>>, Long, Tuple2<Long,Tuple2<Float,String>>> {
    @Override
    public Tuple2<Long, Tuple2<Long,Tuple2<Float,String>>> call(Tuple2<String, Tuple2<Tuple2<Long,Tuple2<Float,String>>, String>> line) throws Exception {
        String key = line._2()._2();
        Tuple2<String,Long> pair = new MakeLongKey().call(key);
        return new Tuple2<>(line._2()._1()._1(), new Tuple2<>(pair._2(),line._2()._1()._2()));
    }
}
