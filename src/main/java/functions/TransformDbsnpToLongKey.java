package functions;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

/**
 * Created by yat on 2015/12/7.
 */
public class TransformDbsnpToLongKey implements PairFunction<Tuple2<String,String>, Long,String> {
    @Override
    public Tuple2<Long, String> call(Tuple2<String,String> line) throws Exception {
        Long longID = new MakeLongKey().call(line._2())._2();
        return new Tuple2<>(longID,"0");
    }
}
