package functions;

import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


/**
 * Created by yat on 2015/12/8.
 */
public class MakeHapmapldToTuple implements PairFunction<String,Long, Tuple2<Long, Tuple2<Float, String>>> {
    @Override
    public Tuple2<Long, Tuple2<Long, Tuple2<Float, String>>> call(String line) throws Exception {
        String [] innerMap = line.split("\\t");
        String [] longIDs = innerMap[0].split("##");
        String [] latter = innerMap[1].split("##");
        Long long1 = Long.parseLong(longIDs[0]);
        Long long2 = Long.parseLong(longIDs[1]);
        Float score = Float.parseFloat(latter[0]);
        String popCode = latter[1];

        return new Tuple2<>(long1, new Tuple2<>(long2, new Tuple2<>(score,popCode)));
    }
}