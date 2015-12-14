package functions;
import com.google.common.base.Optional;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;


/**
 * Created by yat on 2015/12/7.
 */
public class ParseLeftOuterJoinClinvarDbsnp implements PairFunction<Tuple2<Long, Tuple2<String, Optional<Float>>>, Long, Float> {
    public Tuple2<Long, Float> call(Tuple2<Long, Tuple2<String, Optional<Float>>> line) throws Exception {
        Float variantScore = 0f;
        if(line._2()._2().isPresent()){
            variantScore = line._2()._2().get();
        }
        return new Tuple2<>(line._1(),variantScore);
    }
}