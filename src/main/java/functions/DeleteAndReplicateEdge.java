package functions;

import com.google.common.base.Optional;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.graphx.Edge;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yat on 2015/12/11.
 */
public class DeleteAndReplicateEdge implements PairFlatMapFunction<Tuple2<Long,Tuple2<Tuple2<Long,Tuple2<Float,String>>,Optional<Float>>>, Long, Tuple2<Long, Tuple2<Float, String>>> {
    public Iterable<Tuple2<Long, Tuple2<Long, Tuple2<Float, String>>>> call(Tuple2<Long,Tuple2<Tuple2<Long,Tuple2<Float,String>>,Optional<Float>>> line) throws Exception {
        List<Tuple2<Long, Tuple2<Long, Tuple2<Float, String>>>> results = new ArrayList<>();
        if(line._2()._2().isPresent()){
            // Replicate reverse edge when src is pathogenic vertex (Edges will be reversed afterward)
            results.add(new Tuple2<>(line._2()._1()._1(),new Tuple2<>(line._1(),line._2()._1()._2())));
            results.add(new Tuple2<>(line._2()._1()._1(),new Tuple2<>(line._1(),line._2()._1()._2())));
            results.add(new Tuple2<>(line._2()._1()._1(),new Tuple2<>(line._1(),line._2()._1()._2())));
            results.add(new Tuple2<>(line._2()._1()._1(),new Tuple2<>(line._1(),line._2()._1()._2())));
        } else{
            results.add(new Tuple2<>(line._1(),line._2()._1()));
        }
        return results;
    }
}