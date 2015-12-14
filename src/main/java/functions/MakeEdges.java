package functions;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.graphx.Edge;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by yat on 2015/12/7.
 */
public class MakeEdges implements FlatMapFunction<Tuple2<Long, Tuple2<Long, Tuple2<Float, String>>>, Edge<Float>> {

    @Override
    public Iterable<Edge<Float>> call(Tuple2<Long, Tuple2<Long, Tuple2<Float, String>>> line) throws Exception {
        List<Edge<Float>> docs = new ArrayList<>();
        // If dst is pathogenic Variant -> Do not make edge from any point to patho Variants
        // Instead, Reverse the relationship and quad-replicates the edge
            Edge<Float> reverseEdge = new Edge<>(line._2()._1(), line._1(), line._2()._2()._1());
            docs.add(reverseEdge);
            return docs;
    }
}