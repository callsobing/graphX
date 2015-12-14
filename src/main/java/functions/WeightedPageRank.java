package functions;

import com.google.common.collect.Iterables;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.graphx.Edge;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by yat on 2015/12/7.
 */

public class WeightedPageRank {
    private JavaRDD<Edge<Float>> anomeEdgeRDD;
    private JavaPairRDD<Long, Float> anomeVertexRDD;
    private JavaPairRDD<Long, String> longIDtoKeyID;
    private static class Sum implements Function2<Float,Float,Float> {
        @Override
        public Float call(Float a, Float b) {
            return a + b;
        }
    }

    public WeightedPageRank(JavaRDD<Edge<Float>> anomeEdgeRDD, JavaPairRDD<Long, Float> anomeVertexRDD) {
        this.anomeEdgeRDD = anomeEdgeRDD;
        this.anomeVertexRDD = anomeVertexRDD;
    }

    public JavaPairRDD<String, Float> runWeightedPageRank() {
        JavaPairRDD<Long, Iterable<Tuple2<Long, Float>>> links = anomeEdgeRDD
                //.filter(s -> s.attr() > 0.5)
                .mapToPair(line -> new Tuple2<>(line.srcId(), new Tuple2<>(line.dstId(), line.attr())))
                .groupByKey()
                .cache();

        JavaPairRDD<Long, Float> ranks = anomeVertexRDD;

        for (int current = 0; current < 10; current++) {
            // Calculates contributions to the rank of other vertexes.
            JavaPairRDD<Long, Float> contribs = links.join(ranks).values()
                    .flatMapToPair(s -> {
                        int count = Iterables.size(s._1);
                        List<Tuple2<Long, Float>> results = new ArrayList<>();
                        for(Tuple2<Long, Float> n : s._1()) {
                            results.add(new Tuple2<>(n._1(), s._2()*n._2()/count));
                        }
                        return results;
                    });
            // Re-calculates ranks based on neighbor contributions.
            // New_rank = 0.15 + 0.35*oldRank + 0.5*newContribution
            JavaPairRDD<Long,Float> thisContribs = contribs.reduceByKey(new Sum());
            JavaPairRDD<Long,Float> oldRank = ranks;
            ranks = oldRank.join(thisContribs).mapValues(sum -> 0.15f + sum._1()*0.35f + sum._2()*0.5f);
        }

        JavaPairRDD<String, Float> mappedVertices = ranks
                .mapToPair(x -> new Tuple2<>(x._2(), x._1()))
                .sortByKey()
                .mapToPair(Tuple2::swap)
                .mapToPair(new LongKeyToAnomeKey());

        return mappedVertices;
    }
}
