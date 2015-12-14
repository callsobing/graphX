package functions;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.graphx.Edge;
import org.apache.spark.graphx.Graph;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;
import scala.reflect.ClassTag$;

/**
 * Created by yat on 2015/12/7.
 */
public class PageRank {
    private JavaRDD<Edge<Float>> anomeEdgeRDD;

    public PageRank(JavaRDD<Edge<Float>> anomeEdgeRDD) {
        this.anomeEdgeRDD = anomeEdgeRDD;
    }

    public JavaPairRDD<String, Float> runPageRank() {
        Graph<Integer, Float> anomeGraph = Graph.fromEdges(anomeEdgeRDD.rdd() , 1,
                StorageLevel.MEMORY_AND_DISK(), StorageLevel.MEMORY_AND_DISK(),
                ClassTag$.MODULE$.apply(Integer.class), ClassTag$.MODULE$.apply(Float.class)).cache();

        // Run PageRank on anomeGraph, and transform vertices to PairRDD
        JavaPairRDD<Long, Float> pagerankedVertex = anomeGraph
                .ops().staticPageRank(10,0.15).vertices()
                .toJavaRDD()
                .mapToPair(x-> new Tuple2<>(Long.parseLong(x._1().toString()),Float.parseFloat(x._2().toString())));

        // Join vertexRDD with dbsnp longID-KeyID RDD to get original keyID
        JavaPairRDD<String, Float> mappedVertices = pagerankedVertex
                .mapToPair(x -> new Tuple2<>(x._2(),x._1()))
                .sortByKey()
                .mapToPair(Tuple2::swap)
                .mapToPair(new LongKeyToAnomeKey());

        return mappedVertices;
    }
}
