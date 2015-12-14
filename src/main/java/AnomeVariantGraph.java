import functions.*;

import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.graphx.Edge;
import scala.Tuple2;


// ARGS:
// 1.hapmapld score threshold
// 2.PageRank iteration
// 3.Hapmap population code

public final class AnomeVariantGraph {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("Graph x Graph");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

        JavaRDD<String> hapmapldRaw = ctx.textFile("/vartotal/yattmp/anomeGraph/edge/hapmapld/*.snappy");
        JavaRDD<String> dbsnpRaw = ctx.textFile("/vartotal/yattmp/anomeGraph/vertex/dbsnp/*.snappy");
        JavaRDD<String> clinvarRaw = ctx.textFile("/vartotal/yattmp/anomeGraph/vertex/clinvar/*.snappy");
        JavaRDD<String> lovdRaw = ctx.textFile("/vartotal/yattmp/anomeGraph/vertex/lovd/*.snappy");

        //      Get LongID to KeyID (dbsnp) mapping table
        JavaPairRDD<Long, Float> dbsnpContext = dbsnpRaw
                .mapToPair(x -> new Tuple2<>(Long.parseLong(x.split("\\t")[0]), Float.parseFloat(x.split("\\t")[1]))).cache();

        JavaPairRDD<Long, Float> clinvarContext = clinvarRaw
                .mapToPair(x -> new Tuple2<>(Long.parseLong(x.split("\\t")[0]), Float.parseFloat(x.split("\\t")[1]))).cache();

        JavaPairRDD<Long, Float> lovdContext = lovdRaw
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(Long.parseLong(x.split("\\t")[0]), Float.parseFloat(x.split("\\t")[1].split("##")[0])),x.split("\\t")[1].split("##")[1]))
                .filter(x -> x._2().equals("lovdtsc"))
                .mapToPair(Tuple2::_1).cache();

        //      Loads Variant data and makes RDD of Variant Vertex with property: -1, 0, or 1
        JavaPairRDD<Long, Float> anomeVertexRDD = dbsnpContext
                .union(clinvarContext)
                .union(lovdContext)
                .reduceByKey(Math::max);

        //      Transform hapmapldRaw to Tuple Format
        JavaPairRDD<Long, Tuple2<Long, Tuple2<Float, String>>> hapmapldConverted = hapmapldRaw
                .mapToPair(new MakeHapmapldToTuple());

        //      If user specified population code, filtered by that population, otherwise use all data to build edge.
        JavaRDD<Edge<Float>> anomeEdgeRDD = hapmapldConverted
                // Reverse all edges to avoid pathogenic vertex to serve as dst vertex
                .map(line -> new Edge<>(line._2()._1(), line._1(), line._2()._2()._1())).cache();

        // Build Graph from Edges
        // Run PageRank on anomeGraph, and transform vertices to PairRDD
        // Join vertexRDD with dbsnp longID-KeyID RDD to get original keyID
        PageRank pageRank = new PageRank(anomeEdgeRDD);
        JavaPairRDD<String, Float> pageRankResult = pageRank.runPageRank();

        pageRankResult.saveAsHadoopFile("/vartotal/yattmp/anomeGraph/pagerankOutput/",Long.class,String.class,TextOutputFormat.class);

        /** The following codes are for testing Pagerank algo on existing dataset */
        WeightedPageRank weightedPageRank = new WeightedPageRank(anomeEdgeRDD,anomeVertexRDD);
        JavaPairRDD<String, Float> weightedPageRankResult = weightedPageRank.runWeightedPageRank();
        weightedPageRankResult.saveAsHadoopFile("/vartotal/yattmp/anomeGraph/WeightedPagerankOutput/",Long.class,String.class,TextOutputFormat.class);

        ctx.stop();
    }
}