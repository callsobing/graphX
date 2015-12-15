import functions.LongKeyToAnomeKey;
import functions.MakeHapmapldToTuple;
import functions.PageRank;
import functions.WeightedPageRank;
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
        SparkConf sparkConf = new SparkConf().setAppName("How do you turn this off?");
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


        // Get pathogenic ids
        JavaPairRDD<String, String> clinvarPatho = clinvarContext
                .filter(x -> x._2().equals(1.0f))
                .mapToPair(new LongKeyToAnomeKey())
                .mapToPair(x -> new Tuple2<>(x._1(), "clinvarPatho"));

        JavaPairRDD<String, String> lovdPatho = lovdContext
                .filter(x -> x._2().equals(1.0f))
                .mapToPair(new LongKeyToAnomeKey())
                .mapToPair(x -> new Tuple2<>(x._1(), "lovdPatho"));

        JavaPairRDD<String, String> allPatho = clinvarPatho.union(lovdPatho);


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
                // .filter(x-> x._2()._2()._2().equals("YRI")) // using popCode as filter
                .mapToPair(x -> new Tuple2<>(new Tuple2<>(x._1(), x._2()._1()), x._2()._2()._1()))
                .reduceByKey(Math::max) // Get largest ld info as edge properties
                .map(x -> new Edge<>(x._1()._1(), x._1()._2(), x._2())).cache();

        // Build Graph from Edges
        // Run PageRank on anomeGraph, and transform vertices to PairRDD
        // Join vertexRDD with dbsnp longID-KeyID RDD to get original keyID
        PageRank pageRank = new PageRank(anomeEdgeRDD);
        JavaPairRDD<String, Float> pageRankResult = pageRank.runPageRank();
        JavaPairRDD<String, String> pageRankResultFinal = pageRankResult
                .leftOuterJoin(allPatho)
                .mapToPair(x -> {
                    if (x._2()._2().isPresent()) {
                        return new Tuple2<>(x._1(), x._2()._1().toString().concat(" -> ").concat(x._2()._2.get()));
                    } else {
                        return new Tuple2<>(x._1(), x._2()._1().toString());
                    }
                });

        pageRankResultFinal.saveAsHadoopFile("/vartotal/yattmp/anomeGraph/pagerankOutput/", String.class, String.class, TextOutputFormat.class);

        /** The following codes are for testing Pagerank algo on existing dataset */
        WeightedPageRank weightedPageRank = new WeightedPageRank(anomeEdgeRDD,anomeVertexRDD);
        JavaPairRDD<String, Float> weightedPageRankResult = weightedPageRank.runWeightedPageRank();
        JavaPairRDD<String, String> weightedPageRankResultFinal = weightedPageRankResult
                .leftOuterJoin(allPatho)
                .mapToPair(x -> {
                    if (x._2()._2().isPresent()) {
                        return new Tuple2<>(x._1(), x._2()._1().toString().concat(" -> ").concat(x._2()._2.get()));
                    } else {
                        return new Tuple2<>(x._1(), x._2()._1().toString());
                    }
                });


        weightedPageRankResultFinal.saveAsHadoopFile("/vartotal/yattmp/anomeGraph/WeightedPagerankOutput/", String.class, String.class, TextOutputFormat.class);

        ctx.stop();
    }
}