import functions.CreateDbnsfpgeneInteractContext;
import functions.CreateDbnsfpgenePathwayContext;
import functions.CreatedDbnsfpContext;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Serializable;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * This method is meant to generate essential materials for building graph
 * Two types of feature are list as follows:
 * Vertex:  LongID  Value(score)
 * Edge:    LongID1##LongID2    Value##OtherINFO
 */

public final class GetDatasets implements Serializable {
    public static void main(String[] args) throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("Aegis");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

//        JavaRDD<String> hapmapldRaw = ctx.textFile("/vartotal/db/2015summer/hybrid/0/hapmapld/*.snappy");
//        JavaRDD<String> dbsnpRaw = ctx.textFile("/vartotal/db/2015summer/hybrid/1/dbsnp/*.snappy");
//        JavaRDD<String> clinvarRaw = ctx.textFile("/vartotal/db/2015summer/hybrid/1/clinvar/*.snappy");
//        JavaRDD<String> lovdRaw = ctx.textFile("/vartotal/db/2015summer/hybrid/1/lovd*/*.snappy");
        JavaRDD<String> dbnsfpRaw = ctx.textFile("/vartotal/db/2015summer/hybrid/1/dbnsfp/*.snappy");
        JavaRDD<String> dbnsfpgeneRaw = ctx.textFile("/vartotal/db/2015summer/hybrid/3/1_dbnsfpgenegencode/*.snappy");

        //      Transform hapmapld info "rs01 rs02 0.98" -> (rs01, (rs02, 0.98)) & (rs02,(rs01, 0.98))
        //      Transform dbsnp info -> (rs01, 01_0111_A) (rs02, 01_0999_T)
        //      Transform clinvar info -> (9_09876543_A, 1) (8_37652374_T, -1) where 1:patho,-1:benign
//        JavaPairRDD<String, Tuple2<String, Tuple2<Float,String>>> hapmapldContext = hapmapldRaw.flatMapToPair(new CreateHapmapldContext());
//        JavaPairRDD<String, String> dbsnpContext = dbsnpRaw.mapToPair(new CreateDbsnpContext()).distinct().cache();
//        JavaPairRDD<Long, Float> clinvarContext = clinvarRaw.mapToPair(new CreateClinvarContext()).distinct().cache();
//        JavaPairRDD<Long, String> lovdContext = lovdRaw.mapToPair(new CreateLovdContext()).distinct().cache();
//
        JavaPairRDD<String, Long> dbnsfpContext = dbnsfpRaw.mapToPair(new CreatedDbnsfpContext()).distinct().cache();
        JavaPairRDD<String, String> dbnsfpgenePathContext = dbnsfpgeneRaw.flatMapToPair(new CreateDbnsfpgenePathwayContext()).distinct().cache();
        JavaPairRDD<String, String> dbnsfpgeneInteractContext = dbnsfpgeneRaw.flatMapToPair(new CreateDbnsfpgeneInteractContext()).distinct().cache();

        JavaPairRDD<String, String> pathwayRelationship = dbnsfpContext
                .join(dbnsfpgenePathContext)
                .mapToPair(x -> new Tuple2<>(x._2()._1().toString(), x._2()._2()))
                .mapToPair(Tuple2::swap)
                .reduceByKey((a, b) -> a.concat("##").concat(b))
                .flatMapToPair(line -> {
                    List<Tuple2<String, String>> results = new ArrayList<>();
                    String[] idList = line._2().split("##");
                    for (int current = 0; current < idList.length - 1; current++) {
                        for (int innerCurrent = current; innerCurrent < idList.length; innerCurrent++) {
                            results.add(new Tuple2<>(idList[current].concat("##").concat(idList[innerCurrent]), "0.5"));
                        }
                    }
                    return results;
                });

        JavaPairRDD<String, String> dbnsfpgeneInteractInOne = dbnsfpgeneInteractContext
                .reduceByKey((a, b) -> a.concat("##").concat(b));

        JavaPairRDD<String, String> interactRelationship = dbnsfpContext
                .join(dbnsfpgeneInteractInOne)
                .mapToPair(x -> new Tuple2<>(x._2()._1().toString(), x._2()._2()))
                .flatMapToPair(line -> {
                    List<Tuple2<String, String>> results = new ArrayList<>();
                    String[] geneSyms = line._2().split("##");
                    for (String n : geneSyms) {
                        results.add(new Tuple2<>(n, line._1()));
                    }
                    return results;
                })
                .join(dbnsfpContext)
                .flatMapToPair(line -> {
                    List<Tuple2<String, String>> results = new ArrayList<>();
                    results.add(new Tuple2<>(line._1().concat("##").concat(line._2()._2().toString()), "0.5"));
                    results.add(new Tuple2<>(line._2()._2().toString().concat("##").concat(line._1()), "0.5"));
                    return results;
                });


//        JavaPairRDD<String,String> hapmapldTextOutput = hapmapldContext
//                .join(dbsnpContext)
//                .mapToPair(new TransferRs1ToLongKey())
//                .join(dbsnpContext)
//                .mapToPair(new TransferRs2ToLongKey())
//                .mapToPair(x-> new Tuple2<>(
//                        x._1().toString().concat("##").concat(x._2()._1().toString()),
//                        x._2()._2()._1().toString().concat("##").concat(x._2()._2()._2())));
//
//        JavaPairRDD<Long, String> dbsnpLongIDContext = dbsnpContext.mapToPair(new TransformDbsnpToLongKey());
//
        pathwayRelationship.saveAsHadoopFile("/vartotal/yattmp/anomeGraph/edge/pathway/", String.class, String.class, TextOutputFormat.class);
        interactRelationship.saveAsHadoopFile("/vartotal/yattmp/anomeGraph/edge/ppi/", String.class, String.class, TextOutputFormat.class);
//        hapmapldTextOutput.saveAsHadoopFile("/vartotal/yattmp/anomeGraph/edge/hapmapld/",Long.class,String.class,TextOutputFormat.class);
//        dbsnpLongIDContext.saveAsHadoopFile("/vartotal/yattmp/anomeGraph/vertex/dbsnp/",Long.class,String.class,TextOutputFormat.class);
//        clinvarContext.saveAsHadoopFile("/vartotal/yattmp/anomeGraph/vertex/clinvar",Long.class,Float.class,TextOutputFormat.class);
//        lovdContext.saveAsHadoopFile("/vartotal/yattmp/anomeGraph/vertex/lovd/",Long.class,String.class,TextOutputFormat.class);

        ctx.stop();
    }
}