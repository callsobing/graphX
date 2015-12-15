package functions;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by yat on 2015/12/7.
 */
public class CreateDbnsfpgenePathwayContext implements PairFlatMapFunction<String, String, String> {
    @Override
    public Iterable<Tuple2<String, String>> call(String line) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String[] field = line.split("\\t");
        List<Tuple2<String, String>> results = new ArrayList<>();
        Map<String, Object> innerMap = (Map<String, Object>) mapper.readValue(field[1], HashMap.class).get("dbnsfpgene");
        String geneSym = innerMap.getOrDefault("gene_sym", "0").toString();
        String keggID = innerMap.getOrDefault("pathway_kegg_id", "0").toString();

        String[] keggIDList = keggID.split(";");
        for (String keggName : keggIDList) {
            results.add(new Tuple2<>(geneSym, keggName));
        }

        return results;
    }
}
