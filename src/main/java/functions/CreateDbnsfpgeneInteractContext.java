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
public class CreateDbnsfpgeneInteractContext implements PairFlatMapFunction<String, String, String> {
    @Override
    public Iterable<Tuple2<String, String>> call(String line) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        String[] field = line.split("\\t");
        List<Tuple2<String, String>> results = new ArrayList<>();
        Map<String, Object> innerMap = (Map<String, Object>) mapper.readValue(field[1], HashMap.class).get("dbnsfpgene");
        String geneSym = innerMap.getOrDefault("gene_sym", "0").toString();
        String interactionID = innerMap.getOrDefault("interactions_consensuspathdb", "0").toString();

        String[] interactionIDList = interactionID.split(";");
        for (String interactionPair : interactionIDList) {
            String[] interactionPartner = interactionPair.split("\\[");
            results.add(new Tuple2<>(geneSym, interactionPartner[0]));
        }

        return results;
    }
}
