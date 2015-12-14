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
public class CreateHapmapldContext implements PairFlatMapFunction<String, String, Tuple2<String,Tuple2<Float,String>>> {
    public Iterable<Tuple2<String,Tuple2<String,Tuple2<Float,String>>>> call(String line) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        List<Tuple2<String, Tuple2<String, Tuple2<Float,String>>>> docs = new ArrayList<>();
        String[] field = line.split("\\t");
        Map<String, Object> hapmapldMap =
                (Map<String, Object>) mapper.readValue(field[1], HashMap.class).get("hapmapld");

        String rs1 = hapmapldMap.get("rs1").toString().replace("rs", "");
        String rs2 = hapmapldMap.get("rs2").toString().replace("rs", "");
        List innerList = ((List) hapmapldMap.get("ld_info"));
        Float score = Float.parseFloat(innerList.get(1).toString());
        String popCode = innerList.get(0).toString();

        docs.add(new Tuple2<>(rs1, new Tuple2<>(rs2, new Tuple2<>(score, popCode))));
        docs.add(new Tuple2<>(rs2, new Tuple2<>(rs1, new Tuple2<>(score, popCode))));
        return docs;
    }
}