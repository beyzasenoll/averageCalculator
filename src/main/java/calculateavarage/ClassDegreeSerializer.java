package calculateavarage;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;


public class ClassDegreeSerializer implements FlatMapFunction<ClassDegree, String> {


    ObjectMapper objectMapper;

    @Override
    public void flatMap(ClassDegree s, Collector<String> collector) throws Exception {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        try {
            String classDegreeString = objectMapper.writeValueAsString(s);
            collector.collect(classDegreeString);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
