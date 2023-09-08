package calculateavarage;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.util.Collector;


public class StudentDeserializer implements FlatMapFunction<String, Student> {


    ObjectMapper objectMapper;

    @Override
    public void flatMap(String value, Collector<Student> out) {
        if (objectMapper == null) {
            objectMapper = new ObjectMapper();
        }
        try {
            Student mappedStudentObject = objectMapper.readValue(value, Student.class);
            out.collect(mappedStudentObject);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }


    }
}
