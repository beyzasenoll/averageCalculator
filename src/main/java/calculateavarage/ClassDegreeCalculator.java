package calculateavarage;

import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class ClassDegreeCalculator extends ProcessWindowFunction<Student, ClassDegree, String, TimeWindow> {
    @Override
    public void process(String className, ProcessWindowFunction<Student, ClassDegree, String, TimeWindow>.Context context, Iterable<Student> iterable, Collector<ClassDegree> collector) throws Exception {
        ClassDegree classDegree = new ClassDegree();
        classDegree.setClassName(className);
        classDegree.setTimeStamp(System.currentTimeMillis());

        double sum = 0, count = 0, average = 0;
        for (Student student : iterable) {
            sum += student.getNote();
            count++;
        }
        if (count > 0) {
            average = sum / count;
        }
        classDegree.setAvarage(average);
        collector.collect(classDegree);
    }
}
