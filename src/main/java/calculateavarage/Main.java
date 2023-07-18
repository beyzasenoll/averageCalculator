/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package calculateavarage;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;



import java.time.Instant;
import java.util.Properties;

public class Main {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// Set up Kafka consumer properties
		Properties consumerProps = new Properties();
		consumerProps.setProperty("bootstrap.servers", "localhost:29092");
		consumerProps.setProperty("group.id", "flink-consumer");

		// Set up Kafka producer properties
		Properties producerProps = new Properties();
		producerProps.setProperty("bootstrap.servers", "localhost:29092");

		System.out.println("set properties");

		// Create the Kafka consumer
		FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
				"students.note",
				new SimpleStringSchema(),
				consumerProps
		);


		// Create the Kafka producer
		FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(
				"note.averages",
				new SimpleStringSchema(),
				producerProps
		);


		System.out.println("set producer and consumer");

		// Configure the consumer and producer
		// kafkaConsumer.setStartFromEarliest();
		producer.setWriteTimestampToKafka(true);



		// Add the Kafka consumer as a data source
		DataStream<String> inputStringStream = env.addSource(kafkaConsumer);
		inputStringStream.print();

		DataStream<Student> inputStudentStream = inputStringStream.flatMap(new StudentDeserializer())
				.assignTimestampsAndWatermarks(WatermarkStrategy.<Student>forMonotonousTimestamps()
						.withTimestampAssigner(
								(event, timestamp) -> Instant.now().toEpochMilli()));
		inputStudentStream.print();
		//inputStudentStream.keyBy(Student::getClassName);
		KeyedStream<Student, String> studentKeyedStream = inputStudentStream.keyBy(student -> student.getClassName());
		WindowedStream<Student,String, TimeWindow> studentKeyedWindowedStream = studentKeyedStream.window(TumblingProcessingTimeWindows.of(Time.minutes(1)));
		DataStream<ClassDegree> calculatedAverages= studentKeyedWindowedStream.process(new ClassDegreeCalculator());
		DataStream<String> calculatedAveragesString = calculatedAverages.flatMap(new ClassDegreeSerializer());
		calculatedAveragesString.print();
		calculatedAveragesString.addSink((producer));


		// Execute the Flink job
		env.execute("CalculateAverageJob");
	}

}
