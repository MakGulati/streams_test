from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaProducer

# Set up the execution environment
env = StreamExecutionEnvironment.get_execution_environment()
# env.add_jars(
#     "flink-sql-connector-kafka-1.17.0.jar"
# )
env.set_parallelism(1)

# Define the data stream
stream = env.from_collection([(1, "Hello"), (2, "World"), (3, "!")])

# Write the data stream to a Kafka topic
producer_properties = {"bootstrap.servers": "localhost:9092"}
stream.add_sink(
    FlinkKafkaProducer("my-topic", SimpleStringSchema(), producer_properties)
)

# Execute the job
env.execute("Kafka Producer")
