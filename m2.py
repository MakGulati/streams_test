# from pyflink.common.serialization import SimpleStringSchema
# from pyflink.datastream import StreamExecutionEnvironment
# from pyflink.datastream.connectors import FlinkKafkaConsumer
# from pyflink.table import EnvironmentSettings
# from pyflink.table import TableEnvironment

# # Set up the execution environment
# env = StreamExecutionEnvironment.get_execution_environment()
# env.set_parallelism(1)

# # Set up a Kafka consumer to read from the data stream
# consumer_properties = {"bootstrap.servers": "localhost:9092", "group.id": "my-group"}
# consumer = FlinkKafkaConsumer("my-topic", SimpleStringSchema(), consumer_properties)

# # Read the data stream from Kafka
# stream = env.add_source(consumer)

# # Convert the data stream to a table
# t_env = TableEnvironment.create(EnvironmentSettings.new_instance().in_streaming_mode())
# t_env.register_table("my_table", stream)

# # Print the results to the console
# t_env.from_path("my_table").to_pandas().head()

# # Execute the job
# env.execute("Kafka Consumer")

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.serialization import SimpleStringSchema

# Create a Stream Execution Environment\n
env = StreamExecutionEnvironment.get_execution_environment()

# Define Kafka Consumer Properties
kafka_props = {"bootstrap.servers": "localhost:9092", "group.id": "test"}

# Read data from Kafka and print it
env.add_source(
    FlinkKafkaConsumer(
        "test",  # Topic to read messages from
        SimpleStringSchema(),  # Deserialize messages in string format
        kafka_props,
    )
).print()

# Execute the environment
env.execute("Consume from Kafka")
