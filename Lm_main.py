from Lm_consumers import KafkaConsumer  # Ensure this matches the actual class name
import Lm_configs as conf

try:
    # Extract consumer and producer settings
    consumer_settings = conf.config['consumer_settings']
    dest_connection = conf.config['dest_Connections']
    source_connection = conf.config['source_connection']
    Consignment_Final_Schema = conf.config['Consignment_Final_Schema']
    Refrence_Link_Final_Schema = conf.config['Refrence_Link_Final_Schema']
    Payment_Details_Final_Schema = conf.config['Payment_Details_Final_Schema']
    Adress_Details_Final_Schema = conf.config['Adress_Details_Final_Schema']
    Consignment_History_Final_Schema = conf.config['Consignment_History_Final_Schema']
    producer_settings = conf.config['producer_settings']  # Assuming this is also defined in configs
    topic = conf.config['topics']  # Assuming you want the first topic

    # Create an instance of KafkaRouter
    kafkaConsumer = KafkaConsumer(consumer_settings, topic, dest_connection, source_connection, Consignment_Final_Schema, Refrence_Link_Final_Schema, Payment_Details_Final_Schema, Adress_Details_Final_Schema, Consignment_History_Final_Schema)

    try:
        # Start consuming messages and producing them based on Tracking_Number
        kafkaConsumer.run()
    except Exception as e:
        print(f"Error while processing messages: {e}")

except KeyError as e:
    print(f"Missing configuration key: {e}")
except Exception as e:
    print(f"Error initializing Consumer: {e}")
