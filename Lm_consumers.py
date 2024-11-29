import logging
import json
import pandas as pd
import numpy as np
from confluent_kafka import Consumer, KafkaError
from datetime import datetime
import mysql.connector
from contextlib import closing
import Lm_transformations as tc
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class KafkaConsumer:
    def __init__(self, consumer_settings, consumer_topic, dest_settings, source_connection,
                 Consignment_Final_Schema, Refrence_Link_Final_Schema,
                 Payment_Details_Final_Schema, Adress_Details_Final_Schema,
                 Consignment_History_Final_Schema, batch_size=500, max_workers=10):
        self.consumer_settings = consumer_settings
        self.now = datetime.now()
        self.curent_dt = self.now.strftime("%Y-%m-%d %H:%M:%S")
        self.dest_settings = dest_settings
        self.source_settings = source_connection
        self.Consignment_Final_Schema = Consignment_Final_Schema
        self.Refrence_Link_Final_Schema = Refrence_Link_Final_Schema
        self.Payment_Details_Final_Schema = Payment_Details_Final_Schema
        self.Adress_Details_Final_Schema = Adress_Details_Final_Schema
        self.Consignment_History_Final_Schema = Consignment_History_Final_Schema
        self.consumer_topic = consumer_topic
        self.foc_consigments = 'lm_foc_consignments'
        self.consumer_instance = None

        self.transform_consignments = tc.transform_consignments
        self.transform_address_dtails = tc.transform_address_dtails
        self.transform_refrence_link = tc.transform_refrence_link
        self.transform_payment_details = tc.transform_payment_details
        self.Transform_Consignment_History = tc.Transform_Consignment_History
        
        # Batch processing settings
        self.batch_size = batch_size
        self.max_workers = max_workers

    def create_consumer(self):
        self.consumer_instance = Consumer(**self.consumer_settings)

    def subscribe(self):
        if self.consumer_instance is None:
            raise Exception("Consumer instance is not created. Call create_consumer() first.")
        self.consumer_instance.subscribe(self.consumer_topic)

    def consume_messages(self):
        if self.consumer_instance is None:
            raise Exception("Consumer instance is not created. Call create_consumer() first.")
        
        message_batch = []  # Batch to hold consumed messages
        
        try:
            while True:
                start_time = time.time()  # Track time for throughput
                
                msg = self.consumer_instance.poll(0)  # Adjusted polling time
                if msg is None:
                    continue
                if msg.error():
                    self.handle_error(msg.error())
                    continue  # Continue consuming after handling the error
                
                message_value = msg.value().decode('utf-8')
                tracking_number = self.extract_tracking_number(message_value)
                table_name = self.extract_table_name(message_value)

                if tracking_number and table_name:
                    transformed_data = self.transform_data(message_value, table_name)
                    if transformed_data is not None:
                        message_batch.append((table_name, transformed_data, message_value))

                # Process messages in batches
                if len(message_batch) >= self.batch_size:
                    self.process_batch(message_batch)
                    message_batch = []  # Reset batch

                # Measure throughput
                elapsed_time = time.time() - start_time
                throughput = len(message_batch) / elapsed_time if elapsed_time > 0 else 0
                logging.info(f"Current throughput: {throughput:.2f} messages/sec")

        except KeyboardInterrupt:
            logging.info("Consumer interrupted.")
        except Exception as e:
            logging.error(f"An error occurred during message consumption: {e}")
        finally:
            if message_batch:
                self.process_batch(message_batch)  # Process any remaining messages
            self.close_consumer()

    def handle_error(self, error):
        if error.code() == KafkaError._PARTITION_EOF:
            return  # End of partition, nothing to do
        logging.error(f"Consumer error: {error}")

    def process_batch(self, batch):
        """Process the batch of messages using ThreadPoolExecutor."""
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = []
            for table_name, transformed_data, message_value in batch:
                futures.append(executor.submit(self.process_message, table_name, transformed_data, message_value))
            for future in as_completed(futures):
                try:
                    future.result()  # Wait for result and check for exceptions
                except Exception as e:
                    logging.error(f"Error processing batch message: {e}")

    def process_message(self, table_name, transformed_data, message_value):
        """Process a single message."""
        if table_name == 'CONSIGNMENT':
            queries = self.generate_upsert_query(self.foc_consigments, transformed_data, primary_key='id')
            self.execute_upsert_queries(queries, self.dest_settings)
            #self.consignment_custom_columns(self.foc_consigments, transformed_data)

        elif table_name == 'CONSIGNMENT_HISTORY':
            self.history_table_custom_updates(self.foc_consigments,table_name, transformed_data, message_value)
        
        elif table_name == 'REFERENCE_LINK':
            self.booking_refrence_link_updates(self.foc_consigments,table_name, transformed_data, message_value)

        elif table_name == 'PAYMENT_DETAILS':
            self.payments_details_updates(self.foc_consigments,table_name, transformed_data, message_value)

        else:
            pass    

    def close_consumer(self):
        if self.consumer_instance:
            self.consumer_instance.close()
            logging.info("Consumer closed.")

    def extract_tracking_number(self, message_value):
        try:
            data = json.loads(message_value)
            return data['after'].get("Tracking_Number")
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(f"Failed to extract Tracking_Number: {e}")
            return None

    def extract_table_name(self, message_value):
        try:
            data = json.loads(message_value)
            return data['source']['table']
        except (json.JSONDecodeError, KeyError) as e:
            logging.error(f"Failed to extract table: {e}")
            return None
        

    def transform_data(self, message_value, topic):
        try:
            data = json.loads(message_value)
            opType = data['op']
            if opType not in ['c', 'u']:
                logging.warning(f"Unsupported operation type: {opType} for topic: {topic}")
                return None

            df = pd.DataFrame([data['after']])

            transformations = {
                'CONSIGNMENT': (self.transform_consignments, self.Consignment_Final_Schema),
                'REFERENCE_LINK': (self.transform_refrence_link, self.Refrence_Link_Final_Schema),
                'PAYMENT_DETAILS': (self.transform_payment_details, self.Payment_Details_Final_Schema),
                'CONSIGNMENT_HISTORY': (self.Transform_Consignment_History, self.Consignment_History_Final_Schema)
            }

            if topic in transformations:
                transform_func, schema = transformations[topic]
                latest_update_df = transform_func(self,df)
                latest_update_df = latest_update_df[schema]
                logging.info(f"{topic} transformed successfully with operation type: {opType}.")
                return latest_update_df

            logging.warning(f"Unsupported topic: {topic}")
            return None

        except Exception as e:
            logging.error(f"Transformation failed for topic: {topic} with error: {e}")
        return None

    def generate_upsert_query(self, table_name, df, primary_key):
        queries = []
        for _, row in df.iterrows():
                columns = ', '.join(row.index)
                values = ', '.join(f"'{str(value).replace('\'', '\'\'')}'" for value in row.values)
                update_columns = ', '.join(f"{col} = VALUES({col})" for col in row.index if col != primary_key)
                query = f"""
                INSERT INTO {table_name} ({columns})
                VALUES ({values})
                ON DUPLICATE KEY UPDATE {update_columns};"""
                queries.append(query.strip())

        return queries

    def execute_upsert_queries(self, queries, db_config):
        try:
            with closing(mysql.connector.connect(**db_config)) as connection:
                with closing(connection.cursor()) as cursor:
                    for query in queries:
                        cursor.execute(query)
                    connection.commit()
                    logging.info("All upsert queries executed successfully.")

        except mysql.connector.Error as err:
            logging.error(f"SQL Error: {err}")
   

    def consignment_custom_columns(self, foc_consigments_tbl, df) :
         foc_cnno = df.loc[0, 'foc_cnno']
         foc_pickup_customer_code = df.loc[0, 'foc_pickup_customer_code']
         try:
            with closing(mysql.connector.connect(**self.dest_settings)) as connection:
                with closing(connection.cursor()) as cursor:
                   if foc_pickup_customer_code is not None:
                    consignment_sql = f"""update {foc_consigments_tbl} 
                                          set foc_pickup_customer_code = '{foc_pickup_customer_code}' 
                                          where foc_cnno = '{foc_cnno}';"""
                    cursor.execute(consignment_sql)
                    connection.commit()
                    logging.info(f"Pickup Customer Foc: {foc_cnno} Code {foc_pickup_customer_code} Update successfully.")
                   else:
                       logging.info("foc_pickup_customer_code is none.") 

         except mysql.connector.Error as err:
            logging.error(f"SQL Error: {err}") 


    def history_table_custom_updates(self, foc_consigments_tbl, table_name, df, message_value):
        try:
            source_connection = mysql.connector.connect(**self.source_settings)
            source_cursor = source_connection.cursor(dictionary=True)
            dest_connection = mysql.connector.connect(**self.dest_settings)
            dest_cursor = dest_connection.cursor()
            rows_updated = 0
            foc_cnno = df.loc[0 ,'foc_cnno']
            foc_dlvoffice_code = df.loc[0 ,'foc_current_hub_code']
            foc_dlv_office_code = df.loc[0 ,'foc_dlvoffice_code']

            foc_dlvdate_id = df.loc[0 ,'foc_dlvdate_id']
            if foc_dlvdate_id:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvdate_id = '{foc_dlvdate_id}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount

            foc_dlvdate = df.loc[0 ,'foc_dlvdate']
            if foc_dlvdate:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvdate = '{foc_dlvdate}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount


            foc_dlvupdchannel = df.loc[0 ,'foc_dlvupdchannel']
            if foc_dlvupdchannel:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvupdchannel = '{foc_dlvupdchannel}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount


            foc_dlvupdby = df.loc[0 ,'foc_dlvupdby']
            if foc_dlvupdby:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvupdby = '{foc_dlvupdby}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 

  

            foc_dlvreturndatetime = df.loc[0 ,'foc_dlvreturndatetime']
            if foc_dlvreturndatetime:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvreturndatetime = '{foc_dlvreturndatetime}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 


            foc_dlvlpreprationdatetime = df.loc[0 ,'foc_dlvlpreprationdatetime']
            if foc_dlvlpreprationdatetime:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvlpreprationdatetime = '{foc_dlvlpreprationdatetime}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount

            foc_dlvlmfdatetime = df.loc[0 ,'foc_dlvlmfdatetime']
            if foc_dlvlmfdatetime:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvlmfdatetime = '{foc_dlvlmfdatetime}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount


            foc_dlvlmnfstno = df.loc[0 ,'foc_dlvlmnfstno']
            if foc_dlvlmnfstno:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvlmnfstno = '{foc_dlvlmnfstno}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount


            foc_dlvlstatupddatetime = df.loc[0 ,'foc_dlvlstatupddatetime']
            if foc_dlvlstatupddatetime:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvlstatupddatetime = '{foc_dlvlstatupddatetime}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount


            foc_dlvlndelreason = df.loc[0 ,'foc_dlvlndelreason']
            if foc_dlvlndelreason:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvlndelreason = '{foc_dlvlndelreason}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount  

            foc_dlvlndrdatetime = df.loc[0 ,'foc_dlvlndrdatetime']
            if foc_dlvlndrdatetime:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvlndrdatetime = '{foc_dlvlndrdatetime}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 

            foc_dlvlupdchannel = df.loc[0 ,'foc_dlvlupdchannel']
            if foc_dlvlupdchannel:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvlupdchannel = '{foc_dlvlupdchannel}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount  


            foc_dlvtype = df.loc[0 ,'foc_dlvtype']
            if foc_dlvtype:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvtype = '{foc_dlvtype}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount     


            foc_dlvl_bikercode = df.loc[0 ,'foc_dlvl_bikercode']
            if foc_dlvl_bikercode:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvl_bikercode = '{foc_dlvl_bikercode}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 


            foc_dlvlbkraccdatetime = df.loc[0 ,'foc_dlvlbkraccdatetime']
            if foc_dlvlbkraccdatetime:
                update_query = f"""update {foc_consigments_tbl} 
                                  set foc_dlvlbkraccdatetime = '{foc_dlvlbkraccdatetime}' 
                                  where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 

            foc_dlvlfbdmsavedatetime = df.loc[0 ,'foc_dlvlfbdmsavedatetime']
            if foc_dlvlfbdmsavedatetime:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvlfbdmsavedatetime = '{foc_dlvlfbdmsavedatetime}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 


            foc_dlvlbkrrejdatetime = df.loc[0 ,'foc_dlvlbkrrejdatetime']
            if foc_dlvlbkrrejdatetime:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvlbkrrejdatetime = '{foc_dlvlbkrrejdatetime}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount    


            foc_setrtodatel = df.loc[0 ,'foc_setrtodatel']
            if foc_setrtodatel:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_setrtodatel = '{foc_setrtodatel}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount  


            #Delivery Type---------------------------------------------------------------------------------------
            foc_dlvtype = df.loc[0 ,'foc_dlvtype']  
            if  foc_dlvtype:
                uc = 0
                select_sql = f"""select foc_dlv1type from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_dlv1type is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv1type =  '{foc_dlvtype}' ,
                                       foc_dlvltype = '{foc_dlvtype}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount 
                    rows_updated += uc

                if uc < 1:
                    elect_sql = f"""select foc_dlv2type from {foc_consigments_tbl} 
                                    where foc_cnno = '{foc_cnno}' 
                                    and foc_dlv1type is not null 
                                    and foc_dlv2type is null;""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlv2type =  '{foc_dlvtype}' , 
                                           foc_dlvltype = '{foc_dlvtype}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_dlv3type from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_dlv2type is not null 
                                     and foc_dlv3type is null;""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlv3type =  '{foc_dlvtype}' , 
                                           foc_dlvltype = '{foc_dlvtype}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc

                if uc == 0:
                     update_query = f"""update {foc_consigments_tbl} 
                                        set foc_dlvltype = '{foc_dlvtype}' 
                                        where foc_cnno = '{foc_cnno}';"""
                     dest_cursor.execute(update_query)
                     uc = dest_cursor.rowcount
                     rows_updated += uc     


            foc_biker_code = df.loc[0 ,'foc_biker_code']
            if foc_biker_code:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_biker_code = '{foc_biker_code}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount


            foc_attempt_no = df.loc[0 ,'foc_attempt_no']
            if foc_attempt_no:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_attempt_no = '{foc_attempt_no}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount   

            # Biker Code Update----------------------------------------------------------------------------------
            foc_biker_code = df.loc[0 ,'foc_biker_code']  
            if  foc_biker_code:
                uc = 0
                select_sql = f"""select foc_dlv1_bikercode from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_dlv1_bikercode is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv1_bikercode =  '{foc_biker_code}' , 
                                       foc_dlvl_bikercode = '{foc_biker_code}'
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc 

                if uc < 1:
                    select_sql = f"""select foc_dlv2_bikercode from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_dlv1_bikercode is not null 
                                     and foc_dlv2_bikercode is null 
                                     and  foc_dlv1_bikercode != '{foc_biker_code}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlv2_bikercode =  '{foc_biker_code}' , 
                                           foc_dlvl_bikercode = '{foc_biker_code}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_dlv3_bikercode from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_dlv2_bikercode is not null 
                                     and foc_dlv3_bikercode is null 
                                     and  foc_dlv2_bikercode != '{foc_biker_code}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlv3_bikercode =  '{foc_biker_code}' , 
                                           foc_dlvl_bikercode = '{foc_biker_code}'
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc
                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlvl_bikercode = '{foc_biker_code}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc 


            foc_dmcinscan = df.loc[0 ,'foc_dmcinscan']
            if foc_dmcinscan:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dmcinscan = '{foc_dmcinscan}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount  



            foc_rtortrb_date = df.loc[0 ,'foc_rtortrb_date']  
            if  foc_rtortrb_date:
                select_sql = f"""select foc_rtortrb_date from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_rtortrb_date is null ;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_rtortrb_date =  '{foc_rtortrb_date}' , 
                                       foc_rtortrb_datel = '{foc_rtortrb_date}'
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount  


            foc_reattemptinit_date = df.loc[0 ,'foc_reattemptinit_date']  
            if  foc_reattemptinit_date:
                select_sql = f"""select foc_reattemptinit_date from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_reattemptinit_date is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_reattemptinit_date =  '{foc_reattemptinit_date}' ,
                                       foc_reattemptinit_datel = '{foc_reattemptinit_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount 


            foc_frdesc = df.loc[0 ,'foc_frdesc']  
            if  foc_frdesc:
                select_sql = f"""select foc_frdesc from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_frdesc is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_frdesc =  '{foc_frdesc}' , 
                                       foc_frdescl = '{foc_frdesc}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount 

            #Update foc_frdesc
            foc_frdesc = df.loc[0 ,'foc_frdesc']  
            if  foc_frdesc:
                uc = 0
                select_sql = f"""select foc_frdesc1 from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_frdesc1 is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_frdesc1 =  '{foc_frdesc}' , 
                                       foc_frdescl = '{foc_frdesc}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated +=  uc

                if uc < 1:
                    select_sql = f"""select foc_frdesc2 from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_frdesc1 is not null 
                                     and foc_frdesc2 is null;""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_frdesc2 =  '{foc_frdesc}' , 
                                           foc_frdescl = '{foc_frdesc}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc
                if uc < 1:
                        select_sql = f"""select foc_frdesc3 from {foc_consigments_tbl} 
                                         where foc_cnno = '{foc_cnno}' 
                                         and foc_frdesc2 is not null 
                                         and foc_frdesc3 is null;""" 
                        dest_cursor.execute(select_sql)
                        row_value = dest_cursor.fetchall()

                        if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_frdesc3 =  '{foc_frdesc}' , 
                                               foc_frdescl = '{foc_frdesc}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc
                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_frdescl = '{foc_frdesc}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc         



            foc_dlvbmfno = df.loc[0 ,'foc_dlvbmfno']
            if foc_dlvbmfno:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_dlvbmfno = '{foc_dlvbmfno}' 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 


            #Update foc_rtorerb_date
            foc_rtorerb_date = df.loc[0 ,'foc_rtorerb_date']  
            if  foc_rtorerb_date:
                uc = 0
                select_sql = f"""select foc_rtorerb_date_first from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_rtorerb_date_first is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_rtorerb_date_first =  '{foc_rtorerb_date}' , 
                                       foc_rtorerb_date = '{foc_rtorerb_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_rtorerb_date_second from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_rtorerb_date_first is not null 
                                     and foc_rtorerb_date_second is null
                                     and foc_rtorerb_date_first != '{foc_rtorerb_date}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_rtorerb_date_second =  '{foc_rtorerb_date}' ,
                                           foc_rtorerb_date = '{foc_rtorerb_date}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_rtorerb_date_third from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_rtorerb_date_second is not null
                                     and foc_rtorerb_date_third is null 
                                     and foc_rtorerb_date_second != '{foc_rtorerb_date}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_rtorerb_date_third =  '{foc_rtorerb_date}' , 
                                               foc_rtorerb_date = '{foc_rtorerb_date}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc
                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set  foc_rtorerb_date = '{foc_rtorerb_date}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc        


            foc_rtorerb_attempt_count = df.loc[0 ,'foc_rtorerb_attempt_count']
            if foc_rtorerb_attempt_count:
                update_query = f"""update {foc_consigments_tbl} 
                                   set foc_rtorerb_attempt_count =  foc_rtorerb_attempt_count + {foc_rtorerb_attempt_count} 
                                   where foc_cnno = '{foc_cnno}';"""
                dest_cursor.execute(update_query)
                rows_updated += dest_cursor.rowcount 

            #foc_dlv_bkraccdatetime update ---------------------------------------------------------------------
            foc_dlv_bkraccdatetime = df.loc[0 ,'foc_dlv_bkraccdatetime']  
            if  foc_dlv_bkraccdatetime:
                uc = 0
                select_sql = f"""select foc_dlv1bkraccdatetime from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_dlv1bkraccdatetime is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv1bkraccdatetime =  '{foc_dlv_bkraccdatetime}' , 
                                       foc_dlvlbkraccdatetime = '{foc_dlv_bkraccdatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount 
                    rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_dlv2bkraccdatetime from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_dlv1bkraccdatetime is not null 
                                     and foc_dlv2bkraccdatetime is null 
                                     and foc_dlv1bkraccdatetime != '{foc_dlv_bkraccdatetime}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlv2bkraccdatetime =  '{foc_dlv_bkraccdatetime}' , 
                                           foc_dlvlbkraccdatetime = '{foc_dlv_bkraccdatetime}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_dlv3bkraccdatetime from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_dlv2bkraccdatetime is not null 
                                     and foc_dlv3bkraccdatetime is null 
                                     and foc_dlv2bkraccdatetime != '{foc_dlv_bkraccdatetime}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlv3bkraccdatetime =  '{foc_dlv_bkraccdatetime}' , 
                                           foc_dlvlbkraccdatetime = '{foc_dlv_bkraccdatetime}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc

                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlvlbkraccdatetime = '{foc_dlv_bkraccdatetime}' 
                                           where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc 


            foc_pre_bkbrpmfdatetime = df.loc[0 ,'foc_pre_bkbrpmfdatetime']  
            if  foc_pre_bkbrpmfdatetime:
                select_sql = f"""select foc_pre_bkbrpmfdatetime from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_booking_date > '{foc_pre_bkbrpmfdatetime}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pre_bkbrpmfdatetime =  '{foc_pre_bkbrpmfdatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount    


            foc_pre_bkbrpmf_office_code = df.loc[0 ,'foc_pre_bkbrpmf_office_code']  
            if  foc_pre_bkbrpmf_office_code:
                select_sql = f"""select foc_pre_bkbrpmfdatetime from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_booking_date > '{foc_pre_bkbrpmfdatetime}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_pre_bkbrpmf_office_code =  '{foc_pre_bkbrpmf_office_code}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount

            # Update foc_dlv_fbdmsavedatetime
            foc_dlv_fbdmsavedatetime = df.loc[0 ,'foc_dlv_fbdmsavedatetime']  
            if  foc_dlv_fbdmsavedatetime:
                uc = 0
                select_sql = f"""select foc_dlv1fbdmsavedatetime from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_dlv1fbdmsavedatetime is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv1fbdmsavedatetime =  '{foc_dlv_fbdmsavedatetime}' , 
                                       foc_dlvlfbdmsavedatetime = '{foc_dlv_fbdmsavedatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_dlv2fbdmsavedatetime from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_dlv1fbdmsavedatetime is not null
                                     and foc_dlv2fbdmsavedatetime is null
                                     and foc_dlv1fbdmsavedatetime != '{foc_dlv_fbdmsavedatetime}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlv2fbdmsavedatetime =  '{foc_dlv_fbdmsavedatetime}' , 
                                           foc_dlvlfbdmsavedatetime = '{foc_dlv_fbdmsavedatetime}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_dlv3fbdmsavedatetime from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_dlv2fbdmsavedatetime is not null 
                                     and foc_dlv3fbdmsavedatetime is null
                                     and foc_dlv2fbdmsavedatetime != '{foc_dlv_fbdmsavedatetime}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlv3fbdmsavedatetime =  '{foc_dlv_fbdmsavedatetime}' , 
                                           foc_dlvlfbdmsavedatetime = '{foc_dlv_fbdmsavedatetime}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc

                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                           foc_dlvlfbdmsavedatetime = '{foc_dlv_fbdmsavedatetime}' 
                                           where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc        


            # Update foc_dlv_bkrrejdatetime -------------------------------------------------------------------
            foc_dlv_bkrrejdatetime = df.loc[0 ,'foc_dlv_bkrrejdatetime']  
            if  foc_dlv_bkrrejdatetime:
                uc = 0
                select_sql = f"""select foc_dlv1bkrrejdatetime from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_dlv1bkrrejdatetime is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv1bkrrejdatetime =  '{foc_dlv_bkrrejdatetime}' , 
                                       foc_dlvlbkrrejdatetime = '{foc_dlv_bkrrejdatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc 

                if uc < 1:
                        select_sql = f"""select foc_dlv2bkrrejdatetime from {foc_consigments_tbl} 
                                         where foc_cnno = '{foc_cnno}' 
                                         and foc_d1v1bkrrejdatetime is not null 
                                         and foc_dlv2bkrrejdatetime is null
                                         and foc_dlv1bkrrejdatetime != '{foc_dlv_bkrrejdatetime}';""" 
                        dest_cursor.execute(select_sql)
                        row_value = dest_cursor.fetchall()

                        if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlv2bkrrejdatetime =  '{foc_dlv_bkrrejdatetime}' , 
                                               foc_dlvlbkrrejdatetime = '{foc_dlv_bkrrejdatetime}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_dlv3bkrrejdatetime from {foc_consigments_tbl}
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_dlv2bkrrejdatetime is not null 
                                     and foc_dlv3bkrrejdatetime is null
                                     and foc_dlv2bkrrejdatetime != '{foc_dlv_bkrrejdatetime}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlv3bkrrejdatetime =  '{foc_dlv_bkrrejdatetime}' , 
                                           foc_dlvlbkrrejdatetime = '{foc_dlv_bkrrejdatetime}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc

                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlvlbkrrejdatetime = '{foc_dlv_bkrrejdatetime}' 
                                           where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc    

            #Update foc_setrtodate
            foc_setrtodate = df.loc[0 ,'foc_setrtodate']  
            if  foc_setrtodate:
                uc = 0
                select_sql = f"""select foc_setrtodate1 from {foc_consigments_tbl} 
                                where foc_cnno = '{foc_cnno}' 
                                and foc_setrtodate1 is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_setrtodate1 =  '{foc_setrtodate}' , 
                                       foc_setrtodatel = '{foc_setrtodate}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_setrtodate2 from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_setrtodate1 is not null
                                     and foc_setrtodate2 is null
                                     and foc_setrtodate1 != '{foc_setrtodate}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_setrtodate2 =  '{foc_setrtodate}' , 
                                               foc_setrtodatel = '{foc_setrtodate}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc 

                if uc < 1:
                    select_sql = f"""select foc_setrtodate3 from {foc_consigments_tbl}
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_setrtodate2 is not null
                                     and foc_setrtodate3 is null 
                                     and foc_setrtodate2 != '{foc_setrtodate}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_setrtodate3 =  '{foc_setrtodate}' ,
                                               foc_setrtodatel = '{foc_setrtodate}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc

                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                               foc_setrtodatel = '{foc_setrtodate}' 
                                               where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc


            if  foc_dlv_office_code:
                select_sql = f"""select foc_destination_branch_code from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlv_office_code}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlvoffice_code =  '{foc_dlv_office_code}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount 


            foc_dlvdestinscandatetime = df.loc[0 ,'foc_dlvdestinscandatetime']  
            if  foc_dlvdestinscandatetime:
                select_sql = f"""select foc_dlvdestinscandatetime from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlvdestinscandatetime =  '{foc_dlvdestinscandatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount        


            foc_dlvoffice_cdindatetime = df.loc[0 ,'foc_dlvoffice_cdindatetime']  
            if  foc_dlvoffice_cdindatetime:
                select_sql = f"""select 1 from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlvoffice_cdindatetime =  '{foc_dlvoffice_cdindatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount 


            foc_dlvoffice_bagindatetime = df.loc[0 ,'foc_dlvoffice_bagindatetime']  
            if  foc_dlvoffice_bagindatetime:
                select_sql = f"""select 1 from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl}
                                       set foc_dlvoffice_bagindatetime =  '{foc_dlvoffice_bagindatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount   


            foc_dlvoffice_pkttindatetime = df.loc[0 ,'foc_dlvoffice_pkttindatetime']  
            if  foc_dlvoffice_pkttindatetime:
                select_sql = f"""select 1 from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlvoffice_pkttindatetime =  '{foc_dlvoffice_pkttindatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount   


            foc_dlvcnindatetime = df.loc[0 ,'foc_dlvcnindatetime']  
            if  foc_dlvcnindatetime:
                select_sql = f"""select 1 from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlvcnindatetime =  '{foc_dlvcnindatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount  

            #Update foc_dlv_mfdatetime---------------------------------------------------------------------
            foc_dlv_mfdatetime = df.loc[0 ,'foc_dlv_mfdatetime']  
            if  foc_dlv_mfdatetime:
                uc = 0
                select_sql = f"""select foc_dlv1mfdatetime from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                 and foc_dlv1mfdatetime is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv1mfdatetime =  '{foc_dlv_mfdatetime}',
                                        foc_dlvlmfdatetime = '{foc_dlv_mfdatetime}' 
                                        where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_dlv2mfdatetime from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_dlv1mfdatetime is not null 
                                     and foc_dlv2mfdatetime is null 
                                     and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                     and foc_dlv1mfdatetime != '{foc_dlv_mfdatetime}';""" 
                   
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlv2mfdatetime =  '{foc_dlv_mfdatetime}', 
                                               foc_dlvlmfdatetime = '{foc_dlv_mfdatetime}'
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc
                if uc < 1:
                    select_sql = f"""select foc_dlv3mfdatetime from {foc_consigments_tbl}
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_dlv2mfdatetime is not null
                                     and foc_dlv3mfdatetime is null
                                     and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                     and foc_dlv3mfdatetime != '{foc_dlv_mfdatetime}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlv3mfdatetime =  '{foc_dlv_mfdatetime}', 
                                               foc_dlvlmfdatetime = '{foc_dlv_mfdatetime}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc

                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlvlmfdatetime = '{foc_dlv_mfdatetime}' 
                                               where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc            
        
            #foc_dlv_mnfstno update
            foc_dlv_mnfstno = df.loc[0 ,'foc_dlv_mnfstno']  
            if  foc_dlv_mnfstno:
                uc = 0
                select_sql = f"""select foc_dlv1mnfstno from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                 and foc_dlv1mnfstno is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv1mnfstno =  '{foc_dlv_mnfstno}', 
                                       foc_dlvlmnfstno = '{foc_dlv_mnfstno}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc  

                if uc < 1:
                    select_sql = f"""select foc_dlv2mnfstno from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_dlv1mnfstno is not null
                                     and foc_dlv2mnfstno is null
                                     and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                     and foc_dlv1mnfstno is not null 
                                     and foc_dlv1mnfstno != '{foc_dlv_mnfstno}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlv2mnfstno =  '{foc_dlv_mnfstno}', 
                                               foc_dlvlmnfstno = '{foc_dlv_mnfstno}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc

                if uc < 1:
                        select_sql = f"""select foc_dlv3mnfstno from {foc_consigments_tbl} 
                                         where foc_cnno = '{foc_cnno}' 
                                         and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                         and foc_dlv2mnfstno is not null 
                                         and foc_dlv3mnfstno is null
                                         and foc_dlv2mnfstno != '{foc_dlv_mnfstno}';""" 
                        dest_cursor.execute(select_sql)
                        row_value = dest_cursor.fetchall()

                        if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlv3mnfstno =  '{foc_dlv_mnfstno}', 
                                               foc_dlvlmnfstno = '{foc_dlv_mnfstno}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc

                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlvlmnfstno = '{foc_dlv_mnfstno}' 
                                               where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc            


            foc_dlv1statupddatetime = df.loc[0 ,'foc_dlv1statupddatetime']  
            if  foc_dlv1statupddatetime:
                select_sql = f"""select 1 from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv1statupddatetime =  '{foc_dlv1statupddatetime}', 
                                       foc_dlvlstatupddatetime = '{foc_dlv1statupddatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount
                    
            foc_dlv2statupddatetime = df.loc[0 ,'foc_dlv2statupddatetime']  
            if  foc_dlv2statupddatetime:
                select_sql = f"""select 1 from {foc_consigments_tbl} 
                                where foc_cnno = '{foc_cnno}' 
                                and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                and foc_dlv1statupddatetime != '{foc_dlv2statupddatetime}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv2statupddatetime =  '{foc_dlv2statupddatetime}', 
                                       foc_dlvlstatupddatetime = '{foc_dlv2statupddatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount

            foc_dlv3statupddatetime = df.loc[0 ,'foc_dlv3statupddatetime']  
            if  foc_dlv3statupddatetime:
                select_sql = f"""select 1 from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                 and foc_dlv2statupddatetime != '{foc_dlv3statupddatetime}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv3statupddatetime =  '{foc_dlv3statupddatetime}', 
                                       foc_dlvlstatupddatetime = '{foc_dlv3statupddatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount                  

            #Update foc_dlv_ndelreason ---------------------------------------------------------------------------
            foc_dlv_ndelreason = df.loc[0 ,'foc_dlv_ndelreason']  
            if  foc_dlv_ndelreason:
                uc = 0
                select_sql = f"""select foc_dlv1ndelreason from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                 and foc_dlv1ndelreason is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv1ndelreason =  '{foc_dlv_ndelreason}', 
                                       foc_dlvlndelreason = '{foc_dlv_ndelreason}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc 

                if uc < 1:
                        select_sql = f"""select foc_dlv2ndelreason from {foc_consigments_tbl} 
                                         where foc_cnno = '{foc_cnno}' 
                                         and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                         and foc_dlv1ndelreason is not null 
                                         and foc_dlv2ndelreason is null
                                         and foc_dlv1ndelreason != '{foc_dlv_ndelreason}';""" 
                        dest_cursor.execute(select_sql)
                        row_value = dest_cursor.fetchall()

                        if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlv2ndelreason =  '{foc_dlv_ndelreason}', 
                                               foc_dlvlndelreason = '{foc_dlv_ndelreason}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_dlv3ndelreason from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                     and foc_dlv2ndelreason is not null
                                     and foc_dlv3ndelreason is null 
                                     and foc_dlv2ndelreason != '{foc_dlv_ndelreason}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlv3ndelreason =  '{foc_dlv_ndelreason}', 
                                           foc_dlvlndelreason = '{foc_dlv_ndelreason}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc

            #Update foc_dlv_updchannel
            foc_dlv_updchannel = df.loc[0 ,'foc_dlv_updchannel']  
            if  foc_dlv_updchannel:
                uc = 0
                select_sql = f"""select foc_dlv1updchannel from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                 and foc_dlv1updchannel is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv1updchannel =  '{foc_dlv_updchannel}', 
                                       foc_dlvlupdchannel = '{foc_dlv_updchannel}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_dlv2updchannel from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                     and foc_dlv1updchannel is not null 
                                     and foc_dlv2updchannel is null
                                     and foc_dlv1updchannel != '{foc_dlv_updchannel}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlv2updchannel =  '{foc_dlv_updchannel}', 
                                               foc_dlvlupdchannel = '{foc_dlv_updchannel}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc
                if uc < 1:
                        select_sql = f"""select foc_dlv3updchannel from {foc_consigments_tbl} 
                                        where foc_cnno = '{foc_cnno}' 
                                        and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                        and foc_dlv2updchannel is not null 
                                        and foc_dlv3updchannel is null
                                        and foc_dlv2updchannel != '{foc_dlv_updchannel}';""" 
                        dest_cursor.execute(select_sql)
                        row_value = dest_cursor.fetchall()

                        if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlv3updchannel =  '{foc_dlv_updchannel}', 
                                               foc_dlvlupdchannel = '{foc_dlv_updchannel}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc

                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlvlupdchannel = '{foc_dlv_updchannel}' 
                                               where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc            

            #Update foc_dlvupdby
            foc_dlvupdby = df.loc[0 ,'foc_dlvupdby']
            print(foc_dlvoffice_code)  
            if foc_dlvupdby:
                uc = 0
                select_sql = f"""select foc_dlv1updby from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                 and foc_dlv1updby is null;"""
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()
                print(select_sql)
                print(row_value)

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv1updby =  '{foc_dlvupdby}', 
                                       foc_dlvlupdby = '{foc_dlvupdby}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_dlv2updby from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                     and foc_dlv1updby is not null 
                                     and foc_dlv2updby is null 
                                     and foc_dlv1updby != '{foc_dlvupdby}';"""
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlv2updby =  '{foc_dlvupdby}', 
                                           foc_dlvlupdby = '{foc_dlvupdby}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc

                if uc < 1:
                    select_sql = f"""select foc_dlv3updby from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                     and foc_dlv2updby is not null 
                                     and foc_dlv3updby is null 
                                     and foc_dlv2updby != '{foc_dlvupdby}';"""
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                        update_query = f"""update {foc_consigments_tbl} 
                                           set foc_dlv3updby =  '{foc_dlvupdby}', 
                                           foc_dlvlupdby = '{foc_dlvupdby}' 
                                           where foc_cnno = '{foc_cnno}';"""
                        dest_cursor.execute(update_query)
                        uc = dest_cursor.rowcount
                        rows_updated += uc

                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlvlupdby = '{foc_dlvupdby}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc



            #Update foc_dlv_preprationdatetime
            foc_dlv_preprationdatetime = df.loc[0 ,'foc_dlv_preprationdatetime']  
            if  foc_dlv_preprationdatetime:
                uc = 0
                select_sql = f"""select foc_dlv1preprationdatetime from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                 and foc_dlv1preprationdatetime is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlv1preprationdatetime =  '{foc_dlv_preprationdatetime}', 
                                       foc_dlvlpreprationdatetime = '{foc_dlv_preprationdatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount 
                    rows_updated += uc 

                if uc < 1:
                        select_sql = f"""select foc_dlv2preprationdatetime from {foc_consigments_tbl} 
                                         where foc_cnno = '{foc_cnno}' 
                                         and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                         and foc_dlv1preprationdatetime is not null 
                                         and foc_dlv2preprationdatetime is null
                                         and foc_dlv1preprationdatetime != '{foc_dlv_preprationdatetime}';""" 
                        dest_cursor.execute(select_sql)
                        row_value = dest_cursor.fetchall()

                        if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlv2preprationdatetime =  '{foc_dlv_preprationdatetime}', 
                                               foc_dlvlpreprationdatetime = '{foc_dlv_preprationdatetime}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc
                if uc < 1:
                    select_sql = f"""select foc_dlv3preprationdatetime from {foc_consigments_tbl} 
                                     where foc_cnno = '{foc_cnno}' 
                                     and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                     and foc_dlv2preprationdatetime is not null
                                     and foc_dlv3preprationdatetime is null 
                                     and foc_dlv2preprationdatetime != '{foc_dlv_preprationdatetime}';""" 
                    dest_cursor.execute(select_sql)
                    row_value = dest_cursor.fetchall()

                    if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlv3preprationdatetime =  '{foc_dlv_preprationdatetime}', 
                                               foc_dlvlpreprationdatetime = '{foc_dlv_preprationdatetime}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc 

                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                               set foc_dlvlpreprationdatetime = '{foc_dlv_preprationdatetime}' 
                                               where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc                     


            foc_dlvoffice_airscandatetime = df.loc[0 ,'foc_dlvoffice_airscandatetime']  
            if  foc_dlvoffice_airscandatetime:
                select_sql = f"""select 1 from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlvoffice_airscandatetime =  '{foc_dlvoffice_airscandatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount 



            foc_dlvoffice_first_transport_name = df.loc[0 ,'foc_dlvoffice_first_transport_name']  
            if  foc_dlvoffice_first_transport_name:
                select_sql = f"""select 1 from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                 and foc_dlvoffice_first_transport_name is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlvoffice_first_transport_name =  '{foc_dlvoffice_first_transport_name}', 
                                       foc_dlvoffice_latest_transport_name = '{foc_dlvoffice_first_transport_name}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount 


            foc_dlvoffice_latest_transport_name = df.loc[0 ,'foc_dlvoffice_latest_transport_name']  
            if  foc_dlvoffice_latest_transport_name:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlvoffice_latest_transport_name = '{foc_dlvoffice_latest_transport_name}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount



            foc_dlvoffice_airscandatetime = df.loc[0 ,'foc_dlvoffice_airscandatetime']  
            if  foc_dlvoffice_airscandatetime:
                select_sql = f"""select 1 from {foc_consigments_tbl} 
                                 where foc_cnno = '{foc_cnno}' 
                                 and foc_destination_branch_code = '{foc_dlvoffice_code}';""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_dlvoffice_airscandatetime =  '{foc_dlvoffice_airscandatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    rows_updated += dest_cursor.rowcount   

            #Update foc_d1v_ndrdatetime
            foc_dlv_ndrdatetime = df.loc[0 ,'foc_dlv_ndrdatetime']  
            if  foc_dlv_ndrdatetime:
                uc = 0
                select_sql = f"""select foc_d1v1ndrdatetime from {foc_consigments_tbl}
                                  where foc_cnno = '{foc_cnno}' 
                                  and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                  and foc_d1v1ndrdatetime is null;""" 
                dest_cursor.execute(select_sql)
                row_value = dest_cursor.fetchall()

                if row_value:
                    update_query = f"""update {foc_consigments_tbl} 
                                       set foc_d1v1ndrdatetime =  '{foc_dlv_ndrdatetime}', 
                                       foc_d1vlndrdatetime = '{foc_dlv_ndrdatetime}' 
                                       where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount 
                    rows_updated += uc

                if uc < 1:
                        select_sql = f"""select foc_d1v2ndrdatetime from {foc_consigments_tbl} 
                                         where foc_cnno = '{foc_cnno}' 
                                         and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                         and foc_d1v1ndrdatetime is not null 
                                         and foc_d1v2ndrdatetime is null;""" 
                        dest_cursor.execute(select_sql)
                        row_value = dest_cursor.fetchall()

                        if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_d1v2ndrdatetime =  '{foc_dlv_ndrdatetime}', 
                                               foc_d1vlndrdatetime = '{foc_dlv_ndrdatetime}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc

                if uc < 1:
                        select_sql = f"""select foc_d1v3ndrdatetime from {foc_consigments_tbl} 
                                         where foc_cnno = '{foc_cnno}' 
                                         and foc_destination_branch_code = '{foc_dlvoffice_code}' 
                                         and foc_d1v2ndrdatetime is not null 
                                         and foc_d1v3ndrdatetime is null;""" 
                        dest_cursor.execute(select_sql)
                        row_value = dest_cursor.fetchall()

                        if row_value:
                            update_query = f"""update {foc_consigments_tbl} 
                                               set foc_d1v3ndrdatetime =  '{foc_dlv_ndrdatetime}', 
                                               foc_d1vlndrdatetime = '{foc_dlv_ndrdatetime}' 
                                               where foc_cnno = '{foc_cnno}';"""
                            dest_cursor.execute(update_query)
                            uc = dest_cursor.rowcount
                            rows_updated += uc

                if uc == 0:
                    update_query = f"""update {foc_consigments_tbl} 
                                               set foc_d1vlndrdatetime = '{foc_dlv_ndrdatetime}' 
                                               where foc_cnno = '{foc_cnno}';"""
                    dest_cursor.execute(update_query)
                    uc = dest_cursor.rowcount
                    rows_updated += uc            


            if rows_updated > 0:
                dest_connection.commit()
                logging.info(f"Successfully updated {rows_updated} row(s) for foc_cnno {foc_cnno}.") 
            else:
                 self.log_zero_update(table_name, message_value)
                 logging.info(f"No rows updated for foc_cnno {foc_cnno}.")             

        except mysql.connector.Error as err:
            # Log MySQL specific errors
            logging.error(f"MySQL error: {err}")
        except Exception as e:
            # Log any other unexpected errors
            logging.error(f"Unexpected error: {e}")
        finally:
            for cursor in [source_cursor, dest_cursor]:
                if cursor:
                    cursor.close()
            for connection in [source_connection, dest_connection]:
                if connection:
                    connection.close()  



    def booking_refrence_link_updates(self, foc_consigments_tbl, table_name, df, message_value):
        try:
            with closing(mysql.connector.connect(**self.dest_settings)) as connection:
                with closing(connection.cursor()) as cursor:
                    rows_updated = 0 
                    for _, row in df.iterrows():
                        set_clause = ', '.join(f"{col} = %s" for col in row.index if col != 'foc_cnno')
                        update_query = f"""UPDATE {foc_consigments_tbl} SET {set_clause} WHERE foc_cnno = %s;"""
                        values = (*row.drop('foc_cnno').values, row['foc_cnno'])
                        cursor.execute(update_query, values)
                        rows_updated += cursor.rowcount  # Increment the count of updated rows

                    if rows_updated == 0:
                        self.log_zero_update(table_name, message_value)

                    connection.commit()
                    logging.info("All Booking Detailes queries executed successfully.")

        except mysql.connector.Error as err:
            logging.error(f"SQL Error: {err}")


    def payments_details_updates(self, foc_consigments_tbl, table_name, df, message_value):
        try:
            with closing(mysql.connector.connect(**self.dest_settings)) as connection:
                with closing(connection.cursor()) as cursor:
                    rows_updated = 0 
                    for _, row in df.iterrows():
                        set_clause = ', '.join(f"{col} = %s" for col in row.index if col != 'foc_cnno')
                        update_query = f"""UPDATE {foc_consigments_tbl} SET {set_clause} WHERE foc_cnno = %s;"""
                        values = (*row.drop('foc_cnno').values, row['foc_cnno'])
                        cursor.execute(update_query, values)
                        rows_updated += cursor.rowcount  # Increment the count of updated rows

                    if rows_updated == 0:
                        self.log_zero_update(table_name, message_value)

                    connection.commit()
                    logging.info("All Payments Detailes queries executed successfully.")

        except mysql.connector.Error as err:
            logging.error(f"SQL Error: {err}")                                


    def log_zero_update(self, table_name, message_value):
        try:
            with closing(mysql.connector.connect(**self.dest_settings)) as connection:
                with closing(connection.cursor()) as cursor:
                    log_query = "INSERT INTO failure_log (table_name, message) VALUES (%s, %s);"
                    cursor.execute(log_query, (table_name, message_value))
                    connection.commit()
                    logging.info(f"Logged zero update for message in {table_name}.")
        except mysql.connector.Error as err:
            logging.error(f"Failed to log zero update: {err}")


    def run(self):
        try:
            self.create_consumer()
            self.subscribe()
            self.consume_messages()
        except Exception as e:
            logging.error(f"An error occurred in run: {e}")
        finally:
            self.close_consumer()
