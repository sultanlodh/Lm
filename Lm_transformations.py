
import pandas as pd
import numpy as np   
from datetime import datetime
import json

def transform_consignments(self, df):
        return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
            timestamp = lambda df:  df['BookingTs'].apply(convert_to_timestamp),
            foc_booking_date = lambda df: df['timestamp'].apply(lambda x: x.date() if pd.notna(x) else None),
            foc_dlvoffice_id = lambda df: np.where(df['Delivered_By_Hub_Code'] is not None, df['Delivered_By_Hub_Code'], None),
            foc_dlvpin_code = lambda df: np.where(df['Destination_Pincode'] is not None, df['Destination_Pincode'], None),
            foc_destination_branch_code = lambda df: np.where(df['Destination_Branch_Code'] is not None, df['Destination_Branch_Code'], None),
            foc_dlvlchangedpin = lambda df: np.where(df['Destination_Pincode'] is not None, df['Destination_Pincode'], None)
        )

def transform_refrence_link(self, df):
        now = datetime.now()
        curent_dt = now.strftime("%Y-%m-%d %H:%M:%S") 
        return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
            record_updated_date=lambda x: curent_dt,
            foc_podimage = lambda df: np.where(df['Link'].notna(), df['Link'], None)
        )

def transform_payment_details(self, df):
        now = datetime.now()
        curent_dt = now.strftime("%Y-%m-%d %H:%M:%S") 
        return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
                record_updated_date=lambda x: curent_dt,
                foc_ndx_amtcollected = lambda df: np.where(df['Collected_Amount'].notna(), df['Collected_Amount'], None)
        )
    

def transform_address_dtails(self, df):
        now = datetime.now()
        curent_dt = now.strftime("%Y-%m-%d %H:%M:%S") 
        return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
            foc_receiver_name = np.where(df['Address_Type'] == 'Receiver', df['First_name'].fillna('') + ' ' + df['Middle_name'].fillna('') + ' ' + df['Last_name'].fillna(''), None),
            foc_receiver_company_name = np.where((df['Address_Type'] == 'Receiver') & (df['First_name'].str.contains('LTD', case=False, na=False)),df['First_name'],None),
            foc_receiver_phno = np.where(df['Address_Type'] == 'Receiver', df['Phone'].fillna('') + '/' + df['Mobile'].fillna(''), None),
            foc_receiver_address = np.where(df['Address_Type'] == 'Receiver', df['Street_1'].fillna('') + ',' + df['Street_2'].fillna('') + ',' + df['Street_3'].fillna(''), None),
            foc_receiver_city = np.where(df['Address_Type'] == 'Receiver', df['City'].fillna(''), None),
            foc_receiver_state = np.where(df['Address_Type'] == 'Receiver', df['State'].fillna(''), None),
            foc_receiver_pincode = np.where(df['Address_Type'] == 'Receiver', df['Pincode'].fillna(''), None),
            record_updated_date=lambda x: self.curent_dt,
            foc_sender_name = np.where(df['Address_Type'] == 'Sender', df['First_name'].fillna('') + ' ' + df['Middle_name'].fillna('') + ' ' + df['Last_name'].fillna(''), None),
            foc_sender_company_name = np.where((df['Address_Type'] == 'Sender') & (df['First_name'].str.contains('LTD', case=False, na=False)),df['First_name'],None),
            foc_sender_phno = np.where(df['Address_Type'] == 'Sender', df['Phone'].fillna('') + '/' + df['Mobile'].fillna(''), None),
            foc_sender_address = np.where(df['Address_Type'] == 'Sender', df['Street_1'].fillna('') + ',' + df['Street_2'].fillna('') + ',' + df['Street_3'].fillna(''), None),
            foc_sender_city = np.where(df['Address_Type'] == 'Sender', df['City'].fillna(''), None),
            foc_sender_state = np.where(df['Address_Type'] == 'Sender', df['State'].fillna(''), None),
            foc_sender_pincode = np.where(df['Address_Type'] == 'Sender', df['Pincode'].fillna(''), None)
        )
    

def Transform_Consignment_History(self, df):
    now = datetime.now()
    current_dt = now.strftime("%Y-%m-%d %H:%M:%S")  # Fixed typo here
    return df.rename(columns={'Tracking_Number': 'foc_cnno'}).assign(
            record_updated_date=lambda x: self.curent_dt,
            statusTs_temp = lambda df: df['StatusTs'].apply(convert_to_timestamp),
            foc_dlvdate_id = lambda df: np.where((df['Status_Code'] == 'DLV') & (df['statusTs_temp'] is not None), df['statusTs_temp'], None),
            foc_dlvdate = lambda df: np.where((df['Status_Code'] == 'DLV') & (df['statusTs_temp'] is not None), df['statusTs_temp'], None),
            foc_dlvupdchannel = lambda df: np.where(df['Status_Code'] == 'DLV', df['JSON_Data'].apply(lambda x: json.loads(x).get('NODE_ID', None) if x and isinstance(x, str) else None), None),
            foc_dlvreturndatetime = lambda df: np.where((df['Status_Code'] == 'RTOOUTDLV') & (df['statusTs_temp'] is not None), df['statusTs_temp'], None),
            Franchisee_Code = lambda df: np.where(df['Status_Code'] == 'DLV', df['JSON_Data'].apply(lambda x: json.loads(x).get('Franchisee_Code', None) if x and isinstance(x, str) else None), None),
            foc_biker_code = lambda df: np.where(df['Biker_code'] is not None, df['Biker_code'], None),
            foc_attempt_no = lambda df: np.where(df['Status_Code'].isin(['NONDLV','RTONONDLV']), df['JSON_Data'].apply(lambda x: json.loads(x).get('Attempt_No', None) if x and isinstance(x, str) else None), None),
            foc_dmcinscan = lambda df: np.where((df['Status_Code'].isin(['RERB','RTRB'])) & (df['statusTs_temp'] is not None), df['statusTs_temp'], None),
            foc_dlvbmfno = lambda df: np.where(df['Status_Code'].isin(['OBMN','IBMN','OBMD','OBMN']), df['JSON_Data'].apply(lambda x: json.loads(x).get('Bag_Manifest_Number', None) if x and isinstance(x, str) else None), None),
            foc_rtorerb_attempt_count = lambda df: np.where(df['Status_Code'] == 'RERB', 1, 0),
            Franchisee_Code_temp = lambda df: np.where(df['Status_Code'].isin(['OUTDLV','NONDLV','DLV']), df['JSON_Data'].apply(lambda x: json.loads(x).get('Franchisee_Code', None) if x and isinstance(x, str) else None), None),
            Emp_Code_temp = lambda df: np.where(df['Status_Code'].isin(['OUTDLV','NONDLV','DLV']), df['JSON_Data'].apply(lambda x: json.loads(x).get('EMP_CODE', None) if x and isinstance(x, str) else None), None),
            foc_dlvupdby = lambda df: np.where(df['Emp_Code_temp'] is None, df['Franchisee_Code_temp'], df['Emp_Code_temp']),
            foc_dlvlpreprationdatetime = lambda df: np.where(df['Status_Code'] == 'PREPERD', df['statusTs_temp'], None),
            foc_dlvlmfdatetime = lambda df: np.where(df['Status_Code'].isin(['OUTDLV','DLV']), df['statusTs_temp'], None),
            foc_dlvlmnfstno = np.where(df['Status_Code'].isin(['OUTDLV','DLV']), df['JSON_Data'].apply(lambda x: json.loads(x).get('Fdm_Number', None) if x and isinstance(x, str) else None), None),
            foc_dlvlstatupddatetime = lambda df: np.where(df['Status_Code'] == 'STATUSCODE', df['statusTs_temp'], None),
            foc_dlvlndelreason = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['Reason_Code'], None),
            foc_dlvlndrdatetime = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['statusTs_temp'], None),
            foc_dlvlupdchannel = np.where(df['Status_Code'] == 'OUTDLV', df['JSON_Data'].apply(lambda x: json.loads(x).get('NODE_ID', None) if x and isinstance(x, str) else None), None),
            foc_dlvlupdby = lambda df: np.where(df['Emp_Code_temp'] is None, df['Franchisee_Code_temp'], df['Emp_Code_temp']),

            foc_dlvtype = lambda df: np.where(df['Franchisee_Code'] is None, 'B', 'F'),
            foc_dlvltype = lambda df: np.where(df['Franchisee_Code'] is None, 'B', 'F'),
            #foc_dlv1type = lambda df: np.where(df['Franchisee_Code'] is None, 'B', 'F'),
            #foc_dlv2type = lambda df: np.where(df['Franchisee_Code'] is None, 'B', 'F'),
            #foc_dlv3type = lambda df: np.where(df['Franchisee_Code'] is None, 'B', 'F'),

            foc_dlv1_bikercode = lambda df: np.where(df['Biker_code'] is not None, df['Biker_code'], None),
            foc_dlv2_bikercode = lambda df: np.where(df['Biker_code'] is not None, df['Biker_code'], None),
            foc_dlv3_bikercode = lambda df: np.where(df['Biker_code'] is not None, df['Biker_code'], None),
            foc_dlvl_bikercode = lambda df: np.where(df['Biker_code'] is not None, df['Biker_code'], None),
            foc_rtortrb_date = lambda df: np.where(df['Status_Code'] == 'RTRB', df['statusTs_temp'], None),
            foc_reattemptinit_date = lambda df: np.where(df['Status_Code'] == 'REATTEMPTINIT', df['statusTs_temp'], None),

            foc_frdesc = lambda df: df['JSON_Data'].apply(lambda x: json.loads(x).get('Frnch_Type', None) if isinstance(x, str) and x else None),
            foc_frdesc2 = lambda df: df['JSON_Data'].apply(lambda x: json.loads(x).get('Frnch_Type', None) if isinstance(x, str) and x else None),
            foc_frdesc3 = lambda df: df['JSON_Data'].apply(lambda x: json.loads(x).get('Frnch_Type', None) if isinstance(x, str) and x else None),
            foc_frdesc1 = lambda df: df['JSON_Data'].apply(lambda x: json.loads(x).get('Frnch_Type', None) if isinstance(x, str) and x else None),

            foc_rtorerb_date = lambda df: np.where(df['Status_Code'] == 'RERB', df['statusTs_temp'], None),
            foc_rtorerb_date_first = lambda df: np.where(df['Status_Code'] == 'RERB', df['statusTs_temp'], None),
            foc_rtorerb_date_second = lambda df: np.where(df['Status_Code'] == 'RERB', df['statusTs_temp'], None),
            foc_rtorerb_date_third = lambda df: np.where(df['Status_Code'] == 'RERB', df['statusTs_temp'], None),
            foc_dlv_bkraccdatetime = lambda df: np.where(df['Status_Code'] == 'FDMA', df['statusTs_temp'], None),
            foc_dlv1bkraccdatetime = lambda df: np.where(df['Status_Code'] == 'FDMA', df['statusTs_temp'], None),
            #foc_dlv1bkraccdatetime = lambda df: np.where(df['Status_Code'] == 'FDMA', df['statusTs_temp'], None),
            foc_dlv2bkraccdatetime = lambda df: np.where(df['Status_Code'] == 'FDMA', df['statusTs_temp'], None),
            foc_dlv3bkraccdatetime = lambda df: np.where(df['Status_Code'] == 'FDMA', df['statusTs_temp'], None),
            foc_dlvlbkraccdatetime = lambda df: np.where(df['Status_Code'] == 'FDMA', df['statusTs_temp'], None),
            foc_pre_bkbrpmfdatetime = lambda df: np.where(df['Status_Code'].isin(['IPMF','OPMF']), df['statusTs_temp'], None),
            foc_pre_bkbrpmf_office_code = lambda df: np.where(df['Status_Code'].isin(['IPMF','OPMF']), df['Current_Hub_Code'], None),
            foc_dlv_fbdmsavedatetime = lambda df: np.where(df['Status_Code'] == 'BDMS', df['statusTs_temp'], None),
            foc_dlv1fbdmsavedatetime = lambda df: np.where(df['Status_Code'] == 'BDMS', df['statusTs_temp'], None),
            foc_dlv2fbdmsavedatetime = lambda df: np.where(df['Status_Code'] == 'BDMS', df['statusTs_temp'], None),
            foc_dlv3fbdmsavedatetime = lambda df: np.where(df['Status_Code'] == 'BDMS', df['statusTs_temp'], None),
            foc_dlvlfbdmsavedatetime = lambda df: np.where(df['Status_Code'] == 'BDMS', df['statusTs_temp'], None),

            foc_dlv_bkrrejdatetime = lambda df: np.where(df['Status_Code'] == 'FDMS', df['statusTs_temp'], None),
            foc_dlv1bkrrejdatetime = lambda df: np.where(df['Status_Code'] == 'FDMS', df['statusTs_temp'], None),
            foc_dlv2bkrrejdatetime = lambda df: np.where(df['Status_Code'] == 'FDMS', df['statusTs_temp'], None),
            foc_dlv3bkrrejdatetime = lambda df: np.where(df['Status_Code'] == 'FDMS', df['statusTs_temp'], None),
            foc_dlvlbkrrejdatetime = lambda df: np.where(df['Status_Code'] == 'FDMS', df['statusTs_temp'], None),

            foc_setrtodate1 = lambda df: np.where(df['Status_Code'] == 'SETRTO', df['statusTs_temp'], None),
            foc_setrtodate = lambda df: np.where(df['Status_Code'] == 'SETRTO', df['statusTs_temp'], None),
            foc_setrtodate2 = lambda df: np.where(df['Status_Code'] == 'SETRTO', df['statusTs_temp'], None),
            foc_setrtodate3 = lambda df: np.where(df['Status_Code'] == 'SETRTO', df['statusTs_temp'], None),
            foc_setrtodatel = lambda df: np.where(df['Status_Code'] == 'SETRTO', df['statusTs_temp'], None),
            foc_dlvoffice_code = lambda df: np.where(df['Status_Code'].isin(['DLV','OUTDLV','NONDLV']), df['Current_Hub_Code'], None),
            foc_current_hub_code = lambda df: np.where(df['Current_Hub_Code'] is not None , df['Current_Hub_Code'], None),
            foc_dlvoffice_cdindatetime = lambda df: np.where(df['Status_Code'] == 'CDIN', df['statusTs_temp'], None),
            foc_dlvoffice_bagindatetime = lambda df: np.where(df['Status_Code'].isin(['IBMN','IBMD']), df['statusTs_temp'], None),
            foc_dlvoffice_pkttindatetime = lambda df: np.where(df['Status_Code'] == 'IPMF', df['statusTs_temp'], None),
            foc_dlvcnindatetime = lambda df: np.where(df['Status_Code'] == 'INSCAN', df['statusTs_temp'], None),
            foc_dlv_mfdatetime = lambda df: np.where(df['Status_Code'].isin(['OUTDLV','DLV']), df['statusTs_temp'], None),
            foc_dlv1mfdatetime = lambda df: np.where(df['Status_Code'].isin(['OUTDLV','DLV']), df['statusTs_temp'], None),
            foc_dlv_mnfstno = lambda df: np.where(df['Status_Code'].isin(['OUTDLV','DLV']), df['JSON_Data'].apply(lambda x: json.loads(x).get('Fdm_Number', None) if x and isinstance(x, str) else None), None),
            foc_dlv1mnfstno = lambda df: np.where(df['Status_Code'].isin(['OUTDLV','DLV']), df['JSON_Data'].apply(lambda x: json.loads(x).get('Fdm_Number', None) if x and isinstance(x, str) else None), None),
            foc_dlv_statupddatetime = lambda df: np.where(df['Status_Code'] == 'STATUS_CODE', df['statusTs_temp'], None),
            foc_dlv1statupddatetime = lambda df: np.where(df['Status_Code'] == 'STATUS_CODE', df['statusTs_temp'], None),
            foc_dlv1ndelreason = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['Reason_Code'], None),
            foc_dlv_ndelreason = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['Reason_Code'], None),
            foc_dlv_updchannel = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['JSON_Data'].apply(lambda x: json.loads(x).get('NODE_ID', None) if x and isinstance(x, str) else None), None),
            foc_dlv1updchannel = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['JSON_Data'].apply(lambda x: json.loads(x).get('NODE_ID', None) if x and isinstance(x, str) else None), None),
            #foc_dlv1updby = lambda df: np.where(df['Franchisee_Code'] is None, df['Emp_Code_temp'], df['Franchisee_Code']),
            foc_dlv2mfdatetime = lambda df: np.where(df['Status_Code'].isin(['OUTDLV','DLV']), df['statusTs_temp'], None),
            foc_dlv3mfdatetime = lambda df: np.where(df['Status_Code'].isin(['OUTDLV','DLV']), df['statusTs_temp'], None),
            foc_dlv2mnfstno = lambda df: np.where(df['Status_Code'].isin(['OUTDLV','DLV']), df['JSON_Data'].apply(lambda x: json.loads(x).get('Fdm_Number', None) if x and isinstance(x, str) else None), None),
            foc_dlv2statupddatetime = lambda df: np.where(df['Status_Code'] == 'OUTDLV', df['statusTs_temp'], None),
            foc_dlv2ndelreason = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['Reason_Code'], None),
            foc_dlv2updchannel = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['JSON_Data'].apply(lambda x: json.loads(x).get('NODE_ID', None) if x and isinstance(x, str) else None), None),
            foc_dlv2updby = lambda df: np.where(df['Franchisee_Code'] is None, df['Emp_Code_temp'], df['Franchisee_Code']),
            foc_dlv3mnfstno = lambda df: np.where(df['Status_Code'].isin(['OUTDLV','DLV']), df['JSON_Data'].apply(lambda x: json.loads(x).get('Fdm_Number', None) if x and isinstance(x, str) else None), None),
            foc_dlv3statupddatetime = lambda df: np.where(df['Status_Code'] == 'STATUS_CODE', df['statusTs_temp'], None),
            foc_dlv3ndelreason = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['Reason_Code'], None),
            foc_dlv3updchannel = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['JSON_Data'].apply(lambda x: json.loads(x).get('NODE_ID', None) if x and isinstance(x, str) else None), None),
            foc_dlv3updby = lambda df: np.where(df['Franchisee_Code'] is None, df['Emp_Code_temp'], df['Franchisee_Code']),

            foc_dlv_preprationdatetime = lambda df: np.where(df['Status_Code'] == 'PREPERD', df['statusTs_temp'], None),
            #foc_dlv1preprationdatetime = lambda df: np.where(df['Status_Code'] == 'PREPERD', df['statusTs_temp'], None),
            #foc_dlv2preprationdatetime = lambda df: np.where(df['Status_Code'] == 'PREPERD', df['statusTs_temp'], None),
            #foc_dlv3preprationdatetime = lambda df: np.where(df['Status_Code'] == 'PREPERD', df['statusTs_temp'], None),

            #foc_d1v1ndrdatetime = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['statusTs_temp'], None),
            #foc_d1v2ndrdatetime = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['statusTs_temp'], None),
            #foc_d1v3ndrdatetime = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['statusTs_temp'], None),

            foc_dlvoffice_airscandatetime = lambda df: np.where(df['Status_Code'] == 'AIRCDIN', df['statusTs_temp'], None),
            foc_dlvoffice_first_transport_name = lambda df: np.where(df['Status_Code'].isin(['CDIN','CDOUT']), df['JSON_Data'].apply(lambda x: json.loads(x).get('Vehicle_Name', None) if x and isinstance(x, str) else None), None),
            foc_dlv_ndrdatetime = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['statusTs_temp'], None),
            #foc_dlv1ndrdatetime = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['statusTs_temp'], None),
            #foc_dlv2ndrdatetime = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['statusTs_temp'], None),
            #foc_dlv3ndrdatetime = lambda df: np.where(df['Status_Code'] == 'NONDLV', df['statusTs_temp'], None),
            foc_dlvoffice_latest_transport_name = lambda df: np.where(df['Status_Code'].isin(['CDIN','CDOUT']), df['JSON_Data'].apply(lambda x: json.loads(x).get('Vehicle_Name', None) if x and isinstance(x, str) else None), None),
            foc_dlvdestinscandatetime = lambda df: np.where(df['Status_Code'] == 'RADCDIN', df['statusTs_temp'], None)


    ) 


def convert_timestamp_to_datetime(value):
    if isinstance(value, (int, float)):  # Check if it's a numeric (epoch timestamp)
        return pd.to_datetime(value, unit='ns', errors='coerce')
    try:
        # If it's already a string or datetime-like object, convert directly
        return pd.to_datetime(value, errors='coerce')
    except (ValueError, TypeError):
        return pd.NaT
    

def convert_to_timestamp(value):
    if isinstance(value, (int, float)):
        if abs(value) > 1e13:
            return pd.to_datetime(value, unit='ns', errors='coerce')
        else:
            return pd.to_datetime(value, unit='ms', errors='coerce')
    try:
        return pd.to_datetime(value, errors='coerce')
    except (ValueError, TypeError):
        return pd.NaT  
