config = {
    "consumer_settings": {
        "bootstrap.servers": 'localhost:9092',  # Single string, not a list
        #"auto.offset.reset": 'latest',
        "auto.offset.reset": "earliest",
        "group.id": 'LmConsumersGroup-2',
        "enable.auto.commit": False
    },

    
    "producer_settings": {
        "bootstrap.servers": 'localhost:9092'  # Same as consumer settings
    },

    #,'tracking_db.tracking_live.PAYMENT_DETAILS','tracking_db.tracking_live.CONSIGNMENT_HISTORY'

    "topics": ['tracking_db.tracking_live.CONSIGNMENT','tracking_db.tracking_live.CONSIGNMENT_HISTORY','tracking_db.tracking_live.REFERENCE_LINK'],

    "Consignment_Final_Schema": ['foc_cnno','foc_booking_date','foc_dlvoffice_id','foc_dlvpin_code','foc_destination_branch_code','foc_dlvlchangedpin'],

    "Refrence_Link_Final_Schema": ['foc_cnno', 'record_updated_date','foc_podimage'],

    "Adress_Details_Final_Schema": ['foc_cnno','record_updated_date','foc_receiver_name','foc_receiver_phno', 'foc_receiver_address', 'foc_receiver_city', 'foc_receiver_state', 'foc_receiver_pincode', 'foc_sender_name', 'foc_sender_phno', 'foc_sender_address', 'foc_sender_city', 'foc_sender_state', 'foc_sender_pincode', 'foc_receiver_company_name', 'foc_sender_company_name'],

    "Payment_Details_Final_Schema": ['foc_cnno','record_updated_date', 'foc_ndx_amtcollected'],
    
    "Consignment_History_Final_Schema": ['foc_cnno', "foc_dlvdate_id", "foc_dlvdate", "foc_dlvoffice_code", "foc_dlvoffice_cdindatetime", 
    "foc_dlvoffice_bagindatetime", "foc_dlvoffice_pkttindatetime", "foc_dlvcnindatetime", 
    "foc_dlv1mfdatetime", "foc_dlv1mnfstno", "foc_dlv1statupddatetime", "foc_dlv1ndelreason", 
    "foc_dlv1updchannel", "foc_dlv2mfdatetime", "foc_dlv2mnfstno", 
    "foc_dlv2statupddatetime", "foc_dlv2ndelreason", "foc_dlv2updchannel", "foc_dlv2updby", 
    "foc_dlv3mfdatetime", "foc_dlv3mnfstno", "foc_dlv3statupddatetime", "foc_dlv3ndelreason", 
    "foc_dlv3updchannel", "foc_dlv3updby", "foc_dlvupdchannel", "foc_dlvupdby", "foc_dlvreturndatetime", 
    "foc_dlvlpreprationdatetime", "foc_dlvlmfdatetime", "foc_dlvlmnfstno", 
    "foc_dlvlstatupddatetime", "foc_dlvlndelreason", "foc_dlvlndrdatetime", "foc_dlvlupdchannel", 
    "foc_dlvlupdby", "foc_dlvltype", "foc_dlvtype", 
    "foc_biker_code", "foc_attempt_no", "foc_dlv1_bikercode", "foc_dlv2_bikercode", 
    "foc_dlv3_bikercode", "foc_dlvl_bikercode", 
    "foc_dmcinscan", "foc_rtortrb_date", "foc_reattemptinit_date", "foc_dlvoffice_airscandatetime", 
    "foc_frdesc", "foc_dlvoffice_first_transport_name", "foc_dlvoffice_latest_transport_name", 
    "foc_dlvbmfno", "foc_dlvdestinscandatetime", "foc_rtorerb_date_first", "foc_rtorerb_date_second", 
    "foc_rtorerb_date_third", "foc_rtorerb_attempt_count", "foc_dlv1bkraccdatetime", 
    "foc_dlv2bkraccdatetime", "foc_dlv3bkraccdatetime", "foc_dlvlbkraccdatetime", 
    "foc_pre_bkbrpmfdatetime", "foc_pre_bkbrpmf_office_code", "foc_dlv1fbdmsavedatetime", 
    "foc_dlv2fbdmsavedatetime", "foc_dlv3fbdmsavedatetime", "foc_dlvlfbdmsavedatetime", 
    "foc_dlv1bkrrejdatetime", "foc_dlv2bkrrejdatetime", "foc_dlv3bkrrejdatetime", 
    "foc_dlvlbkrrejdatetime", "foc_setrtodate","foc_setrtodate1", "foc_setrtodate2", "foc_setrtodate3", 
    "foc_setrtodatel","foc_frdesc1","foc_frdesc2","foc_frdesc3","foc_dlv_bkraccdatetime","foc_dlv_bkrrejdatetime",
    "foc_dlv_mfdatetime","foc_dlv_ndelreason","foc_dlv_preprationdatetime","foc_rtorerb_date",
    "foc_dlv_fbdmsavedatetime","foc_dlv_ndrdatetime","foc_dlv_mnfstno","foc_dlv_statupddatetime",
    "foc_dlv_updchannel","foc_current_hub_code"]                                 
}
