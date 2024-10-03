import pandas as pd
import polars as pl
import numpy as np
import os 

def ALP_PETROMILES_ACCURAL(payload):

    # path = '/Users/apple/Downloads/BPCL/ALP_PETROMILES_ACCURAL/ConsolidatedReport.xlsx'
    # output_reports = '/Users/apple/Downloads/BPCL/ALP_PETROMILES_ACCURAL/ALP_VS_ACCRUAL_PETROMILES_Report_01-02-2024.xlsx'

    date = payload['statementDate'].strftime("%d-%m-%Y")

    date1 = payload['statementDate'].strftime("%d%m%Y")

    # path = f'/data/ngerecon/mft/OUTPUT/65c4e0578b611e59650ee9f5/{date1}/ConsolidatedReport.xlsx'

    os.makedirs(f'/data/ngerecon/mft/OUTPUT/65c4e0578b611e59650ee9f5/{date1}', exist_ok=True)

    output_reports = f'/data/ngerecon/mft/OUTPUT/65c4e0578b611e59650ee9f5/{date1}/ALP_VS_ACCRUAL_PETROMILES_Report_{date}.xlsx'

    writer = pd.ExcelWriter(output_reports)  # engine='openpyxl',mode='a',if_sheet_exists='replace'

    # sap = pd.read_excel(path, sheet_name='SAP_GL', dtype='str')

    sap = payload['results']['SAP_GL'].clone() #pd.read_excel(path, sheet_name='UFILL_VOUCHER_CREATED', dtype='str')
    sap = sap.with_columns(pl.col('SOURCE').map_dict(payload['sourceIdNameMap']).alias('SOURCE'))
    sap = sap.to_pandas()

    print('Read sap done')

    alp = payload['results']['ALP_TRANSACTION_DETAILS'].clone() #pd.read_excel(path, sheet_name='UFILL_VOUCHER_CREATED', dtype='str')
    alp = alp.with_columns(pl.col('SOURCE').map_dict(payload['sourceIdNameMap']).alias('SOURCE'))
    alp = alp.to_pandas()

    print('Read alp done')


    capli = payload['results']['CAPILLARY_TRANSACTION_REPORT'].clone() #pd.read_excel(path, sheet_name='UFILL_VOUCHER_CREATED', dtype='str')
    capli = capli.with_columns(pl.col('SOURCE').map_dict(payload['sourceIdNameMap']).alias('SOURCE'))
    capli = capli.to_pandas()

    print('Read capli done')

    writer = pd.ExcelWriter(output_reports)  # engine='openpyxl',mode='a',if_sheet_exists='replace'

    sap.rename(columns={'AMOUNT': 'DC_AMOUNT','DOC_DATE':'DATE'}, inplace=True)
    sap['DC_AMOUNT'] = sap['DC_AMOUNT'].replace('', 0).fillna(0).astype(np.float64)
    sap['DATE'] = pd.to_datetime(sap['DATE'],format='%Y-%m-%d').dt.strftime('%d-%m-%Y')

   
    alp.rename(columns={'p_xblnr': 'REFERENCE', 'DC_NET_AMOUNT': 'DC_AMOUNT','p_transactiondate':'DATE'}, inplace=True)
    alp['DC_AMOUNT'] = alp['DC_AMOUNT'].replace('', 0).fillna(0).astype(np.float64)
    alp['DATE'] = pd.to_datetime(alp['DATE'],format='%Y-%m-%d %H:%M:%S').dt.strftime('%d-%m-%Y')

    # capli = pd.read_excel(path, sheet_name='CAPILLARY_TRANSACTION_REPORT', dtype='str')

    capli.rename(columns={'p_xblnr': 'REFERENCE', 'DC_NET_AMOUNT': 'DC_AMOUNT','Date':'DATE','BILL_NUMBER':'P_TRANSACTIONID'}, inplace=True)
    capli['DC_AMOUNT'] = capli['DC_AMOUNT'].replace('', 0).fillna(0).astype(np.float64)
    capli['DATE'] = pd.to_datetime(capli['DATE'],format='%d/%m/%y').dt.strftime('%d-%m-%Y')


    sap['3_WAY_REMARKS'] = sap['ACTION'] = ''
    sap.loc[((sap['MATCHING_STATUS'] == 'UNMATCHED') & (sap['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))& (sap['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), '3_WAY_REMARKS'] = "SAP, ALP and CAPILLARY Amount Mismatched"
    sap.loc[((sap['MATCHING_STATUS'] == 'UNMATCHED') & (sap['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))& (sap['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), 'ACTION'] = "SAP, ALP and CAPILLARY need to check"

    sap.loc[((sap['MATCHING_STATUS'] == 'UNMATCHED') & (sap['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))& ~(sap['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), '3_WAY_REMARKS'] = "REFERENCE missing in CAPILLARY_TRANSACTION_REPORT"
    sap.loc[((sap['MATCHING_STATUS'] == 'UNMATCHED') & (sap['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))& ~(sap['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), 'ACTION'] = "CAPILLARY_TRANSACTION_REPORT need to check"

    sap.loc[((sap['MATCHING_STATUS'] == 'UNMATCHED') & ~(sap['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))& (sap['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), '3_WAY_REMARKS'] = "REFERENCE missing in ALP_TRANSACTION_DETAILS"
    sap.loc[((sap['MATCHING_STATUS'] == 'UNMATCHED') & ~(sap['REFERENCE'].isin(alp['REFERENCE'].unique().tolist())) & (sap['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), 'ACTION'] = "ALP_TRANSACTION_DETAILS need to check"

    sap.loc[((sap['MATCHING_STATUS'] == 'UNMATCHED') & ~(sap['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))& ~(sap['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), '3_WAY_REMARKS'] = "REFERENCE missing in ALP_TRANSACTION_DETAILS & CAPILLARY_TRANSACTION_REPORT"
    sap.loc[((sap['MATCHING_STATUS'] == 'UNMATCHED') & ~(sap['REFERENCE'].isin(alp['REFERENCE'].unique().tolist())) & ~(sap['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), 'ACTION'] = "ALP_TRANSACTION_DETAILS & CAPILLARY_TRANSACTION_REPORT need to check"


    sap.loc[(sap['REFERENCE'].fillna('') == ''), '3_WAY_REMARKS'] = "REFERENCE number is missing"
    sap.loc[(sap['REFERENCE'].fillna('') == ''), 'ACTION'] = "SAP need to check"

    sap.loc[(sap['MATCHING_STATUS'] == 'MATCHED'), '3_WAY_REMARKS'] = "SAP Posting done - SAP , ALP and CAPILLARY Settlement File Matched correctly"
    sap.loc[(sap['MATCHING_STATUS'] == 'MATCHED'), 'ACTION'] = "No Action Required"
    print(sap['3_WAY_REMARKS'].value_counts())
    print(sap['ACTION'].value_counts())
    list1 = ['REFERENCE', 'DOC_NO','DATE','3_WAY_REMARKS', 'ACTION','CARRY_FORWARD', 'MATCHING_STATUS','SOURCE']
    for i in list1:
        sap[i] = sap[i].fillna('').astype('str')

    sap1 = sap.groupby(['REFERENCE', 'DOC_NO','DATE','3_WAY_REMARKS', 'ACTION','CARRY_FORWARD', 'MATCHING_STATUS','SOURCE']).agg(
        {'DC_AMOUNT': 'sum', 'FEED_FILE_NAME': 'count'}).reset_index()
    sap1.rename(columns={'DC_AMOUNT':'SAP_AMOUNT','FEED_FILE_NAME':'SAP_COUNT'},inplace=True)
    print('sapl1\n',sap1)


    alp['3_WAY_REMARKS'] = alp['2_WAY_REMARKS'] = alp['ACTION'] = ''
    alp.loc[((alp['MATCHING_STATUS'] == 'UNMATCHED') & (alp['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & (alp['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), '3_WAY_REMARKS'] = "SAP, ALP and CAPILLARY Amount Mismatched"
    alp.loc[((alp['MATCHING_STATUS'] == 'UNMATCHED') & (alp['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & (alp['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), 'ACTION'] = "SAP, ALP and CAPILLARY need to check"

    alp.loc[((alp['MATCHING_STATUS'] == 'UNMATCHED') & (alp['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & ~(alp['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), '3_WAY_REMARKS'] += "REFERENCE missing in CAPILLARY_TRANSACTION_REPORT"
    alp.loc[((alp['MATCHING_STATUS'] == 'UNMATCHED') & (alp['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & ~(alp['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), 'ACTION'] += "CAPILLARY_TRANSACTION_REPORT need to check"

    alp.loc[((alp['MATCHING_STATUS'] == 'UNMATCHED') & ~(alp['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & (alp['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), '3_WAY_REMARKS'] += "REFERENCE missing in SAP"
    alp.loc[((alp['MATCHING_STATUS'] == 'UNMATCHED') & ~(alp['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & (alp['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), 'ACTION'] += "SAP need to check"

    alp.loc[((alp['MATCHING_STATUS'] == 'UNMATCHED') & ~(alp['REFERENCE'].isin(sap['REFERENCE'].unique().tolist()))& ~(alp['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), '3_WAY_REMARKS'] = "REFERENCE missing in CAPILLARY_TRANSACTION_REPORT & SAP"
    alp.loc[((alp['MATCHING_STATUS'] == 'UNMATCHED') & ~(alp['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & ~(alp['REFERENCE'].isin(capli['REFERENCE'].unique().tolist()))), 'ACTION'] = "CAPILLARY_TRANSACTION_REPORT & SAP need to check"

    alp.loc[(alp['REFERENCE'].fillna('') == ''), '3_WAY_REMARKS'] = "REFERENCE number is missing"
    alp.loc[(alp['REFERENCE'].fillna('') == ''), 'ACTION'] = "ALP need to check"


    #alp.loc[(alp['CAPILLARY_TRANSACTION_REPORT Level_1_Status']=='UNMATCHED'), '2_WAY_REMARKS'] = "REFERENCE Not Matched with CAPILLARY_TRANSACTION_REPORT"
    #alp.loc[(alp['CAPILLARY_TRANSACTION_REPORT Level_1_Status']=='MATCHED'), '2_WAY_REMARKS'] = "REFERENCE Matched with CAPILLARY_TRANSACTION_REPORT"
     
    #akhil added

    alp.loc[(alp['CAPILLARY_TRANSACTION_REPORT Level_1_Status']=='UNMATCHED'), '2_WAY_REMARKS'] = "P_TRANSACTIONID Not Matched with CAPILLARY_TRANSACTION_REPORT"
    alp.loc[(alp['CAPILLARY_TRANSACTION_REPORT Level_1_Status']=='MATCHED'), '2_WAY_REMARKS'] = "P_TRANSACTIONID Matched with CAPILLARY_TRANSACTION_REPORT"



    alp.loc[(alp['MATCHING_STATUS'] == 'MATCHED'), '3_WAY_REMARKS'] = "SAP Posting done - SAP, ALP and CAPILLARY Settlement File Matched correctly"
    alp.loc[(alp['MATCHING_STATUS'] == 'MATCHED'), 'ACTION'] = "No Action Required"
    print(alp['3_WAY_REMARKS'].value_counts())
    print(alp['ACTION'].value_counts())

    list1 = ['REFERENCE','DATE','3_WAY_REMARKS', 'ACTION','CARRY_FORWARD', 'MATCHING_STATUS','SOURCE']
    for i in list1:
        alp[i] = alp[i].fillna('').astype('str')

    alp1 = alp.groupby(['REFERENCE','DATE','3_WAY_REMARKS', 'ACTION','CARRY_FORWARD', 'MATCHING_STATUS','SOURCE']).agg(
        {'DC_AMOUNT': 'sum', 'FEED_FILE_NAME': 'count'}).reset_index()
    alp1.rename(columns={'DC_AMOUNT':'ALP_AMOUNT','FEED_FILE_NAME':'ALP_COUNT'},inplace=True)

    print(alp1)

    capli['3_WAY_REMARKS'] = capli['2_WAY_REMARKS'] = capli['ACTION'] = ''
    capli.loc[((capli['MATCHING_STATUS'] == 'UNMATCHED') & (capli['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & (capli['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))), '3_WAY_REMARKS'] = "SAP, ALP and CAPILLARY Amount Mismatched"
    capli.loc[((capli['MATCHING_STATUS'] == 'UNMATCHED') & (capli['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & (capli['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))), 'ACTION'] = "SAP, ALP and CAPILLARY need to check"

    capli.loc[((capli['MATCHING_STATUS'] == 'UNMATCHED') & (capli['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & ~(capli['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))), '3_WAY_REMARKS'] += "REFERENCE missing in ALP_TRANSACTION_DETAILS"
    capli.loc[((capli['MATCHING_STATUS'] == 'UNMATCHED') & (capli['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & ~(capli['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))), 'ACTION'] += "ALP_TRANSACTION_DETAILS need to check"

    capli.loc[((capli['MATCHING_STATUS'] == 'UNMATCHED') & ~(capli['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & (capli['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))), '3_WAY_REMARKS'] += "REFERENCE missing in SAP"
    capli.loc[((capli['MATCHING_STATUS'] == 'UNMATCHED') & ~(capli['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & (capli['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))), 'ACTION'] += "SAP need to check"

    capli.loc[((capli['MATCHING_STATUS'] == 'UNMATCHED') & ~(capli['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & ~(capli['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))), '3_WAY_REMARKS'] += "REFERENCE missing in ALP_TRANSACTION_DETAILS & SAP"
    capli.loc[((capli['MATCHING_STATUS'] == 'UNMATCHED') & ~(capli['REFERENCE'].isin(sap['REFERENCE'].unique().tolist())) & ~(capli['REFERENCE'].isin(alp['REFERENCE'].unique().tolist()))), 'ACTION'] += "ALP_TRANSACTION_DETAILS & SAP need to check"

    capli.loc[(capli['REFERENCE'].fillna('') == ''), '3_WAY_REMARKS'] = "REFERENCE number is missing"
    capli.loc[(capli['REFERENCE'].fillna('') == ''), 'ACTION'] = "CAPILLARY need to check"

    capli.loc[(capli['ALP_TRANSACTION_DETAILS Level_1_Status']=='UNMATCHED'), '2_WAY_REMARKS'] = "P_TRANSACTIONID Not Matched with ALP_TRANSACTION_DETAILS"
    capli.loc[(capli['ALP_TRANSACTION_DETAILS Level_1_Status']=='MATCHED'), '2_WAY_REMARKS'] = "P_TRANSACTIONID Matched with ALP_TRANSACTION_DETAILS"


    capli.loc[(capli['MATCHING_STATUS'] == 'MATCHED'), '3_WAY_REMARKS'] = "SAP Posting done - SAP, ALP and CAPILLARY Settlement File Matched correctly"
    capli.loc[(capli['MATCHING_STATUS'] == 'MATCHED'), 'ACTION'] = "No Action Required"
    print(capli['3_WAY_REMARKS'].value_counts())
    print(capli['ACTION'].value_counts())

    list1 = ['REFERENCE','DATE','3_WAY_REMARKS', 'ACTION','CARRY_FORWARD', 'MATCHING_STATUS','SOURCE']
    for i in list1:
        capli[i] = capli[i].fillna('').astype('str')

    capli1 = capli.groupby(['REFERENCE','DATE','3_WAY_REMARKS', 'ACTION','CARRY_FORWARD', 'MATCHING_STATUS','SOURCE']).agg(
        {'DC_AMOUNT': 'sum', 'FEED_FILE_NAME': 'count'}).reset_index()
    capli1.rename(columns={'DC_AMOUNT':'CAPILLARY_AMOUNT','FEED_FILE_NAME':'CAPILLARY_COUNT'},inplace=True)
    print(capli1)


    alp1.rename(columns={'3_WAY_REMARKS': '3_WAY_REMARKS_alp', 'ACTION': 'ACTION_alp', 'CARRY_FORWARD': 'CARRY_FORWARD_alp'},inplace=True)

    summary = sap1.merge(alp1[['REFERENCE', 'MATCHING_STATUS', 'ALP_COUNT', 'ALP_AMOUNT','3_WAY_REMARKS_alp','ACTION_alp','CARRY_FORWARD_alp']].drop_duplicates(['REFERENCE', 'MATCHING_STATUS']),
                        on=['REFERENCE', 'MATCHING_STATUS'], how='outer')

    summary.loc[(summary['3_WAY_REMARKS'].fillna('') == ''), '3_WAY_REMARKS'] = summary['3_WAY_REMARKS_alp']
    summary.loc[(summary['ACTION'].fillna('') == ''), 'ACTION'] = summary['ACTION_alp']
    summary.loc[(summary['CARRY_FORWARD'].fillna('') == ''), 'CARRY_FORWARD'] = summary['CARRY_FORWARD_alp']
    summary.drop(['3_WAY_REMARKS_alp','ACTION_alp','CARRY_FORWARD_alp'],axis=1,inplace=True,errors='ignore')

    capli1.rename(columns={'3_WAY_REMARKS': '3_WAY_REMARKS_cp', 'ACTION': 'ACTION_cp', 'CARRY_FORWARD': 'CARRY_FORWARD_cp'},inplace=True)

    summary = summary.merge(capli1[['REFERENCE', 'MATCHING_STATUS', 'CAPILLARY_COUNT', 'CAPILLARY_AMOUNT','3_WAY_REMARKS_cp','ACTION_cp','CARRY_FORWARD_cp']].drop_duplicates(['REFERENCE', 'MATCHING_STATUS']),
                            on=['REFERENCE', 'MATCHING_STATUS'], how='outer')
    summary.loc[(summary['3_WAY_REMARKS'].fillna('') == ''), '3_WAY_REMARKS'] = summary['3_WAY_REMARKS_cp']
    summary.loc[(summary['ACTION'].fillna('') == ''), 'ACTION'] = summary['ACTION_cp']
    summary.loc[(summary['CARRY_FORWARD'].fillna('') == ''), 'CARRY_FORWARD'] = summary['CARRY_FORWARD_cp']
    summary.drop(['3_WAY_REMARKS_cp','ACTION_cp','CARRY_FORWARD_cp'],axis=1,inplace=True,errors='ignore')

    summary['ALP_VS_CAPILLARY_DIFF'] = summary['ALP_AMOUNT'].fillna(0) - summary['CAPILLARY_AMOUNT'].fillna(0)
    summary['ALP_VS_SAP_DIFF'] = summary['ALP_AMOUNT'].fillna(0) - summary['SAP_AMOUNT'].fillna(0)
    
    summary['ALP_VS_CAPILLARY_DIFF'] = summary['ALP_VS_CAPILLARY_DIFF'].fillna(0).abs()
    summary['ALP_VS_SAP_DIFF'] = summary['ALP_VS_SAP_DIFF'].fillna(0).abs()
    
    reorder = ['REFERENCE','DOC_NO','DOC_DATE', 'CARRY_FORWARD', 'MATCHING_STATUS',
               'ALP_COUNT','CAPILLARY_COUNT','SAP_COUNT','ALP_AMOUNT','CAPILLARY_AMOUNT','SAP_AMOUNT','ALP_VS_CAPILLARY_DIFF','ALP_VS_SAP_DIFF','3_WAY_REMARKS', 'ACTION']
    extra_col = [] #[x for x in summary.columns.tolist() if x not in reorder]
    reorder.extend(extra_col)
    summary = summary.loc[:, ~summary.columns.duplicated()].copy()
    summary = summary.reindex(columns=reorder)
    summary.columns = [x.upper() for x in summary.columns.tolist()]
    summary.sort_values(['MATCHING_STATUS','REFERENCE'], inplace=True)
    summary.to_excel(writer, sheet_name='Summary', index=False)

    reorder = ["REFERENCE","DOC_NO","DATE","DC_AMOUNT","3_WAY_REMARKS","ACTION","CARRY_FORWARD","MATCHING_STATUS","FILENAME","POST_DATE","DOC_TYPE"]
    extra_col = [x for x in sap.columns.tolist() if x not in reorder]
    reorder.extend(extra_col)
    sap = sap.loc[:, ~sap.columns.duplicated()].copy()
    sap = sap.reindex(columns=reorder)
    sap.columns = [x.upper() for x in sap.columns.tolist()]
    sap.sort_values(['MATCHING_STATUS','REFERENCE'], inplace=True)
    sap.to_excel(writer, sheet_name='SAP_GL', index=False)

    reorder = ["REFERENCE","DOC_NO","p_transactionid","DATE","DC_AMOUNT","3_WAY_REMARKS","ACTION","2_WAY_REMARKS","CARRY_FORWARD","MATCHING_STATUS","FILENAME","POST_DATE","DOC_TYPE",'PK_TYPE','Code','PK_STATUS','StatusCode','PK_MODE','PaymentCode']
    extra_col = [x for x in alp.columns.tolist() if x not in reorder]
    reorder.extend(extra_col)
    alp = alp.loc[:, ~alp.columns.duplicated()].copy()
    alp = alp.reindex(columns=reorder)
    alp.columns = [x.upper() for x in alp.columns.tolist()]
    alp.sort_values(['MATCHING_STATUS','REFERENCE'], inplace=True)
    alp.to_excel(writer, sheet_name='ALP_TRANSACTION_DETAILS', index=False)

    reorder = ["REFERENCE","DOC_NO","Bill_Number","DATE","DC_AMOUNT","3_WAY_REMARKS","ACTION","2_WAY_REMARKS","CARRY_FORWARD","MATCHING_STATUS","FILENAME","POST_DATE","DOC_TYPE"]
    extra_col = [x for x in capli.columns.tolist() if x not in reorder]
    reorder.extend(extra_col)
    capli = capli.loc[:, ~capli.columns.duplicated()].copy()
    capli = capli.reindex(columns=reorder)
    capli.columns = [x.upper() for x in capli.columns.tolist()]
    capli.sort_values(['MATCHING_STATUS','REFERENCE'], inplace=True)
    capli.to_excel(writer, sheet_name='CAPILLARY_TRANSACTION_REPORT', index=False)
    writer.save()

    print('DONE')

# payload={}
# ALP_PETROMILES_ACCURAL(payload)
# exit()
