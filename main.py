import glob
import pandas as pd
import numpy as np
import re
import xlsxwriter
import lxml

def dataframes2xls(dfs, fname, path=''):
    with pd.ExcelWriter(path + fname + '.xlsx', engine='xlsxwriter') as writer:
        for name, df in dfs.items():
            df.to_excel(writer, index=False, header=True, sheet_name=name)
            # Get the xlsxwriter workbook and worksheet objects.
            workbook = writer.book
            worksheet = writer.sheets[name]
            # Get the dimensions of the dataframe.
            (max_row, max_col) = df.shape
            # Make the columns wider for clarity.
            for (col_name, col_data) in df.iteritems():
                col_index = df.columns.get_loc(col_name)
                max_len = col_data.map(lambda x: len(str(x))).max()
                max_len = max(max_len, len(col_name))
                worksheet.set_column(col_index, col_index, max_len + 6)
            # Set the autofilter.
            worksheet.autofilter(0, 0, max_row, max_col - 1)

def tidelxml(xfile):
    print(xfile)
    df = pd.read_xml(xfile)
    df = df[['TransactionNumber', 'AssociatedTransactionNumber', 'Code', 'Type',
             'TimeStamp', 'BusinessDate', 'AccountingPeriod', 'MachineId', 'Items',
             'LongDescription', 'RebootRequest', 'ApplicationVersion', 'UserName',
             'UserGroups', 'OwnerName', 'DeviceId', 'Device', 'DepartmentName',
             'RegisterName', 'TillID', 'DoorEvent', 'DeviceErrorEvent',
             'ClearErrorEvent', 'PreExchange', 'Number', 'MiscItems', 'VaultFundId']]
    df['TimeStamp'] = pd.to_datetime(df['TimeStamp'])

    time = df.iloc[0]['TimeStamp'].strftime('%Y-%m-%d')
    mID = df.iloc[0]['MachineId']

    df_user = df.loc[df.Type == 'TillCheckout'].copy()
    df_user = df_user[["TillID", 'OwnerName']]
    df_user = df_user.append({'TillID': 81, 'OwnerName': 'FL81'}, ignore_index=True)
    df_user = df_user.append({'TillID': 82, 'OwnerName': 'FL82'}, ignore_index=True)
    df_user = df_user.append({'TillID': 83, 'OwnerName': 'FL83'}, ignore_index=True)
    df_user = df_user.append({'TillID': 84, 'OwnerName': 'FL84'}, ignore_index=True)

    df = df[['TransactionNumber', 'AssociatedTransactionNumber', 'Code', 'Type',
             'TimeStamp', 'BusinessDate', 'AccountingPeriod', 'MachineId', 'Items',
             'LongDescription', 'RebootRequest', 'ApplicationVersion', 'UserName',
             'UserGroups', 'DeviceId', 'Device', 'DepartmentName',
             'RegisterName', 'TillID', 'DoorEvent', 'DeviceErrorEvent',
             'ClearErrorEvent', 'PreExchange', 'Number', 'MiscItems', 'VaultFundId']]

    df = pd.merge(df, df_user, on='TillID', how='left')


    df_till = df[df.TillID.notnull()]
    df_till = df_till[['Type', 'TimeStamp', 'LongDescription', 'TillID', 'OwnerName']]
    df_till = df_till.loc[df['Type'].isin(['TillCheckout', 'VaultDrop', 'TillCheckin', 'AdvanceCash', 'CashPickup'])]


    def etl_description(x):
        print(x)
        x = x.replace(",", "")
        if 'TillCheckout' in x or 'AdvanceCash' in x:
            x = re.search("\$[0-9]\d*(\.\d\d)?(?![\d.])", x)
            return '-' + x[0]
        elif 'TillCheckin' in x or 'CashPickup' in x:
            x = re.search("\$[0-9]\d*(\.\d\d)?(?![\d.])", x)
            if x:
                return x[0]
            return "$0.00"
        elif 'VaultDrop' in x:
            y = re.search("\$[0-9]\d*(\.\d\d)?(?![\d.])", x)
            z = re.search("(?<=Number).*$", x)
            if y and z:
                return z[0] + ' ' + y[0]
            elif y:
                return 'error' + y[0]
            else:
                return 'Invalid $0.00'
        else:
            return 'Erro';


    df_till['LongDescription'] = df_till['LongDescription'].apply(etl_description)

    df_till.sort_values(by=['TillID', 'TimeStamp'])



    df_net = df_till.copy()
    df_net['LongDescription'] = df_net['LongDescription'].apply(lambda x: x.replace("$", ""))
    df_net.loc[df_net.Type == 'VaultDrop', 'LongDescription'] = df_net.loc[
        df_net.Type == 'VaultDrop', 'LongDescription'].apply(lambda x: x.split()[1])
    df_net['LongDescription'] = pd.to_numeric(df_net['LongDescription'], errors='coerce')
    df_net = df_net.groupby('OwnerName')['LongDescription'].sum().reset_index()

    df_bag = df_till.loc[df_till.Type == 'VaultDrop'].copy()
    df_bag['LongDescription'] = df_bag['LongDescription'].str.strip()
    df_bag[['BagID', 'Amount']] = df_bag.LongDescription.str.split(' ', 2,expand=True)
    df_bag['BagID'] = df_bag['BagID'].str.lstrip('0')
    df_bag = df_bag[['BagID', 'Amount', 'OwnerName', 'TillID', 'TimeStamp', 'Type']]
    df_bag = df_bag.sort_values(by=['BagID'])

    df_addloan = df_till.loc[df['Type'].isin(['AdvanceCash', 'CashPickup'])]

    df_vault = df.loc[df['Type'].isin(["VaultFundTransferFrom", "VaultFundAddCash", "VaultFundContent"])]
    df_vault = df_vault[['TimeStamp', 'Type', 'LongDescription']]
    df_vault['LongDescription'] = df_vault['LongDescription'].apply(lambda x: x.split('External Vault')[1].strip())


    def etl_vault(x):
        x = x.replace(",", "")
        if 'VaultFundTransferFrom' in x:
            x = re.search("\$[1-9]\d*(\.\d\d)?(?![\d.])", x)
            return '-' + x[0]
        else:
            x = re.search("\$[1-9]\d*(\.\d\d)?(?![\d.])", x)
            if x:
                return x[0]
            else:
                return 0


    df_vault['Total'] = df_vault['LongDescription'].apply(etl_vault)

    df_vault = df_vault[["TimeStamp", "Type",  "Total", "LongDescription"]]



    df_list = {

        "Net Cashier": df_net,
        "Detailed Cashier": df_till,
        "Vault Drop Bags": df_bag,
        "Loan | Adv": df_addloan,
        "External Vault": df_vault,
        "Raw": df
    }

    dataframes2xls(df_list, mID +' ' + time, './')


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    myFiles = glob.glob(f'*TransactionXML.xml')
    for afile in myFiles:
        # Read in the file
        with open(afile, 'r') as file:
            filedata = file.read()
        # Replace the target string
        filedata = filedata.replace('End of Report', '')
        # Write the file out again
        with open(afile, 'w') as file:
            file.write(filedata)
        tidelxml(afile)

# See PyCharm help at https://www.jetbrains.com/help/pycharm/
