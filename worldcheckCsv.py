import os
from pickle import APPEND
import xml.etree.ElementTree as ET
import pyodbc 
from datetime import datetime
from config import get_database_connection,load_env
import logging
import requests
import traceback
import re
import time
from datetime import date
import csv
logging.basicConfig(filename='app.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class lstWatchlistNew:
    def __init__(self):
        self.UniqueRefNo = None
        self.ListType = None
        self.EntityType = None
        self.EntityID = None
        self.ActualName = None
        self.CountryToken = None
        self.IDRegDoc = None
        self.CreatedDate = None
        # self.ServiceLogId = None
        self.Status = None
        self.YearOfBirth = None
        self.WLCategory = None
        self.WatchlistLookup = []
    def clone(self):
        return self 

class lstWatchlistLookup:
    def __init__(self):
        self.ListTypeID = None
        self.EntityTypeID = None
        self.EntityID = None
        self.EntryTypeID = None
        self.LUValue = None
        
def chunk_array(input_list, chunk_size):
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]

def bulk_insert_watchlist(finalList,TenantValue,DB,VersionMSSQL):
    try:
        connWB = get_database_connection(TenantValue)
        cursor = connWB.cursor()

        Watchlistdata_to_insert = [
            (
                item.UniqueRefNo, item.ListType, item.EntityType, item.EntityID,
                item.ActualName, item.CountryToken, item.IDRegDoc, item.CreatedDate,
                item.UpdatedDate, item.ServiceLogId, item.Status, item.YearOfBirth, item.WLCategory
            )
            for item in finalList
        ]
        if DB=='MSSQL'and VersionMSSQL=='OLD':
            insert_query_watchlist = '''
            INSERT INTO watchlist.ListNew (
                UniqueRefNo, ListType, EntityType, EntityID, ActualName, CountryToken,
                IDRegDoc, CreatedDate, UpdatedDate, ServiceLogId, Status, YearOfBirth, WLCategory
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        elif DB=='MSSQL'and VersionMSSQL=='NEW':
            insert_query_watchlist = '''
            INSERT INTO watchlist_ListNew (
                UniqueRefNo, ListType, EntityType, EntityID, ActualName, CountryToken,
                IDRegDoc, CreatedDate, UpdatedDate, ServiceLogId, Status, YearOfBirth, WLCategory
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''  
        else:
            insert_query_watchlist ='''INSERT INTO watchlist_ListNew (
                UniqueRefNo, ListType, EntityType, EntityID, ActualName, CountryToken,
                IDRegDoc, CreatedDate, Status, YearOfBirth, WLCategory
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
        
        cursor.executemany(insert_query_watchlist, Watchlistdata_to_insert)
        connWB.commit()
        cursor.close()
        connWB.close()
    except Exception as e:
        logging.error(e)
        traceback.print_exc()
        print(f'An Error Occured: {e}')

def bulk_update_records(ids,TenantValue,DB,VersionMSSQL):
    try:
        connWB = get_database_connection(TenantValue)
        cursor = connWB.cursor()
        if DB=='MSSQL'and VersionMSSQL=='OLD':
          sql = "UPDATE watchlist.listnew SET Status = ? WHERE entityid IN ({})".format(', '.join('?' for _ in ids))
        elif DB=='MSSQL'and VersionMSSQL=='OLD':
          sql = "UPDATE watchlist_listnew SET Status = ? WHERE entityid IN ({})".format(', '.join('?' for _ in ids))
        else:
            sql = "UPDATE watchlist_listnew SET Status = %s WHERE entityid IN ({})".format(', '.join('%s' for _ in ids))
        # Specify the new value to column
        new_value = 'I'
        # Execute the query with the list of IDs as parameters
        cursor.execute(sql, [new_value] + ids)  # Use [new_value] + ids to prepend the new_value to the list of IDs
        connWB.commit()
        cursor.close()
        connWB.close()
    except Exception as e:
        logging.error(e)
        traceback.print_exc()
        print(f'An Error Occured: {e}')
        return False

def query_execute(queries,TenantValue):
        conn1 = get_database_connection(TenantValue)
        cursor = conn1.cursor()
        for query in queries:
            cursor.execute(f"{query}")
        conn1.commit()
        cursor.close
        conn1.close()  
        
def call_nameDerivativesApi(LogId,TenantID):
    # Define the API endpoint URL
    load_env(TenantID)
    url=os.environ.get("NAME_DERIVATIVE_API")
    api_url = f"{url}{LogId}"
    try:
        headers = {
        'TenantId': TenantID,  # Headers
        }
        # Send a GET request to the API
        response = requests.get(api_url,headers=headers)
        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            print("API Response:", response.status_code)
        else:
            print("Name derivatives Request failed with status code:", response.status_code)

    except requests.exceptions.RequestException as e:
        logging.error(e)
        traceback.print_exc()
        # Handle exceptions such as network errors
        print("Name derivatives Request error:", str(e))
        
def bulk_insert_lookup(finalList,LogID,TenantValue,DB,VersionMSSQL):
    try:
        connWB = get_database_connection(TenantValue)
        cursor = connWB.cursor()

        Lookupdata_to_insert = [
    (
       finalListRow.ListTypeID, finalListRow.EntityTypeID, finalListRow.EntityID, finalListRow.EntryTypeID,finalListRow.LUValue,LogID
    )
    for finalListRow in finalList]
        if DB=='MSSQL' and VersionMSSQL =='OLD':
            insert_query_lookup = "INSERT INTO Watchlist.LookupTable (ListTypeID, EntityTypeID, EntityID, EntryTypeID, LUValue,LogID) VALUES (?, ?, ?, ?, ?, ?)"
        elif DB=='MSSQL' and VersionMSSQL =='NEW':
            insert_query_lookup = "INSERT INTO Watchlist_LookupTable (ListTypeID, EntityTypeID, EntityID, EntryTypeID, LUValue,LogID) VALUES (?, ?, ?, ?, ?, ?)" 
        else: 
            insert_query_lookup ="INSERT INTO Watchlist_LookupTable (ListTypeID, EntityTypeID, EntityID, EntryTypeID, LUValue,LogID) VALUES (%s, %s, %s, %s, %s, %s)"
        
        cursor.executemany(insert_query_lookup, Lookupdata_to_insert)
        connWB.commit()
        cursor.close()
        connWB.close()
    except Exception as e:
        logging.error(e)
        traceback.print_exc()
        print(f'An Error Occured: {e}')
        

def populate_world_check_csv(dir_list,ListID,LogID,path,TenantValue,ArchPath,ErrorPath,DB,VersionMSSQL):    
    count = 0
    filecount = 0
    load_env('sql')  
    Check_Pep_Status = os.environ.get("CHECK_PEP_STATUS")
    try:
        for file in dir_list:
            finalList = []
            #lstWatchlistNew()
            listLookup=[] 
            updatedIds=[] 
            filecount = filecount + 1
            print(filecount)
            csv_file_path = os.path.join(path, file)
            conn1 = get_database_connection(TenantValue)       
            cursor = conn1.cursor()
            query = "SELECT COUNT(*) FROM Watchlist.listNew WHERE ListType = 11"
            cursor.execute(query)
            count_result = cursor.fetchone()[0]
            query = "SELECT Country_name, alpha2_code FROM COUNTRIES"
            cursor.execute(query)
            results = cursor.fetchall()
            ignore_headers_flag = 0
            #making dictionary with countries as key and their codes as values
            country_dict = {country_name.upper(): alpha2_code for country_name, alpha2_code in results}
            with open(csv_file_path, mode='r') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter='\t')
                for record in csv_reader:
                    try:
                        if ignore_headers_flag == 0:
                            ignore_headers_flag=ignore_headers_flag+1
                            continue
                        logging.debug(f"Processing record ID: {record[0]}")
                        entered_date = record[23]
                        updated_date = record[24]
                        date_format = "%Y/%m/%d"
                        date_format1 = "%Y-%m-%d"
                        if (entered_date is not None and entered_date!=''): 
                            entered_date = datetime.strptime(entered_date, date_format).date() #handle empty dates
                        if (updated_date is not None and updated_date!=''):
                            updated_date = datetime.strptime(updated_date, date_format).date() 
                        todays_date = datetime.now().date()
                        cursor.execute("SELECT CONVERT(DATE, MAX(lastsuccessupdated)) FROM Config.ListType WHERE typeid = 11")
                        date_to_compare = cursor.fetchone()[0]
                        date_to_compare = datetime.strptime(date_to_compare, date_format1).date()
                        if Check_Pep_Status==1 and (record[7] is not None and record[7] == 'PEP'): 
                        #if ((entered_date is not None and entered_date.date() == todays_date) or (updated_date is not None and updated_date.date() == todays_date)) or count_result==0:     #todays_date ki jaga bas maxdate utha lo from config_listtype table and == ki jaga > krdo
                            if ((entered_date is not None or entered_date!='') and entered_date >= date_to_compare) or ((updated_date is not None or updated_date!='') and updated_date>= date_to_compare) or count_result==0:
                                #entered_date = datetime.strptime(record[23], '%Y-%m-%d')
                                #updated_date = datetime.strptime(record[24], '%Y-%m-%d')
                                customer = lstWatchlistNew()
                                customer.UniqueRefNo=record[0]
                                customer.EntityID=record[0]
                                customer.ListType=ListID
                                if record[18] == 'E':
                                    customer.EntityType = 2
                                else:
                                    customer.EntityType = 1
                                customer.CreatedDate = datetime.today()
                                country_name = record[16]
                                customer.CountryToken=country_dict.get(country_name, None)
                                try:
                                    IDRegDocArr= record[13].split(';')
                                    cleanIdRegDocArr=[]
                                    for rec in IDRegDocArr:
                                        cleanRec = re.sub(r'\([^)]*\)', '', rec).strip()
                                        cleanRec = ''.join(e for e in cleanRec if (e.isalnum()))
                                        cleanIdRegDocArr.append(cleanRec)
                                    customer.IDRegDoc = '|'.join(cleanIdRegDocArr)
                                except Exception as e:
                                    logging.info("Format issue in record")
                                customer.WLCategory=None
                                customer.Status= 'A'
                                if record[10] is not None and record[10] != '' and '/' in record[10]:
                                    customer.YearOfBirth=int(record[10].split('/')[2])   #only year?
                                else:
                                    customer.YearOfBirth=None
                                customer.ActualName=record[2]+ ' ' +record[1]
                                    
                            else:
                                continue  
                            finalList.append(customer)
                            #() customer.append(person.attrib.get('id')) if person.attrib.get('id') not in customer else customer
                            
                            lstLookupRow=lstWatchlistLookup()
                            lstLookupRow.ListTypeID=ListID
                            lstLookupRow.EntityTypeID=customer.EntityType
                            lstLookupRow.EntityID=customer.EntityID
                            lstLookupRow.EntryTypeID= 1  
                            lstLookupRow.LUValue=customer.ActualName
                            listLookup.append(lstLookupRow)
                                #for Created Date
                            lstLookupRow=lstWatchlistLookup()
                            lstLookupRow.ListTypeID=ListID
                            lstLookupRow.EntityTypeID=customer.EntityType
                            lstLookupRow.EntityID=customer.EntityID
                            lstLookupRow.EntryTypeID=9
                            lstLookupRow.LUValue=customer.CreatedDate
                            listLookup.append(lstLookupRow)
                                #for DOB
                            lstLookupRow=lstWatchlistLookup()
                            lstLookupRow.ListTypeID=ListID
                            lstLookupRow.EntityTypeID=customer.EntityType
                            lstLookupRow.EntityID=customer.EntityID
                            lstLookupRow.EntryTypeID=4
                            lstLookupRow.LUValue=customer.YearOfBirth
                            listLookup.append(lstLookupRow)
                                #for Designation/Titles
                            lstLookupRow=lstWatchlistLookup()
                            title=record[6]
                            lstLookupRow.ListTypeID=ListID
                            lstLookupRow.EntityTypeID=customer.EntityType
                            lstLookupRow.EntityID=customer.EntityID
                            lstLookupRow.EntryTypeID=11
                            if  title is not None and title != '':
                                lstLookupRow.LUValue= f'Title: {title}  Position: {record[8]}'
                            else:
                                lstLookupRow.LUValue= f'Title: NULL  Position: {record[8]}'
                            listLookup.append(lstLookupRow)
                            # title.set_designation("")
                            
                            #for Other information / Comments
                            lstLookupRow=lstWatchlistLookup()
                            lstLookupRow.ListTypeID=ListID
                            lstLookupRow.EntityTypeID=customer.EntityType
                            lstLookupRow.EntityID=customer.EntityID
                            lstLookupRow.EntryTypeID=14
                            lstLookupRow.LUValue= record[20]
                            listLookup.append(lstLookupRow)
                            
                            #for PEP status
                            lstLookupRow=lstWatchlistLookup()
                            lstLookupRow.ListTypeID=ListID
                            lstLookupRow.EntityTypeID=customer.EntityType
                            lstLookupRow.EntityID=customer.EntityID
                            lstLookupRow.EntryTypeID=15
                            if record[7] == 'PEP':
                                lstLookupRow.LUValue='active'
                            else:
                                lstLookupRow.LUValue='inactive'
                            listLookup.append(lstLookupRow)
                            
                            # #for PEP role Details
                            # lstLookupRow=lstWatchlistLookup()
                            # lstLookupRow.ListTypeID=ListID
                            # lstLookupRow.EntityTypeID=customer.EntityType
                            # lstLookupRow.EntityID=customer.EntityID
                            # lstLookupRow.EntryTypeID=16
                            # lstLookupRow.LUValue=getPepRoles(record)
                            # listLookup.append(lstLookupRow)
                                # FOR NAME ALIASES
                            for nameAlias in record[3].split(';'):
                                if nameAlias is not None and nameAlias != '':
                                    customer = lstWatchlistNew()
                                    customer.UniqueRefNo=record[0]
                                    customer.EntityID=record[0]
                                    customer.ListType=ListID
                                    if record[18] == 'E':
                                        customer.EntityType = 2
                                    else:
                                        customer.EntityType = 1
                                    customer.CreatedDate = datetime.today()
                                    customer.CountryToken=country_dict.get(record[16], None)
                                    try:
                                        IDRegDocArr= record[13].split(';')
                                        cleanIdRegDocArr=[]
                                        for rec in IDRegDocArr:
                                            cleanRec = re.sub(r'\([^)]*\)', '', rec).strip()
                                            cleanRec = ''.join(e for e in cleanRec if (e.isalnum()))
                                            cleanIdRegDocArr.append(cleanRec)
                                        customer.IDRegDoc = '|'.join(cleanIdRegDocArr)
                                    except Exception as e:
                                        logging.info("Format issue in record")
                                    customer.WLCategory=None
                                    customer.ActualName=record[2]+ ' ' +record[1]
                                    if record[10] is not None and record[10] != '' and '/' in record[10]:
                                        customer.YearOfBirth=int(record[10].split('/')[2])   #only year?
                                    else:
                                        customer.YearOfBirth=None
                                        customer.Status= 'A'
                                        finalList.append(customer)
                            
                            if updated_date is not None and updated_date == todays_date:
                                updatedIds.append(record[0])
                        else:
                            # print(f"{entered_date}, {type(entered_date)}, {date_to_compare}, {type(date_to_compare)}, {updated_date}, {type(updated_date)}")
                            if ((entered_date is not None and entered_date >= date_to_compare) or (updated_date is not None and updated_date>= date_to_compare)) or count_result==0:
                                customer = lstWatchlistNew()
                                customer.UniqueRefNo=record[0]
                                customer.EntityID=record[0]
                                customer.ListType=ListID
                                if record[18] == 'E':
                                    customer.EntityType = 2
                                else:
                                    customer.EntityType = 1
                                customer.CreatedDate = datetime.today()
                                country_name = record[16]
                                customer.CountryToken=country_dict.get(country_name, None)
                                try:
                                    IDRegDocArr= record[13].split(';')
                                    cleanIdRegDocArr=[]
                                    for rec in IDRegDocArr:
                                        cleanRec = re.sub(r'\([^)]*\)', '', rec).strip()
                                        cleanRec = ''.join(e for e in cleanRec if (e.isalnum()))
                                        cleanIdRegDocArr.append(cleanRec)
                                    customer.IDRegDoc = '|'.join(cleanIdRegDocArr)
                                except Exception as e:
                                    logging.info("Format issue in record")
                                customer.WLCategory=None
                                customer.Status= 'A'
                                if record[10] is not None and record[10] != '' and '/' in record[10]:
                                    customer.YearOfBirth=int(record[10].split('/')[2])   #only year?
                                else:
                                    customer.YearOfBirth=None
                                customer.ActualName=record[2]+ ' ' +record[1]
                                    
                            else:
                                continue  
                            finalList.append(customer)
                            #() customer.append(person.attrib.get('id')) if person.attrib.get('id') not in customer else customer
                            
                            lstLookupRow=lstWatchlistLookup()
                            lstLookupRow.ListTypeID=ListID
                            lstLookupRow.EntityTypeID=customer.EntityType
                            lstLookupRow.EntityID=customer.EntityID
                            lstLookupRow.EntryTypeID= 1  
                            lstLookupRow.LUValue=customer.ActualName
                            listLookup.append(lstLookupRow)
                                #for Created Date
                            lstLookupRow=lstWatchlistLookup()
                            lstLookupRow.ListTypeID=ListID
                            lstLookupRow.EntityTypeID=customer.EntityType
                            lstLookupRow.EntityID=customer.EntityID
                            lstLookupRow.EntryTypeID=9
                            lstLookupRow.LUValue=customer.CreatedDate
                            listLookup.append(lstLookupRow)
                                #for DOB
                            lstLookupRow=lstWatchlistLookup()
                            lstLookupRow.ListTypeID=ListID
                            lstLookupRow.EntityTypeID=customer.EntityType
                            lstLookupRow.EntityID=customer.EntityID
                            lstLookupRow.EntryTypeID=4
                            lstLookupRow.LUValue=customer.YearOfBirth
                            listLookup.append(lstLookupRow)
                                #for Designation/Titles
                            lstLookupRow=lstWatchlistLookup()
                            title=record[6]
                            lstLookupRow.ListTypeID=ListID
                            lstLookupRow.EntityTypeID=customer.EntityType
                            lstLookupRow.EntityID=customer.EntityID
                            lstLookupRow.EntryTypeID=11
                            if  title is not None and title != '':
                                lstLookupRow.LUValue= f'Title: {title}  Position: {record[8]}'
                            else:
                                lstLookupRow.LUValue= f'Title: NULL  Position: {record[8]}'
                            listLookup.append(lstLookupRow)
                            # title.set_designation("")
                            
                            #for Other information / Comments
                            lstLookupRow=lstWatchlistLookup()
                            lstLookupRow.ListTypeID=ListID
                            lstLookupRow.EntityTypeID=customer.EntityType
                            lstLookupRow.EntityID=customer.EntityID
                            lstLookupRow.EntryTypeID=14
                            lstLookupRow.LUValue= record[20]
                            listLookup.append(lstLookupRow)
                            
                            #for PEP status
                            lstLookupRow=lstWatchlistLookup()
                            lstLookupRow.ListTypeID=ListID
                            lstLookupRow.EntityTypeID=customer.EntityType
                            lstLookupRow.EntityID=customer.EntityID
                            lstLookupRow.EntryTypeID=15
                            if record[7] == 'PEP':
                                lstLookupRow.LUValue='active'
                            else:
                                lstLookupRow.LUValue='inactive'
                            listLookup.append(lstLookupRow)
                            
                            # #for PEP role Details
                            # lstLookupRow=lstWatchlistLookup()
                            # lstLookupRow.ListTypeID=ListID
                            # lstLookupRow.EntityTypeID=customer.EntityType
                            # lstLookupRow.EntityID=customer.EntityID
                            # lstLookupRow.EntryTypeID=16
                            # lstLookupRow.LUValue=getPepRoles(record)
                            # listLookup.append(lstLookupRow)
                            
                                # FOR NAME ALIASES
                            for nameAlias in record[3].split(';'):
                                if nameAlias is not None and nameAlias != '':
                                    customer = lstWatchlistNew()
                                    customer.UniqueRefNo=record[0]
                                    customer.EntityID=record[0]
                                    customer.ListType=ListID
                                    if record[18] == 'E':
                                        customer.EntityType = 2
                                    else:
                                        customer.EntityType = 1
                                    customer.CreatedDate = datetime.today()
                                    customer.CountryToken=country_dict.get(record[16], None)
                                    try:
                                        IDRegDocArr= record[13].split(';')
                                        cleanIdRegDocArr=[]
                                        for rec in IDRegDocArr:
                                            cleanRec = re.sub(r'\([^)]*\)', '', rec).strip()
                                            cleanRec = ''.join(e for e in cleanRec if (e.isalnum()))
                                            cleanIdRegDocArr.append(cleanRec)
                                        customer.IDRegDoc = '|'.join(cleanIdRegDocArr)
                                    except Exception as e:
                                        logging.info("Format issue in record")
                                    customer.WLCategory=None
                                    customer.ActualName=record[2]+ ' ' +record[1]
                                    if record[10] is not None and record[10] != '' and '/' in record[10]:
                                        customer.YearOfBirth=int(record[10].split('/')[2])   #only year?
                                    else:
                                        customer.YearOfBirth=None
                                    customer.Status= 'A'
                                    finalList.append(customer)
                            
                            if updated_date is not None and updated_date == todays_date:
                                updatedIds.append(record[0])
                                
                    except Exception as e:
                        logging.info("Format issue in record")
            
                                            
            logging.info("Parsing complete")
            #Inserting watchlistAndLookup
            if len(finalList)>0:
                my_list = finalList
                chunk_size = 500
                chunks = chunk_array(my_list, chunk_size)
                if count_result == 0: 
                    for i, chunk in enumerate(chunks):
                        bulk_insert_watchlist(chunk,TenantValue,DB,VersionMSSQL)
                else:
                    if len(updatedIds)!=0:
                        bulk_update_records(updatedIds,TenantValue,DB,VersionMSSQL)
                        logging.info("Completed bulk update records")
                        
                my_list = finalList       
                for i, chunk in enumerate(chunks):
                    bulk_insert_watchlist(chunk,TenantValue,DB,VersionMSSQL)
                logging.info("Completed bulk insert watchlist")
                    
                query_execute([f"DELETE FROM Watchlist.LookupTable WHERE ListTypeID = '{ListID}'"] if DB=='MSSQL' and VersionMSSQL=='OLD' else [f"DELETE FROM Watchlist_LookupTable WHERE ListTypeID = '{ListID}'"],TenantValue)
                bulk_insert_lookup(listLookup,LogID,TenantValue,DB,VersionMSSQL)
                query_execute(["exec Watchlist.MergeLookups;"]if DB=='MSSQL' else ["Call MergeLookups()"],TenantValue)
                target_dir=ArchPath
                target_file_path = os.path.join(target_dir, file)  # Full path to the target file
                csv_file_path=os.path.join(path, file)
                time.sleep(2)
                os.replace(csv_file_path, target_file_path)
            # if DB=='MSSQL':   
            #     # query_execute(["EXEC Watchlist.ListImport;"],TenantValue)
            #     query_execute(["EXEC Watchlist.MergeLookups;"],TenantValue) #add mergelookups
            #call_nameDerivativesApi(LogID,TenantValue)
            query_execute(["EXEC Watchlist.ListImport;"],TenantValue)if DB=='MSSQL' and VersionMSSQL=='OLD' else call_nameDerivativesApi(LogID,TenantValue)
            logging.info("Called name derivatives API")
            if DB=='MSSQL' and VersionMSSQL=='OLD': 
                    query=["Update Config.ListType set ListStatus = 'Uploaded Successfully', FileStatus = 'S' where TypeID = '"+str(ListID)+"'","Update Config.sanctionlistlogs set ListStatus = 'Uploaded Successfully' where id = (Select Top 1 id from Config.sanctionlistlogs where TypeID ='"+str(ListID)+"' order by id desc)"] 
            elif DB=='MSSQL' and VersionMSSQL=='NEW':
                    query=["Update Config_ListType set ListStatus = 'Uploaded Successfully', FileStatus = 'S' where TypeID = '"+str(ListID)+"'","Update Config_sanctionlistlogs set ListStatus = 'Uploaded Successfully' where id = (Select Top 1 id from Config_sanctionlistlogs where TypeID ='"+str(ListID)+"' order by id desc)"]
            else:
                    query=["Update Config_ListType set ListStatus = 'Uploaded Successfully', FileStatus = 'S' where TypeID = '"+str(ListID)+"'","UPDATE Config_sanctionlistlogs AS c JOIN (SELECT id FROM Config_sanctionlistlogs WHERE TypeID = '"+str(ListID)+"' ORDER BY id DESC LIMIT 1) AS sub ON c.id = sub.id SET c.ListStatus = 'Uploaded Successfully'"]
            query_execute(query,TenantValue)

        
    except Exception as e:
            logging.error(e)
            traceback.print_exc()
            if DB=='MSSQL' and VersionMSSQL=='OLD': 
                query=["Update Config.ListType set ListStatus = 'Upload Failed', FileStatus = 'F' where TypeID = '"+str(ListID)+"'","Update Config.sanctionlistlogs set ListStatus = 'Upload Failed' where id = (Select Top 1 id from Config.sanctionlistlogs where TypeID ='"+str(ListID)+"' order by id desc)"] 
            elif DB=='MSSQL' and VersionMSSQL=='NEW':
                query=["Update Config_ListType set ListStatus = 'Upload Failed', FileStatus = 'F' where TypeID = '"+str(ListID)+"'","Update Config_sanctionlistlogs set ListStatus = 'Upload Failed' where id = (Select Top 1 id from Config_sanctionlistlogs where TypeID ='"+str(ListID)+"' order by id desc)"]
            else:
                query=["Update Config_ListType set ListStatus = 'Upload Failed', FileStatus = 'F' where TypeID = '"+str(ListID)+"'","UPDATE Config_sanctionlistlogs AS c JOIN (SELECT id FROM Config_sanctionlistlogs WHERE TypeID = '"+str(ListID)+"' ORDER BY id DESC LIMIT 1) AS sub ON c.id = sub.id SET c.ListStatus = 'Upload Failed'"]
            query_execute(query,TenantValue)
            target_dir=ErrorPath
            target_file_path = os.path.join(target_dir, file)  # Full path to the target file
            csv_file_path=os.path.join(path, file)
            os.replace(csv_file_path, target_file_path)