import os
from pickle import APPEND
import xml.etree.ElementTree as ET
import pyodbc 
from datetime import datetime
import datetime
import gc
from config import get_database_connection,load_env
import logging
import requests
import traceback
<<<<<<< HEAD

logging.basicConfig(filename='app.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
=======
from worldcheckCsv import populate_world_check_csv
logging.basicConfig(filename='app.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
logging.basicConfig(filename='app.log', level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

>>>>>>> 647eb968eb4e10cef06b6001dcd2dc8f24b9b5d6

class OtherIdRegDoc:
    def __init__(self):
        self._idDoc = None
    
    # Setter 
    def set_idDoc(self, idDoc):
        self._idDoc = idDoc
    # Getter 
    def get_idDoc(self):
        return self._idDoc

class Title:
    def __init__(self, root):
        self._title = None
        designation = root.find('.//position')
        self._title = designation.text
        
    # Getter 
    def get_designation(self):
        return self._title
    # Setter 
    def set_designation(self, title):
        self._title = title
        
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
        self.UpdatedDate = None
        self.ServiceLogId = None
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
    
    
# class lstWatchlistLookup:
#     def __init__(self):
#         self.ListTypeID = None
#         self.EntityTypeID = None
#         self.EntityID = None
#         self.EntryTypeID = None
#         self.LUValue = None
#     def __init__(self):
#         self.ListTypeID = None
#         self.EntityTypeID = None
#         self.EntityID = None
#         self.EntryTypeID = None
#         self.LUValue = None

def get_aliaseName(name_value):
    try:
        fullname=""
        for child in name_value:
            name=child.text
            fullname+=name + ' '
                       
        full_name=fullname.rstrip(" ")
        return full_name
    except Exception as e:
        logging.error(e)
        traceback.print_exc()
        return None
    
def get_status(root):
    try:
        status = root.find('ActiveStatus')
        if status.text == 'Active':
            status='A'
        else:
            status='I'
        return status
       
    except Exception as e:
        logging.error(e)
        traceback.print_exc()
        return None
    
def get_actualName(root):  
    # title = Title()
    try:
        first_name = root.find('.//first_name').text
        last_name = root.find('.//last_name').text
        actual_name = f"{first_name} {last_name}"
        return actual_name
    except Exception as e:
        logging.error(e)
        traceback.print_exc()
        return None
    
def get_yearOfBirth(root):
    dob_element = root.find('.//dob')
    dob_text = dob_element.text if dob_element is not None else None

    if dob_text:
        try:
            year_of_birth = int(dob_text.split('-')[0])
        except ValueError as e:
            logging.error(e)
            traceback.print_exc()
            print("Error parsing date of birth:", dob_text)
            year_of_birth = None
    else:
        year_of_birth = None

    return year_of_birth
    
def get_countryToken(root):
    countries = ""
    country_element = root.find('.//countries')
    if country_element is not None:
        country_list = country_element.findall('country')
        country_count = len(country_list)
        
        for i, country in enumerate(country_list):
            countries += country.text
            if i < country_count - 1:
                countries += ' | '

    return countries

        
    
def get_Title(root):
    try:
        #concatenated_titles="" in concatentedTitles only title text is inserting 
        title=""
        # previous_roles = root.findall(".//Roles[@RoleType='Previous Roles']")
        previous_roles = root.find(".//title")
        # for roles in previous_roles:
        #     for occ_title in roles.findall("OccTitle"):
        #         attributes = occ_title.attrib
        #         tag_text = occ_title.text
        #         result += ', '.join([f"{attr}={value}" for attr, value in attributes.items()]) + f", {tag_text}"
                #concatenated_titles += occ_title.text + ", "
        
        #concatenated_titles = concatenated_titles.rstrip(", ")
        title= previous_roles.text
        return title
    except Exception as e:
        logging.error(e)
        traceback.print_exc()
        return None
    
    
def getEntityTypeWCHECK(root):
    EntityType = 1
    ETypeElement = root.find('./person')
    if ETypeElement.attrib.get('e-i') == 'e':
        EntityType = 2
        
    return EntityType

def getPepStatus(root):
    StatusElement = root.find('.//pep_status')
    PepStatus = StatusElement.text
    return PepStatus

def getPepRoles(root):
    result = ""
    pep_role_details = root.findall('.//pep_role_detail')
    
    for detail in pep_role_details:
        for element in detail:
            if element.tag not in ["pep_role_term_start_date", "pep_role_term_end_date"]:
                result += f'{element.tag}: {element.text} ; '

    return result

def query_execute(queries,TenantValue):
        conn1 = get_database_connection(TenantValue)
        cursor = conn1.cursor()
        for query in queries:
            cursor.execute(f"{query}")
        conn1.commit()
        cursor.close
        conn1.close()  
          
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
                IDRegDoc, CreatedDate, Status, YearOfBirth, WLCategory
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
    
def getEnteredDate(root):
    entered_date_str = root.attrib.get('entered', '')  # Use a default value of '' if the attribute is missing
    if entered_date_str.strip():  # Check if the string is not empty or just whitespace
        entered_date = datetime.datetime.strptime(entered_date_str, '%Y-%m-%d')
        return entered_date
    else:
        return None  # 

def getUpdatedDate(root):
    updated_date_str = root.attrib.get('updated', '')  # Use a default value of '' if the attribute is missing
    if updated_date_str.strip(): 
        updated_date = datetime.datetime.strptime(updated_date_str, '%Y-%m-%d')
        return updated_date
    else:
        return None

def chunk_array(input_list, chunk_size):
    return [input_list[i:i + chunk_size] for i in range(0, len(input_list), chunk_size)]
    
    
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
        

def call_nameDerivativesApi(LogId,TenantID):
    # Define the API endpoint URL
    load_env(TenantID)
    url=os.environ.get("NAME_DERIVATIVE_API")
    api_url = f"{url}{LogId}"
<<<<<<< HEAD

=======
    
>>>>>>> 647eb968eb4e10cef06b6001dcd2dc8f24b9b5d6
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

def populate_world_check(dir_list, ListID, LogID, path, TenantValue, ArchPath, ErrorPath, DB, VersionMSSQL):
    count = 0
    filecount = 0

    try:
        for file in dir_list:
            finalList = []
            listLookup = []
            updatedIds = []
            filecount += 1
            recordcount=0
            print(filecount)
            xml_file_path = os.path.join(path, file)
            
            context = ET.iterparse(xml_file_path, events=('start', 'end'))
            context = iter(context)
            event, root = next(context)

            conn1 = get_database_connection(TenantValue)
            cursor = conn1.cursor()

            query = "SELECT COUNT(*) FROM Watchlist.listNew WHERE ListType = 11"
            cursor.execute(query)
            count_result = cursor.fetchone()[0]
<<<<<<< HEAD
            # LogID = get_logId(TenantValue,DB,VersionMSSQL)     #right??
            for record in root:
                pep_status = record.find('.//pep_role_details/pep_status')
                #pep_status = getPepStatus(record)
                entered_date = getEnteredDate(record)
                updated_date = getUpdatedDate(record)
                todays_date = datetime.datetime.now().date() 
                # print(f"pep_status: {pep_status.text.lower()}")
                # print(f"entered_date: {entered_date.date()}")
                # print(f"updated_date: {updated_date.date()}")
                # print(f"todays_date: {todays_date}")
                if (pep_status.text is not None and pep_status.text.lower() == 'active'):
                    if ((entered_date is not None and entered_date.date() == todays_date) or (updated_date is not None and updated_date.date() == todays_date)) or count_result==0:      #todays_date ki jaga bas maxdate utha lo from config_listtype table and == ki jaga > krdo
=======
            cursor.execute("SELECT CONVERT(DATE, MAX(lastsuccessupdated)) FROM Config.ListType WHERE typeid = 11")
            date_to_compare = cursor.fetchone()[0]
            if date_to_compare is not None:
                date_to_compare = datetime.datetime.strptime(date_to_compare, "%Y-%m-%d").date()
            
            for event, record in context:
                if event == 'end' and record.tag == 'record':
                    recordcount+=1
                    if recordcount%100000==0:
                        print(f'{recordcount} records parsed')
                    pep_status = record.find('.//pep_role_details/pep_status')
                    entered_date = getEnteredDate(record)
                    updated_date = getUpdatedDate(record)
                    todays_date = datetime.datetime.now().date()
                    if (pep_status.text is not None and pep_status.text.lower() == 'active'):
                    #IF DATE_TO_COMPARE IS NOT NONE THEN UNCOMMENT BELOW CONDITION
                    #if ((entered_date is not None and entered_date.date() >= date_to_compare) or (updated_date is not None and updated_date.date() >= date_to_compare)) or count_result==0:
>>>>>>> 647eb968eb4e10cef06b6001dcd2dc8f24b9b5d6
                        customer = lstWatchlistNew()
                        customer.UniqueRefNo=record.attrib.get('uid')
                        customer.EntityID=record.attrib.get('uid')
                        logging.info(f'Parsing record number: {recordcount} UID: {customer.EntityID}')
                        customer.ListType=ListID
                        customer.ServiceLogId = LogID
                        customer.EntityType=getEntityTypeWCHECK(record)
                        customer.CreatedDate = getEnteredDate(record)
                        customer.UpdatedDate = updated_date 
                        customer.CountryToken=get_countryToken(record)
                        customer.IDRegDoc= ""
                        customer.WLCategory=None
                        customer.Status= 'A'
                        customer.YearOfBirth=get_yearOfBirth(record)    #check for multiple dobs
                        customer.ActualName=get_actualName(record)

                        #else:
                            #continue  
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
                        title=Title(record)
                        lstLookupRow.ListTypeID=ListID
                        lstLookupRow.EntityTypeID=customer.EntityType
                        lstLookupRow.EntityID=customer.EntityID
                        lstLookupRow.EntryTypeID=11
                        if  get_Title(record) is not None:
                            lstLookupRow.LUValue= f'Title: {get_Title(record)}  Position: {title.get_designation()}'
                        else:
                            lstLookupRow.LUValue= f'Title: NULL  Position: {title.get_designation()}'

                        listLookup.append(lstLookupRow)
                        title.set_designation("")

                        #for Other information / Comments
                        lstLookupRow=lstWatchlistLookup()
                        lstLookupRow.ListTypeID=ListID
                        lstLookupRow.EntityTypeID=customer.EntityType
                        lstLookupRow.EntityID=customer.EntityID
                        lstLookupRow.EntryTypeID=14
                        lstLookupRow.LUValue= record.find('.//further_information').text
                        listLookup.append(lstLookupRow)

                        #for PEP status
                        lstLookupRow=lstWatchlistLookup()
                        lstLookupRow.ListTypeID=ListID
                        lstLookupRow.EntityTypeID=customer.EntityType
                        lstLookupRow.EntityID=customer.EntityID
                        lstLookupRow.EntryTypeID=15
                        lstLookupRow.LUValue=getPepStatus(record)
                        listLookup.append(lstLookupRow)

                        #for PEP role Details
                        lstLookupRow=lstWatchlistLookup()
                        lstLookupRow.ListTypeID=ListID
                        lstLookupRow.EntityTypeID=customer.EntityType
                        lstLookupRow.EntityID=customer.EntityID
                        lstLookupRow.EntryTypeID=16
                        lstLookupRow.LUValue=getPepRoles(record)
                        listLookup.append(lstLookupRow)


                            # FOR NAME ALIASES
                        for nameAlias in record.find('.//aliases'):
                            if nameAlias is not None:
                                customer = lstWatchlistNew()
                                customer.UniqueRefNo=record.attrib.get('uid')
                                customer.EntityID=record.attrib.get('uid')
                                customer.ListType=ListID
                                customer.EntityType=getEntityTypeWCHECK(record)
                                customer.CreatedDate = getEnteredDate(record)
                                customer.CountryToken=get_countryToken(record)
                                customer.IDRegDoc= ""
                                customer.WLCategory=None
                                customer.ActualName=nameAlias.text
                                customer.YearOfBirth=get_yearOfBirth(record)   #check for multiple dobs
                                customer.Status= 'A'
                                finalList.append(customer)

                        #if condition here for incremental records updation/insertion?
                        if updated_date is not None and updated_date.date() == todays_date:
                            updatedIds.append(record.attrib.get('uid'))
                        record.clear()
                        gc.collect()
                        root.clear()

                    else:
                        continue
                    # Inserting watchlistAndLookup
            context = None
            logging.info("Parsing complete")      
            print("Parsing complete")
            if len(finalList) > 0:
                my_list = finalList
                chunk_size = 1000
                chunks = chunk_array(my_list, chunk_size)
                if count_result == 0: 
                    for i, chunk in enumerate(chunks):
                        bulk_insert_watchlist(chunk, TenantValue, DB, VersionMSSQL)
                else:
                    if len(updatedIds) != 0:
                        bulk_update_records(updatedIds, TenantValue, DB, VersionMSSQL)
                my_list = finalList       
                for i, chunk in enumerate(chunks):
                    bulk_insert_watchlist(chunk, TenantValue, DB, VersionMSSQL)
                logging.info("Data Inserted in Watchlist")
                print("Data Inserted in Watchlist")  
                query_execute([f"DELETE FROM Watchlist.LookupTable WHERE ListTypeID = '{ListID}'"] if DB == 'MSSQL' and VersionMSSQL == 'OLD' else [f"DELETE FROM Watchlist_LookupTable WHERE ListTypeID = '{ListID}'"], TenantValue)
                bulk_insert_lookup(listLookup, LogID, TenantValue, DB, VersionMSSQL) 
                query_execute(["exec Watchlist.MergeLookups;"] if DB == 'MSSQL' else ["Call MergeLookups()"], TenantValue)
                target_dir = ArchPath
                target_file_path = os.path.join(target_dir, file)  # Full path to the target file
                xml_file_path = os.path.join(path, file)
                os.replace(xml_file_path, target_file_path)
            logging.info("Running Sp")
            print("Running Sp")
            query_execute(["EXEC Watchlist.ListImport;"],TenantValue)if DB=='MSSQL' and VersionMSSQL=='OLD' else call_nameDerivativesApi(LogID,TenantValue)
            if DB=='MSSQL' and VersionMSSQL=='OLD': 
                    query=["Update Config.ListType set ListStatus = 'Uploaded Successfully', FileStatus = 'S' where TypeID = '"+str(ListID)+"'","Update Config.sanctionlistlogs set ListStatus = 'Uploaded Successfully' where id = (Select Top 1 id from Config.sanctionlistlogs where TypeID ='"+str(ListID)+"' order by id desc)"] 
            elif DB=='MSSQL' and VersionMSSQL=='NEW':
                    query=["Update Config_ListType set ListStatus = 'Uploaded Successfully', FileStatus = 'S' where TypeID = '"+str(ListID)+"'","Update Config_sanctionlistlogs set ListStatus = 'Uploaded Successfully' where id = (Select Top 1 id from Config_sanctionlistlogs where TypeID ='"+str(ListID)+"' order by id desc)"]
            else:
                    query=["Update Config_ListType set ListStatus = 'Uploaded Successfully', FileStatus = 'S' where TypeID = '"+str(ListID)+"'","UPDATE Config_sanctionlistlogs AS c JOIN (SELECT id FROM Config_sanctionlistlogs WHERE TypeID = '"+str(ListID)+"' ORDER BY id DESC LIMIT 1) AS sub ON c.id = sub.id SET c.ListStatus = 'Uploaded Successfully'"]
            query_execute(query,TenantValue)

    except Exception as e:
<<<<<<< HEAD
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
            xml_file_path=os.path.join(path, file)
            os.replace(xml_file_path, target_file_path)
=======
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
        xml_file_path=os.path.join(path, file)
        #os.replace(xml_file_path, target_file_path)
            
>>>>>>> 647eb968eb4e10cef06b6001dcd2dc8f24b9b5d6
def get_logId(TenantValue,DB,VersionMSSQL):
    if DB=='MSSQL' and VersionMSSQL=='OLD':
        insert_query = """
        INSERT INTO Service.Logs (StartDate, EndDate, ExecutedBy, ExecutedFrom)
        VALUES (GETDATE(), NULL, 2, 7);
    """
    elif DB=='MSSQL' and VersionMSSQL=='NEW':
        insert_query = """
        INSERT INTO Service_Logs (StartDate, EndDate, ExecutedBy, ExecutedFrom)
        VALUES (GETDATE(), NULL, 2, 7);
    """  
    else:
        insert_query = """
        INSERT INTO Service_Logs (StartDate, EndDate, ExecutedBy, ExecutedFrom)
        VALUES (NOW(), NULL, 2, 7);
    """

    # Define the SQL query for the SELECT
    select_query = """
        SELECT SCOPE_IDENTITY() AS InsertedID;
    """ if DB=='MSSQL' else """SELECT LAST_INSERT_ID() AS InsertedID;"""


    # Connect to the database and execute the INSERT query
    connection = get_database_connection(TenantValue)
    cursor = connection.cursor()
    cursor.execute(insert_query)
    connection.commit()

    # Execute the SELECT query and fetch the inserted ID
    cursor.execute(select_query)
    inserted_id = cursor.fetchone().InsertedID if DB=='MSSQL' else cursor.fetchone()[0]
    print("Inserted ID:", inserted_id)

    # Close the cursor and connection
    cursor.close()
    connection.close()
    return inserted_id

def update_logId(TenantValue,DB,VersionMSSQL,LogId):
    if DB=='MSSQL' and VersionMSSQL=='OLD':
        update_query = f"""
        UPDATE SERVICE.LOGS SET ENDDATE = GETDATE() WHERE EXECUTEDFROM = 7 AND LOGID ={LogId}
    """
    elif DB=='MSSQL' and VersionMSSQL=='NEW':
        update_query = f"""
        UPDATE SERVICE_LOGS SET ENDDATE = GETDATE() WHERE EXECUTEDFROM = 7 AND LOGID ={LogId}
    """  
    else:
        update_query = f"""
        UPDATE SERVICE.LOGS SET ENDDATE = NOW() WHERE EXECUTEDFROM = 7 AND LOGID ={LogId}
    """

    # Connect to the database and execute the INSERT query
    connection = get_database_connection(TenantValue)
    cursor = connection.cursor()
    cursor.execute(update_query)
    connection.commit()
    cursor.close()
    connection.close()

def Parse_WorldCheck(TenantValue):
    # Get the list of all files and directories
    load_env(TenantValue)   #picks sql.env or mySql.env based on value provided in api header
    Path=os.environ.get("FILE_PATH")
    ArchPath=os.environ.get("ARCHIEVE_PATH")
    ErrorPath=os.environ.get("ERROR_PATH")
    DB=os.environ.get("DB_DRIVER_NAME")
    VersionMSSQL=os.environ.get("SERVICE_VERSION")
    File_Type= os.environ.get("FILE_TYPE", "csv")
    conn = get_database_connection(TenantValue)
    cursor = conn.cursor()
    path =  Path
    #"D://DowJones//Factiva_PFA_Feed_XML//"
    dir_list = os.listdir(path)
    type_key = 'WCHECK'
    query = f"SELECT TypeId FROM config.listtype WHERE TypeKey = '{type_key}'" if DB=="MSSQL" and VersionMSSQL=="OLD" else f"SELECT TypeId FROM config_listtype WHERE TypeKey = '{type_key}'"
    cursor.execute(query)
    row= cursor.fetchone()
    conn.commit()    
    cursor.close()
    conn.close() 
    ListID = row.TypeId if DB=='MSSQL' else row[0]
    if len(dir_list) != 0:  
    #For Worldcheck Parsing
        try:
            if DB=='MSSQL' and VersionMSSQL=='OLD': 
                query=["Update Config.ListType set ListStatus = 'In Progress', FileStatus = 'I' where TypeID = '"+str(ListID)+"'","Update Config.sanctionlistlogs set ListStatus = 'In Progress' where id = (Select Top 1 id from Config.sanctionlistlogs where TypeID ='"+str(ListID)+"' order by id desc)"] 
            elif DB=='MSSQL' and VersionMSSQL=='NEW':
                query=["Update Config_ListType set ListStatus = 'In Progress', FileStatus = 'I' where TypeID = '"+str(ListID)+"'","Update Config_sanctionlistlogs set ListStatus = 'In Progress' where id = (Select Top 1 id from Config_sanctionlistlogs where TypeID ='"+str(ListID)+"' order by id desc)"]
            else:
                query=["Update Config_ListType set ListStatus = 'In Progress', FileStatus = 'I' where TypeID = '"+str(ListID)+"'","UPDATE Config_sanctionlistlogs AS c JOIN (SELECT id FROM Config_sanctionlistlogs WHERE TypeID = '"+str(ListID)+"' ORDER BY id DESC LIMIT 1) AS sub ON c.id = sub.id SET c.ListStatus = 'In Progress'"]
            query_execute(query,TenantValue)
            LogID=get_logId(TenantValue,DB,VersionMSSQL)
            logging.debug('WorldCheck Import Start') 
            if File_Type=="csv":
                populate_world_check_csv(dir_list,ListID,LogID,path,TenantValue,ArchPath,ErrorPath,DB,VersionMSSQL)
            else:
                populate_world_check(dir_list,ListID,LogID,path,TenantValue,ArchPath,ErrorPath,DB,VersionMSSQL)
            logging.debug('WorldCheck Import END')
            update_logId(TenantValue,DB,VersionMSSQL,LogID)
            return True
        except Exception as e:
                traceback.print_exc()
                logging.error(e)
                return False
                
    else:
        logging.error('File Not Found')