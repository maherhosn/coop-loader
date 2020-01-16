import requests
import urllib3 
import bs4
import lxml
import pypyodbc
import time
import sys



KEY = ""
CREATE_LOCATIONS_TBL = "CREATE TABLE Locations(ReferenceID  VARCHAR(64) NOT NULL PRIMARY KEY,Name  VARCHAR(64),Address  VARCHAR(64) NOT NULL,City  VARCHAR(64) NOT NULL,County  VARCHAR(64),State  VARCHAR(64) NOT NULL,PostalCode  VARCHAR(64),Country  VARCHAR(64),Phone  VARCHAR(64),Fax  VARCHAR(64),WebAddress  VARCHAR(64),Latitude  DECIMAL(9,6),Longitude  DECIMAL(9,6),Hours  VARCHAR(64),RetailOutlet  VARCHAR(64),RestrictedAccess  VARCHAR(64),AcceptDeposit  VARCHAR(64),AcceptCash  VARCHAR(64),EnvelopeRequired  VARCHAR(64),OnMilitaryBase  VARCHAR(64),OnPremise  VARCHAR(64),Surcharge  VARCHAR(64),Access  VARCHAR(64),AccessNotes  VARCHAR(64),InstallationType  VARCHAR(64),HandicapAccess  VARCHAR(64),LocationType  VARCHAR(64),HoursMonOpen VARCHAR(200),HoursMonClose     VARCHAR(200),HoursTueOpen      VARCHAR(200),HoursTueClose     VARCHAR(200),HoursWedOpen      VARCHAR(200),HoursWedClose     VARCHAR(200),HoursThuOpen      VARCHAR(200),HoursThuClose     VARCHAR(200),HoursFriOpen      VARCHAR(200),HoursFriClose     VARCHAR(200),HoursSatOpen      VARCHAR(200),HoursSatClose     VARCHAR(200),HoursSunOpen      VARCHAR(200),HoursSunClose     VARCHAR(200),HoursDTMonOpen    VARCHAR(200),HoursDTMonClose   VARCHAR(200),HoursDTTueOpen    VARCHAR(200),HoursDTTueClose   VARCHAR(200),HoursDTWedOpen    VARCHAR(200),HoursDTWedClose   VARCHAR(200),HoursDTThuOpen    VARCHAR(200),HoursDTThuClose   VARCHAR(200),HoursDTFriOpen    VARCHAR(200),HoursDTFriClose   VARCHAR(200),HoursDTSatOpen    VARCHAR(200),HoursDTSatClose   VARCHAR(200),HoursDTSunOpen    VARCHAR(200),HoursDTSunClose   VARCHAR(200),Cashless  VARCHAR(64),DriveThruOnly VARCHAR(64),LimitedTransactions VARCHAR(64),MilitaryIdRequired VARCHAR(64),SelfServiceDevice VARCHAR(64),SelfServiceOnly VARCHAR(64));\n"
'''
Gets Co-Op API key from local txt file. This file contains one line, which is the API key.
'''
def get_key():
    global KEY
    with open('key.txt', 'r') as file:
        KEY = file.read()

'''
Calls Co-Op API.
param: offset; offset as viewed by Co-Op. If offset = 0, result = [1,100]. If offset = 100, result = [101, 200]
return: BeautifulSoup list of locations, number of records if offset = 0, else just locations list.
'''
def _call_api(offset=0):
    r = requests.get('https://api.co-opfs.org/locator/proximitysearch', params={'zip':'98122', 'offset': str(offset)}, headers={'Accept':'application/xml', 'Version':'1', 'Authorization': KEY})
    soup = bs4.BeautifulSoup(r.text, 'xml')
    records_avail = int(soup.find('RecordsAvailable').text)
    print('Records available: ', records_avail)
    locations = soup.find_all('Location')
    if offset == 0: return locations, records_avail
    return locations

'''
Checks if field is empty
param: field (string)
return: True or False
'''
def _is_empty(field):
    if len(field) == 0: return True
    return False

'''
Checks if a field is true or false
precondition: assumes _is_empty has been called on the field and has returned false
param: field (the text of a field)
return: a bit (True = 1, False = 0)
'''
def _is_true(field):
    if field == 'Y': return 1
    return 0
    
'''
Opens file and writes in create table sql statements.
Calls location_insert_driver to insert locations into table
'''
def sql_file_driver():
    with open('sql_startup.txt', 'w+') as file:
        # Create tables
        file.write(CREATE_LOCATIONS_TBL)
        # Write location sql insert statements to file
        sql_statements = _insert_driver()
        for statement in sql_statements:
            file.write(statement)
        file.close()

        conn = pypyodbc.connect('Driver={SQL Server};Server=csp199.cslab.seattleu.edu;Database=testdb;uid=sa;pwd=KoeningMPass2019!')
        cursor = conn.cursor()
         # Execute db statements
        File1 = open('sql_startup.txt', 'r')
        i=0
        for line in File1:
            sqlCommand=line.strip()
            try:
                cursor.execute(sqlCommand)
            # NB : you won't get an IntegrityError when reading
            except (pypyodbc.ProgrammingError, pypyodbc.Error)  as e:
                 print(e)
                 pass
            cursor.commit()
            #print(sqlCommand)
            #time.sleep(1)
            #print(line.strip())




'''
Calls Co-Op API. For now only looks at all locations in one zipcode.
Calls insert_into_locations, insert_into_contact, insert_into_specialqualities.
'''
def _insert_driver():
    offset = 0
    records_avail = -1
    sql_statements = []

    # Call API
    locations, records_avail = _call_api(offset)

    # Check if total records exceed the length of returned locations
    while offset+1 < records_avail:
        for location in locations:
            offset += 1
            sql_statements.append(_insert_into_locations(location) + '\n')
        break
    return sql_statements

'''
Parses location object to create sql insert statement into Locations table
param: location (a single location object)
return: sql insert statement (string)
'''
def _insert_into_locations(location):
    values = []
    values.append(_find_value(location, 'ReferenceID'))
    values.append(_find_value(location, 'Name'))
    values.append(_find_value(location, 'Address'))
    values.append(_find_value(location, 'City'))
    values.append(_find_value(location, 'County'))
    values.append(_find_value(location, 'State'))
    values.append(_find_value(location, 'PostalCode'))
    values.append(_find_value(location, 'Country'))
    values.append(_find_value(location, 'Phone'))
    values.append(_find_value(location, 'Fax'))
    values.append(_find_value(location, 'WebAddress'))
    values.append(_find_value(location, 'Latitude'))
    values.append(_find_value(location, 'Longitude'))
    hours = _find_value(location, 'Hours')
    if hours != '':
        values.append(hours)
    else:
        values.append(_get_daily_hours(location))
    values.append(_find_value(location, 'RetailOutlet'))
    values.append(_find_value(location, 'RestrictedAccess'))
    values.append(_find_value(location, 'AcceptDeposit'))
    values.append(_find_value(location, 'AcceptCash'))
    values.append(_find_value(location, 'EnvelopeRequired'))
    values.append(_find_value(location, 'OnMilitaryBase'))
    values.append(_find_value(location, 'OnPremise'))
    values.append(_find_value(location, 'Surcharge'))
    values.append(_find_value(location, 'Access'))
    values.append(_find_value(location, 'AccessNotes'))
    values.append(_find_value(location, 'InstallationType'))
    values.append(_find_value(location, 'HandicapAccess'))
    type_name = _find_value(location, 'LocationType')
    if type_name == 'A': values.append('ATM')
    elif type_name == 'S': values.append('Shared Branch')
    else: values.append('')
    values.append(_find_value(location, 'HoursMonOpen'))
    values.append(_find_value(location, 'HoursMonClose'))
    values.append(_find_value(location, 'HoursTueOpen'))
    values.append(_find_value(location, 'HoursTueClose'))
    values.append(_find_value(location, 'HoursWedOpen'))
    values.append(_find_value(location, 'HoursWedClose'))
    values.append(_find_value(location, 'HoursThuOpen'))
    values.append(_find_value(location, 'HoursThuClose'))
    values.append(_find_value(location, 'HoursFriOpen'))
    values.append(_find_value(location, 'HoursFriClose'))
    values.append(_find_value(location, 'HoursSatOpen'))
    values.append(_find_value(location, 'HoursSatClose'))
    values.append(_find_value(location, 'HoursSunOpen'))
    values.append(_find_value(location, 'HoursSunClose'))
    values.append(_find_value(location, 'HoursDTMonOpen'))
    values.append(_find_value(location, 'HoursDTMonClose'))
    values.append(_find_value(location, 'HoursDTTueOpen'))
    values.append(_find_value(location, 'HoursDTTueClose'))
    values.append(_find_value(location, 'HoursDTWedOpen'))
    values.append(_find_value(location, 'HoursDTWedClose'))
    values.append(_find_value(location, 'HoursDTThuOpen'))
    values.append(_find_value(location, 'HoursDTThuClose'))
    values.append(_find_value(location, 'HoursDTFriOpen'))
    values.append(_find_value(location, 'HoursDTFriClose'))
    values.append(_find_value(location, 'HoursDTSatOpen'))
    values.append(_find_value(location, 'HoursDTSatClose'))
    values.append(_find_value(location, 'HoursDTSunOpen'))
    values.append(_find_value(location, 'HoursDTSunClose'))
    values.append(_find_value(location, 'Cashless'))
    values.append(_find_value(location, 'DriveThruOnly'))
    values.append(_find_value(location, 'LimitedTransactions'))
    values.append(_find_value(location, 'MilitaryIdRequired'))
    values.append(_find_value(location, 'SelfServiceDevice'))
    values.append(_find_value(location, 'SelfServiceOnly'))
    return _insert_sql_statement(values, 'Locations')

'''
Parses location object to create a sql insert statement into Contact table
param: location (a single location object)
return: sql insert statement (string)
'''
def _insert_into_contact(location):
    values = []
    # LocationID
    values.append(_find_value(location, 'ReferenceID'))
    # Phone
    values.append(_find_value(location, 'Phone'))
    # Fax
    values.append(_find_value(location, 'Fax'))
    # Terminal
    values.append(_find_value(location, 'WebAddress'))
    return _insert_sql_statement(values, 'Contact')

'''
Parses location object to create a sql insert statement into SpecialQualities table
param: location (a single location object)
return: sql insert statement (string)
'''
def _insert_into_specialqualities(location):
    values = []
    # LocationID
    values.append(_find_value(location, 'ReferenceID'))
    # RestrictedAccess
    values.append(_bool_to_bit(location, 'RestrictedAccess'))
    # DepositTaking
    values.append(_bool_to_bit(location, 'AcceptDeposit'))
    # LimitedTransactions
    values.append(_bool_to_bit(location, 'LimitedTransaction'))
    # HandicapAccess
    values.append(_bool_to_bit(location, 'HandicapAccess'))
    # AcceptsCash
    values.append(_bool_to_bit(location, 'AcceptCash'))
    # Cashless
    values.append(_bool_to_bit(location, 'Cashless'))
    # SelfServiceOnly
    values.append(_bool_to_bit(location, 'SelfServiceOnly'))
    # Surcharge
    values.append(_bool_to_bit(location, 'Surcharge'))
    # OnMilitaryBase
    values.append(_bool_to_bit(location, 'OnMilitaryBase'))
    # MilitaryIDRequired
    values.append(_bool_to_bit(location, 'MilitaryIdRequired'))
    # AdditionalDetail
    values.append(_bool_to_bit(location, 'AccessNotes'))
    return _insert_sql_statement(values, 'SpecialQualities')

'''
Finds value for a specified location field
param: location (beautiful soup object for one location)
param: Co-Op field to look for (string)
return: value of field (string)
'''
def _find_value(location, field):
    try:
        return location.find(field).text
    except AttributeError:
        return ''

'''
Gets hours for each day M-SUN
param: location (beautiful soup object)
returns: hours for each day (1 string). days with the same hours are grouped together.
'''
def _get_daily_hours(location):
    '''
    Returns hour range as a single string
    param: day (string). eg: Mon, Tue, Wed, etc.
    '''
    def _hour_string(day):
        op = _find_value(location, 'Hours' + day + 'Open')
        cl = _find_value(location, 'Hours' + day + 'Close')
        if op == '' or cl == '': return 'No hours information available'
        return op + ' - ' + cl

    days = ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun']
    hour_ranges = {}
    ret_string = 'No hours information available'
    for day in days: # Get all ranges
        hour_range = _hour_string(day)
        if hour_range == ret_string: continue # if no info, do not add to hour_ranges
        # Get abbreviation
        if day == 'Thu':
            slc = 'Th'
        elif day == 'Sun':
            slc = 'Su'
        else:
            slc = day[0]
        # insert into days dict
        if hour_range in hour_ranges: # if more than one day share the same range
            hour_ranges[hour_range] = hour_ranges[hour_range] + slc
        else: # if unique range thusfar
            hour_ranges[hour_range] = slc
    # if location info is available
    if len(hour_ranges) != 0:
        ret_string = ''
        for key in hour_ranges: # Generate return string
            ret_string += '{}: {}, '.format(hour_ranges[key], key)
        ret_string = ret_string[:-2] # Removes trailing whitespace and comma
    return ret_string

def _insert_sql_statement(value_list, table):
    statement = 'INSERT INTO {} VALUES ('.format(table)
    for value in value_list:
        if value == '': value = 'NULL'
        elif type(value) == str: value = "'{}'".format(value)
        statement += str(value) + ', '
    statement = statement[:-2] # Gets rid of last comma and trailing whitespace
    statement += ") SELECT '{0}' WHERE NOT EXISTS(SELECT * FROM {1} WHERE ReferenceID='{0}');".format(value_list[0], table)
    return statement

def _bool_to_bit(location, field):
    value = _find_value(location, field)
    if value == 'Y': value = 1
    elif value == 'N': value = 0
    return value

if __name__ == '__main__':
    get_key()
    sql_file_driver()
