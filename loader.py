import requests
import urllib3 
import bs4
import lxml

KEY = ""
CREATE_LOCATIONS_TBL = "CREATE TABLE Locations(LocationID VARCHAR(64) NOT NULL PRIMARY KEY, InstitutionName VARCHAR(50), TypeName VARCHAR(15) NOT NULL, Street VARCHAR(25) NOT NULL, City VARCHAR(25) NOT NULL, State CHAR(2) NOT NULL, Zipcode VARCHAR(13) NOT NULL, Lat DECIMAL(9,6) NOT NULL, Long DECIMAL(9,6) NOT NULL, RetailOutlet VARCHAR(25), Hours VARCHAR(200));\n"
CREATE_CONTACT_TBL = "CREATE TABLE Contact(LocationID VARCHAR(64) NOT NULL PRIMARY KEY, Phone VARCHAR(13), Fax VARCHAR(13), Terminal VARCHAR(60), FOREIGN KEY (LocationID) REFERENCES [Locations] (LocationID) ON UPDATE NO ACTION ON DELETE CASCADE);\n"
CREATE_SPECIAL_QUALITIES = "CREATE TABLE SpecialQualities(LocationID VARCHAR(64) NOT NULL PRIMARY KEY, RestrictedAccess BIT, DepositTaking BIT, LimitedTransactions BIT, HandicapAccess BIT, AcceptsCash BIT, Cashless BIT, SelfServiceOnly BIT, Surcharge BIT, OnMilitaryBase BIT, MilitaryIDRequired BIT, AdditionalDetail VARCHAR(50), FOREIGN KEY (LocationID) REFERENCES [Locations] (LocationID) ON UPDATE NO ACTION ON DELETE CASCADE);\n"

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
        file.write(CREATE_CONTACT_TBL)
        file.write(CREATE_SPECIAL_QUALITIES)
        # Write location sql insert statements to file
        sql_statements = _insert_driver()
        for statement in sql_statements:
            file.write(statement)

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
            sql_statements.append(_insert_into_contact(location) + '\n')
            sql_statements.append(_insert_into_specialqualities(location) + '\n')
        break
    return sql_statements

'''
Parses location object to create sql insert statement into Locations table
param: location (a single location object)
return: sql insert statement (string)
'''
def _insert_into_locations(location):
    values = []

    # LocationID
    values.append(_find_value(location, 'ReferenceID'))
    # InstitutionName
    values.append(_find_value(location, 'Name'))
    # TypeName
    type_name = _find_value(location, 'LocationType')
    if type_name == 'A': values.append('ATM')
    elif type_name == 'S': values.append('Shared Branch')
    else: values.append('')
    # Street
    values.append(_find_value(location, 'Address'))
    # City
    values.append(_find_value(location, 'City'))
    # State
    values.append(_find_value(location, 'State'))
    # Zipcode
    values.append(_find_value(location, 'PostalCode'))
    # Lat
    values.append(float(_find_value(location, 'Latitude')))
    # Long
    values.append(float(_find_value(location, 'Longitude')))
    # RetailOutlet
    values.append(_find_value(location, 'RetailOutlet'))
    # Hours
    hours = _find_value(location, 'Hours')
    if hours != '':
        values.append(hours)
    else:
        values.append(_get_daily_hours(location))
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
    statement += ") SELECT '{0}' WHERE NOT EXISTS(SELECT * FROM {1} WHERE LocationID='{0}');".format(value_list[0], table)
    return statement

def _bool_to_bit(location, field):
    value = _find_value(location, field)
    if value == 'Y': value = 1
    elif value == 'N': value = 0
    return value

if __name__ == '__main__':
    get_key()
    sql_file_driver()