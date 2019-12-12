import requests
import urllib3 
import bs4
import lxml

KEY = ""
CREATE_LOCATIONS_TBL = "CREATE TABLE Locations(LocationID CHAR(64) NOT NULL PRIMARY KEY, InstitutionName VARCHAR(50), TypeName VARCHAR(15) NOT NULL, Street VARCHAR(25) NOT NULL, City VARCHAR(25) NOT NULL, State CHAR(2) NOT NULL, Zipcode VARCHAR(13) NOT NULL, Lat DECIMAL(9,6) NOT NULL, Long DECIMAL(9,6) NOT NULL, RetailOutlet VARCHAR(25), Hours VARCHAR(200));\n"
CREATE_CONTACT_TBL = "CREATE TABLE Contact(LocationID CHAR(64) NOT NULL PRIMARY KEY, Phone VARCHAR(13), Fax VARCHAR(13), Terminal VARCHAR(60), FOREIGN KEY LocationID REFERENCES [Locations] (LocationID) ON UPDATE NO ACTION ON DELETE CASCADE);\n"
CREATE_SPECIAL_QUALITIES = "CREATE TABLE Special_Qualities(LocationID CHAR(64) NOT NULL PRIMARY KEY, RestrictedAccess BIT, DepositTaking BIT, LimitedTransactions BIT, HandicapAccess BIT, AcceptsCash BIT, Cashless BIT, SelfServiceOnly BIT, Surcharge BIT, OnMilitaryBase BIT, MilitaryIDRequired BIT AdditionalDetail VARCHAR(50), FOREIGN KEY LocationID REFERENCES [Locations] (LocationID) ON UPDATE NO ACTION ON DELETE CASCADE);\n"

'''
Gets Co-Op API key from local txt file. This file contains one line, which is the API key.
'''
def get_key():
    global KEY
    with open('key.txt', 'r') as file:
        KEY = file.read()

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
        sql_statements = location_insert_driver()
        for statement in sql_statements:
            file.write(statement)

'''
Calls Co-Op API. For now only looks at all locations in one zipcode.
Calls insert_into_locations, insert_into_contact, insert_into_specialqualities.
'''
def location_insert_driver():
    offset = 0
    records_avail = -1
    sql_statements = []

    # Call API
    locations, records_avail = _call_api(offset)

    # Check if total records exceed the length of returned locations
    while offset+1 < records_avail:
        for location in locations:
            offset += 1
            sql_statements.append(_insert_into_locations(location))
            sql_statements.append(_insert_into_contact(location))
            sql_statements.append(_insert_into_specialqualities(location))

        break
    return sql_statements

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
Parses location object to create sql insert statement into Locations table
param: location (a single location object)
return: sql insert statement (string)
'''
#FIXME
def _insert_into_locations(location):
    print('in _insert_into_locations')
    return None

'''
Parses location object to create a sql insert statement into Contact table
param: location (a single location object)
return: sql insert statement (string)
'''
#FIXME
def _insert_into_contact(location):
    print('in _insert_into_contact')
    return None

'''
Parses location object to create a sql insert statement into SpecialQualities table
param: location (a single location object)
return: sql insert statement (string)
'''
#FIXME
def _insert_into_specialqualities(location):
    print('in _insert_into_specialqualities')
    return None

if __name__ == '__main__':
    '''
    locations = soup.find_all('Location')

    print(locations[0].prettify())
    print(locations[0].find('Name').text)

    print(locations[0].prettify())

    for i in range(0, len(locations)):
        print("location: ", locations[i].find('Name').text)
        print("\tlatitude: ", locations[i].find('Latitude').text)
        print("\tlongitude: ", locations[i].find('Longitude').text)
    '''
    get_key()
    sql_file_driver()