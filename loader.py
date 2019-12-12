import requests
import urllib3 
import bs4
import lxml

KEY = ""
CREATE_LOCATIONS_TBL = "CREATE TABLE Locations(LocationID CHAR(64) NOT NULL PRIMARY KEY, InstitutionName VARCHAR(50), TypeName VARCHAR(15) NOT NULL, Street VARCHAR(25) NOT NULL, City VARCHAR(25) NOT NULL, Zipcode VARCHAR(13) NOT NULL, Lat DECIMAL(9,6) NOT NULL, Long DECIMAL(9,6) NOT NULL, RetailOutlet VARCHAR(25));\n"
CREATE_CONTACT_TBL = "CREATE TABLE Contact(LocationID CHAR(64) NOT NULL PRIMARY KEY, Phone VARCHAR(13), Fax VARCHAR(13), Terminal VARCHAR(60), LocationID CHAR(64) FOREIGN KEY (LocationID) REFERENCES [Locations] (LocationID) ON UPDATE NO ACTION ON DELETE CASCADE);\n"
CREATE_SPECIAL_QUALITIES = "CREATE TABLE Special_Qualities(LocationID CHAR(64) NOT NULL PRIMARY KEY, RestrictedAccess BIT, DepositTaking BIT, LimitedHours BIT, Branch BIT, AdditionalDetail VARCHAR(50), AdditionalData VARCHAR(50), FOREIGN KEY LocationID REFERENCES [Locations] (LocationID) ON UPDATE NO ACTION ON DELETE CASCADE);\n"

'''
Gets Co-Op API key from local txt file. This file contains one line, which is the API key.
'''
def get_key():
    global KEY
    with open('key.txt', 'r') as file:
        KEY = file.read()

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
        location_insert_driver(file)

'''
Calls Co-Op API. For now only looks at all locations in one zipcode.
Calls insert_into_locations, insert_into_contact, insert_into_specialqualities.
'''
def location_insert_driver(file):
    offset = 0
    records_avail = -1

    # Call API
    r = requests.get('https://api.co-opfs.org/locator/proximitysearch', params={'zip':'98122', 'offset': str(offset)}, headers={'Accept':'application/xml', 'Version':'1', 'Authorization': KEY})
    soup = bs4.BeautifulSoup(r.text, 'xml')
    records_avail = int(soup.find('RecordsAvailable').text)
    print('Records available: ', records_avail)
    locations = soup.find_all('Location')
    print('First location: ', locations[0].prettify())

    # Check if total records exceed the length of returned locations
    # while offset+1 < records_avail:
        

    # Extract fields
    # Loop if necessary

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