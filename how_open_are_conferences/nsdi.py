import requests
import sqlite3
from bs4 import BeautifulSoup
from multiprocessing import Pool
from db_functions import *

def find_members_nsdi (year):
    link = f'https://www.usenix.org/conference/nsdi{str(year)[2:4]}/call-for-papers'
    create_tables(year, 'NSDI')
    r = requests.get(link)
    soup = BeautifulSoup(r.content, 'html.parser')
    lines = soup.find('div', class_="view-content").find_all('div', class_="field-content")
    list_of_people = []
    for name in lines:
        place = name.find('em')
        name = name.text.rstrip(', ').split(',')[0]
        if (place):
            person = [name, place.text]
        else: person = [name, None]
        list_of_people.append(person)
    with  Pool() as pool:
        pool.map(insert_names_members, [('NSDI', person, year) for person in list_of_people])

def find_papers_nsdi (year):
    if year < 2022 and year > 2019:
        link1 = f'https://www.usenix.org/conference/nsdi{str(year)[2:4]}/accepted-papers'
    elif year <= 2019:
        link1 = f'https://www.usenix.org/conference/nsdi{str(year)[2:4]}/technical-sessions'
    else:
        link1 = f'https://www.usenix.org/conference/nsdi{str(year)[2:4]}/spring-accepted-papers'
    r = requests.get(link1)
    soup = BeautifulSoup(r.content, 'html.parser')
    if year <= 2019:
        lines1 = soup.find('div', class_="content").find('div', class_="paragraphs-items paragraphs-items-field-paragraphs paragraphs-items-field-paragraphs-full paragraphs-items-full").find('div', class_="field-items").find_all('div')
    else:
        lines1 = soup.find('div', class_="content").find('div', class_="field-items").find_all('div')
    if year == 2023:
        link2 = f'https://www.usenix.org/conference/nsdi{str(year)[2:4]}/fall-accepted-papers'
        r = requests.get(link2)
        soup = BeautifulSoup(r.content, 'html.parser')
        lines2 = soup.find('div', class_="content").find('div', class_="content").find('div', class_="content").find('article').find('div', class_="field-items").find_all('div')
        lines1.extend(lines2)
    for line in lines1:
        if year <= 2019:
            try:
                line = line.find('div', class_="field-collection-container clearfix").find('article').find('div', class_="content").find_all('article')
                for li in line:
                    try:
                        find_paper('NSDI', li, year)
                    except:
                        None #print (li.prettify(), '\n?????????????????????????????????????????????????????\n')
            except: None #print (line.prettify(), '\n!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!\n')
        else:
            find_paper('NSDI', line, year)
        
def extract_names_and_universities(content):
    # Initialize variables to store names and the current university
    list_of_people = []
    names = []
    current_university = None

    # Iterate through the contents of the <p> element
    for item in content.contents:
        if isinstance(item, str):
            # Handle text content (names)
            name_list = [name.strip() for name in item.split(',')]
            
            # Process each name in the list
            for name in name_list:
                # Remove "and" from the beginning of names
                name = name.lstrip('and').strip()
                
                # Split names separated by "and"
                names.extend([n.strip().lstrip('and').strip() for n in name.split('and')])
        elif item.name == 'em':
            # Handle <em> tag (university)
            current_university = item.get_text().strip(' ;')
            
            # Associate the current university with names
            for name in names:
                if name:
                    list_of_people.append([name, current_university])
            names = []  # Reset names for the next university

    # If there are names left after the loop, associate them with the last university
    for name in names:
        if current_university and name:
            list_of_people.append([name, current_university])

    return list_of_people

def find_paper(conference_name, line, year):
    paper_name = None
    names = None
    try: 
        paper_name = line.find('h2').find('a').text
        names = line.find('div', class_="content").find('p')
        # if paper_name and names:
        #     print (f'Paper: {paper_name}')
        #     print (f'Authors: {names}')
        #     print ()
        #     print ()
    except:
        None
        # print(line, '$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$')
        # print('\n')
    paper_in_db (paper_name, names, conference_name, year)

def paper_in_db (paper_name, names, conference_name, year):
    if paper_name and names:
        authors = extract_names_and_universities(names)

        # Connect to the SQLite database (or create it if it doesn't exist)
        conn = sqlite3.connect(f'{conference_name}.db')

        # Create a cursor object to execute SQL commands
        cursor = conn.cursor()
        
        # Add or update authors in the Individuals table
        for author_name, university in authors:
            cursor.execute(f"SELECT id FROM Individuals_{year} WHERE Name = ?", (author_name,))
            author_id = cursor.fetchone()
            if author_id:
                # Author exists, update the paper_issuer column to 1
                cursor.execute(f"UPDATE Individuals_{year} SET paper_issuer = 1 WHERE id = ?", (author_id[0],))
            else:
                # Author doesn't exist, insert into the Individuals table
                cursor.execute(f"INSERT INTO Individuals_{year} (Name, Place, member, paper_issuer) VALUES (?, ?, 0, 1)",
                               (author_name, university))
            
            # Add the paper to the Papers table if it doesn't exist
            cursor.execute(f"SELECT id FROM Papers_{year} WHERE Name = ?", (paper_name,))
            paper_id = cursor.fetchone()
            if not paper_id:
                cursor.execute(f"INSERT INTO Papers_{year} (Name) VALUES (?)", (paper_name,))
            
            # Get the IDs of the author and paper
            cursor.execute(f"SELECT id FROM Individuals_{year} WHERE Name = ?", (author_name,))
            author_id = cursor.fetchone()[0]
            cursor.execute(f"SELECT id FROM Papers_{year} WHERE Name = ?", (paper_name,))
            paper_id = cursor.fetchone()[0]
            
            # Add the connection in the Who_wrote_what table
            cursor.execute(f"INSERT INTO Who_wrote_what_{year} (id_ind, id_paper) VALUES (?, ?)", (author_id, paper_id))

        # Commit the changes and close the connection after processing all papers
        conn.commit()
        conn.close()

def nsdi_db():
    for year in range(2017,2024):
        find_members_nsdi(year)
        find_papers_nsdi(year)
        print('Done')
        calculate_percentage_with_member(year, 'NSDI')
        count_individuals_with_member_and_paper_issuer(year, 'NSDI')
        # print_db_to_file(year, 'NSDI')

if __name__ == '__main__':
    nsdi_db()