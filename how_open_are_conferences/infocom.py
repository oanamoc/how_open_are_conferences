import requests
import sqlite3
from bs4 import BeautifulSoup
from multiprocessing import Pool
from db_functions import *

def extract_names_and_universities(name_list):
    list_of_people = []

    names_place = [name.strip(') ') for name in name_list.split(';')]
            
    # Process each name in the list
    for stuff in names_place:
        stuff = stuff.split('(')
        try:
            current_university = stuff[1]
        except: 
            current_university = stuff[0]
        names = stuff[0].split(',')
        separated_names = []    
        for name in names:
            if name:
                # Split names separated by "and"
                separated_names.extend([n.strip().lstrip('and').strip() for n in name.split('and')])
        for name in separated_names:
            list_of_people.append([name, current_university])
    return list_of_people

def switch_case(case):
    switch_dict = {
        2023: ('https://infocom2023.ieee-infocom.org/committees/steering-committee', 
               'https://infocom2023.ieee-infocom.org/committees/organizing-committee',
               'https://infocom2023.ieee-infocom.org/program/accepted-paper-list-main-conference'),
        2022: ('https://infocom2022.ieee-infocom.org/committees/steering-committee.html',
               'https://infocom2022.ieee-infocom.org/committees/organizing-committee',
               'https://infocom2022.ieee-infocom.org/program/accepted-paper-list-main-conference.html'),
        2021: ('https://infocom2021.ieee-infocom.org/committees/steering-committee.html', 
               'https://infocom2021.ieee-infocom.org/committees/organizing-committee.html',
               'https://infocom2021.ieee-infocom.org/accepted-paper-list-main-conference.html'),
        2020: ('https://infocom2020.ieee-infocom.org/committee/steering-committee.html', 
               'https://infocom2020.ieee-infocom.org/committee/organizing-committee-0.html',
               'https://infocom2020.ieee-infocom.org/accepted-paper-list-main-conference.html'),
        2019: ('https://infocom2019.ieee-infocom.org/committee/steering-committee.html', 
               'https://infocom2019.ieee-infocom.org/committee/organizing-committee-0.html',
               'https://infocom2019.ieee-infocom.org/accepted-paper-list-main-conference.html'),
        2018: ('https://infocom2018.ieee-infocom.org/committee/steering-committee.html', 
               'https://infocom2018.ieee-infocom.org/committee/organizing-committee-0.html',
               'https://infocom2018.ieee-infocom.org/program/accepted-paper-list-main-conference.html'),
    }
    result = switch_dict.get(case, 'This is the default case')
    return result

def find_members_infocom (year, link1, link2):
    create_tables(year, 'Infocom')
    links = [link1, link2]
    for link in links:
        r = requests.get(link)
        soup = BeautifulSoup(r.content, 'html.parser')
        lines = soup.find('div', class_="field-items").find_all('p')
        list_of_people = []
        for name in lines:
            text = name.get_text().strip()  # Get the text content and remove leading/trailing spaces
            if text:
                parts = text.split('(')
                if len(parts) == 2:
                    name = parts[0].strip()
                    location = parts[1].rstrip(')').strip()
                    list_of_people.append((name, location))
                elif len(parts) > 2:
                    name = parts[0].strip()
                    location = parts[1].split(')')[0].rstrip(')').strip()
                    list_of_people.append((name, location))
        with  Pool() as pool:
            pool.map(insert_names_members, [('Infocom', person, year) for person in list_of_people])

def find_papers_infocom (year, link):
    r = requests.get(link)
    soup = BeautifulSoup(r.content, 'html.parser')
    if year == 2021 or year == 2020:
        lines = soup.find('div', class_="field-items").find('div')
        find_paper('Infocom', lines, year)
    else:
        lines = soup.find('div', class_="field-items").find('ol').find_all('li')
        for line in lines:
            find_paper('Infocom', line, year)

def find_paper(conference_name, line, year):
    paper_name = None
    names = None
    if year == 2021 or year == 2020:
        for li in line.contents:
            if li.name == 'ol':
                paper_name = li.get_text().strip()
                # print (f'Paper: {paper_name}')
            if li.name == 'p':
                names = li.get_text().strip()
                # print (f'Authors: {names}')
                # print ()
                # print ()
            paper_in_db (paper_name, names, conference_name, year)
    else:
        try: 
            paper_name = line.find('strong').text.strip()
            names = line.text.replace(paper_name, '').strip()
        except:
            print ('error ')
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

def infocom_db():
    for year in range(2018,2024):
        links = switch_case(year)
        link1, link2, link3 = links
        find_members_infocom(year, link1, link2)
        find_papers_infocom(year, link3)
        print('Done')
        calculate_percentage_with_member(year, 'Infocom')
        count_individuals_with_member_and_paper_issuer(year, 'Infocom')
        # print_db_to_file(year, 'Infocom')
        
if __name__ == '__main__':
    infocom_db()