import requests
import sqlite3
from bs4 import BeautifulSoup
from multiprocessing import Pool
from db_functions import *

def switch_case(case):
    switch_dict = {
        2022: ('https://middleware-conf.github.io/2022/program-committee/',
               'https://dblp.org/db/conf/middleware/middleware2022.html'),

        2021: ('https://middleware-conf.github.io/2021/program-committee/', 
               'https://dblp.org/db/conf/middleware/middleware2021.html'),

        2020: ('https://2020.middleware-conference.org/program-committee.html',
               'https://dblp.org/db/conf/middleware/middleware2020.html'),

        2019: ('https://2019.middleware-conference.org/program.html',
               'https://dblp.org/db/conf/middleware/middleware2019.html'),

        2018: ('http://2018.middleware-conference.org/index.php/program-committee/',
               'https://dblp.org/db/conf/middleware/middleware2018.html'),

        2016: ('http://2016.middleware-conference.org/organization/tpc/',
               'https://dblp.org/db/conf/middleware/middleware2016.html'),
    }
    result = switch_dict.get(case, 'This is the default case')
    return result

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

def find_members_middleware (year, link):
    create_tables(year, 'Middleware')
    r = requests.get(link)
    soup = BeautifulSoup(r.content, 'html.parser')
    list_of_people = []
    if year >= 2021:
        lines = soup.find('section', id="program-committee").find('div', class_="block").find_all('li')
        # print (lines)
        for name in lines:
            try:
                parts = name.text.split(', ')
                if len(parts) >= 2:
                    list_of_people.append((parts[0], ', '.join(parts[1:])))
            except:
                None
        with  Pool() as pool:
            pool.map(insert_names_members, [('Middleware', person, year) for person in list_of_people])
    
    elif year == 2020:
        lines = soup.find('main').find_all('p')
        # print (lines)
        for name in lines:
            try:
                parts = name.text.split(', ')
                # print(parts)
                if len(parts) >= 2:
                    list_of_people.append((parts[0], ', '.join(parts[1:])))
            except:
                print (name)
        with  Pool() as pool:
            pool.map(insert_names_members, [('Middleware', person, year) for person in list_of_people])

    elif year == 2019:
        lines = soup.find('section').find('div', class_="section-title-wrap mr-btm-72 text-center").find_all('div')
        # print (lines)
        for stuff in lines:
            # print(stuff)
            try:
                divs = stuff.find('div', class_="cbp-l-caption-body").find_all('div')
                list_of_people.append((divs[0].text, divs[1].text))
            except:
                None
        with  Pool() as pool:
            pool.map(insert_names_members, [('Middleware', person, year) for person in list_of_people])

    elif year == 2018:
        lines = soup.find('div', class_="entry-content").find_all('p')
        # print (lines)
        for name in lines:
            try:
                for a_tag in name.find_all('a', class_='commitee-name'):
                    name = a_tag.text.strip()
                    location = a_tag.find_next_sibling(string=True).strip().lstrip(', ')
                    list_of_people.append((name, location))
            except:
                print (name)
        with  Pool() as pool:
            pool.map(insert_names_members, [('Middleware', person, year) for person in list_of_people])

    elif year == 2016:
        lines = soup.find('div', class_="small-12 columns technical-program-committee").find('table').find_all('tr')
        # print (lines)
        for name in lines:
            try:
                parts = name.find_all('td')
                # print(parts)
                name = parts[0].text
                location = parts[1].text
                list_of_people.append((name, location))
            except:
                print (name)
        with  Pool() as pool:
            pool.map(insert_names_members, [('Middleware', person, year) for person in list_of_people])

def find_papers_middleware (year, link):
    r = requests.get(link)
    soup = BeautifulSoup(r.content, 'html.parser')
    lines = soup.find('ul', class_="publ-list").find_all('li')
    for line in lines:
        find_paper('Middleware', line, year)

def find_paper(conference_name, line, year):
    li = line.find('cite')
    try:
        paper_name = li.find('span', itemprop="name", class_="title").text
        if 'middleware' in paper_name.lower():
            paper_name = None
        # print (f'Paper: {paper_name}')
        names = []
        list_of_names = li.find_all('span', itemprop="author")
        for name in list_of_names:
            names.append(name.text)
        # print (f'Authors: {names}')
        # print ()
        # print ()
        paper_in_db (paper_name, names, conference_name, year)
    except:
        None
    
def paper_in_db(paper_name, names, conference_name, year):
    if paper_name and names:
        try:
            # Connect to the SQLite database (or create it if it doesn't exist)
            conn = sqlite3.connect(f'{conference_name}.db')
            cursor = conn.cursor()

            # Add or update authors in the Individuals table
            for author_name in names:
                cursor.execute(f"SELECT id FROM Individuals_{year} WHERE Name = ?", (author_name,))
                author_id = cursor.fetchone()
                if author_id:
                    # Author exists, update the paper_issuer column to 1
                    cursor.execute(f"UPDATE Individuals_{year} SET paper_issuer = 1 WHERE id = ?", (author_id[0],))
                else:
                    # Author doesn't exist, insert into the Individuals table
                    cursor.execute(f"INSERT INTO Individuals_{year} (Name, member, paper_issuer) VALUES (?, 0, 1)",
                                   (author_name,))

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
        except sqlite3.Error as e:
            print(f"SQLite error: {e}")
        finally:
            conn.close()

def middleware_db():
     for year in range(2016,2023):
        if year != 2017:
            links = switch_case(year)
            link1, link2 = links
            find_members_middleware(year, link1)
            find_papers_middleware(year, link2)
            print('Done')
            calculate_percentage_with_member(year, 'Middleware')
            count_individuals_with_member_and_paper_issuer(year, 'Middleware')
            # print_db_to_file(year, 'Middleware')
        
if __name__ == '__main__':
    middleware_db()