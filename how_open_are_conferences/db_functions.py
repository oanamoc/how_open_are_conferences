import requests
from bs4 import BeautifulSoup
import re
import sqlite3
from multiprocessing import Pool



def create_tables(conference_year, conference_name):
    # Connect to the SQLite database (or create it if it doesn't exist)
    conn = sqlite3.connect(f'{conference_name}.db')

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

     # Create the Individuals table for the specific conference year
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS Individuals_{conference_year} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT,
                        Place TEXT,
                        member BOOLEAN,
                        paper_issuer BOOLEAN
                    )''')

    # Create the Papers table for the specific conference year
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS Papers_{conference_year} (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        Name TEXT
                    )''')

    # Create the Who_wrote_what table for the specific conference year
    cursor.execute(f'''CREATE TABLE IF NOT EXISTS "Who_wrote_what_{conference_year}" (
                        id_ind INTEGER,
                        id_paper INTEGER,
                        FOREIGN KEY (id_ind) REFERENCES Individuals_{conference_year}(id),
                        FOREIGN KEY (id_paper) REFERENCES Papers_{conference_year}(id)
                    )''')

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

    print(f'\nDatabase and tables created successfully. {conference_year} {conference_name}')

def insert_names_members(args):
    conference_name, data, year = args
    # Connect to the database or create one if it doesn't exist
    conn = sqlite3.connect(f'{conference_name}.db')

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Check if the name already exists in the table
    cursor.execute(f'SELECT Name FROM Individuals_{year} WHERE Name = ?', (data[0],))
    existing_name = cursor.fetchone()

    if existing_name is None:
        # Insert the specified row into the "Members" table
        insert_query = f'''
            INSERT INTO Individuals_{year} (Name, Place, member)            
            VALUES (?, ?, 1)
            '''
        cursor.execute(insert_query, data)

    # Commit the changes and close the connection
    conn.commit()
    conn.close()

def print_db(conference_year, conference_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(f'{conference_name}.db')

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Print content of the Individuals table
    print("=== Individuals Table ===")
    cursor.execute(f'SELECT * FROM Individuals_{conference_year}')
    members = cursor.fetchall()
    for member in members:
        print("ID:", member[0])
        print("Name:", member[1])
        print("Place:", member[2])
        print("Member:", member[3])
        print("Paper Issuer:", member[4])
        print()

    # Print content of the Papers table
    print("\n=== Papers Table ===")
    cursor.execute(f'SELECT * FROM Papers_{conference_year}')
    papers = cursor.fetchall()
    for paper in papers:
        print("ID:", paper[0])
        print("Name:", paper[1])
        print()

    # Print content of the Who_wrote_what table
    print("\n=== Who_wrote_what Table ===")
    cursor.execute(f'SELECT * FROM Who_wrote_what_{conference_year}')
    connections = cursor.fetchall()
    for connection in connections:
        print("ID (Individual):", connection[0])
        print("ID (Paper):", connection[1])
        print()

    # Close the connection
    conn.close()

def print_db_to_file(conference_year, conference_name):
    # Define the file name based on the conference year
    file_name = f'{conference_name}_{conference_year}.txt'

    # Connect to the SQLite database
    conn = sqlite3.connect(f'{conference_name}.db')

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Open the file for writing
    with open(file_name, 'w') as file:
        # Write content of the Individuals table to the file
        file.write("=== Individuals Table ===\n")
        cursor.execute(f'SELECT * FROM Individuals_{conference_year}')
        members = cursor.fetchall()
        for member in members:
            file.write(f"ID: {member[0]}\n")
            file.write(f"Name: {member[1]}\n")
            file.write(f"Place: {member[2]}\n")
            file.write(f"Member: {member[3]}\n")
            file.write(f"Paper Issuer: {member[4]}\n\n")

        # Write content of the Papers table to the file
        file.write("\n=== Papers Table ===\n")
        cursor.execute(f'SELECT * FROM Papers_{conference_year}')
        papers = cursor.fetchall()
        for paper in papers:
            file.write(f"ID: {paper[0]}\n")
            file.write(f"Name: {paper[1]}\n\n")

        # Write content of the Who_wrote_what table to the file
        file.write("\n=== Who_wrote_what Table ===\n")
        cursor.execute(f'SELECT * FROM Who_wrote_what_{conference_year}')
        connections = cursor.fetchall()
        for connection in connections:
            file.write(f"ID (Individual): {connection[0]}\n")
            file.write(f"ID (Paper): {connection[1]}\n\n")

    # Close the connection
    conn.close()    

def calculate_percentage_with_member(conference_year, conference_name):
    # Connect to the SQLite database
    conn = sqlite3.connect(f'{conference_name}.db')

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Count the total number of papers
    cursor.execute(f'SELECT COUNT(*) FROM Papers_{conference_year}')
    total_papers = cursor.fetchone()[0]

    # Count the number of papers with at least one member as an author
    cursor.execute(f'''
        SELECT COUNT(DISTINCT P.id)
        FROM Papers_{conference_year} AS P
        JOIN Who_wrote_what_{conference_year} AS W ON P.id = W.id_paper
        JOIN Individuals_{conference_year} AS I ON W.id_ind = I.id
        WHERE I.member = 1
    ''')
    papers_with_member = cursor.fetchone()[0]

    # Calculate the percentage
    if total_papers > 0:
        percentage = (papers_with_member / total_papers) * 100
    else:
        percentage = 0.0

    # Print the result
    print(f"Percentage of papers with at least one member as an author: {percentage:.2f}%")

    # Close the connection
    conn.close()            

def count_individuals_with_member_and_paper_issuer(conference_year, conference_name):

    # Connect to the SQLite database
    conn = sqlite3.connect(f'{conference_name}.db')

    # Create a cursor object to execute SQL commands
    cursor = conn.cursor()

    # Count the number of individuals who are both member and paper issuer
    cursor.execute(f'''
        SELECT COUNT(*)
        FROM Individuals_{conference_year}
        WHERE member = 1 AND paper_issuer = 1
    ''')
    count = cursor.fetchone()[0]

    # Print the result
    print(f"Number of individuals who are both member and paper issuer: {count}")

    # Close the connection
    conn.close()



if __name__ == '__main__':
    None