import sqlite3
import csv
import os

class ContainerDatabase:
    def __init__(self, db_path='data.db'):
        self.db_path = db_path

    def get_data(self):
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        cursor.execute("SELECT container, train, priority FROM container")
        data = cursor.fetchall()
        conn.close()
        return data
    
    def load_data_from_csv(self, csv_file_path, delimiter=','):
        if not os.path.exists(csv_file_path):
            raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
        
        with open(csv_file_path, 'r', newline='') as f:
            reader = csv.reader(f, delimiter=delimiter)
            header = next(reader, None)
            
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()

            cursor.execute('''
            DROP TABLE IF EXISTS container;
            ''')
            
            cursor.execute('''
            CREATE TABLE  container (
                container TEXT,
                train TEXT,
                priority INTEGER,
                PRIMARY KEY (container, train)
            )
            ''')
            
            records_inserted = 0
            for row in reader:
                if len(row) >= 2:
                    container = row[0]
                    train = row[1]
                    try:
                        priority = int(row[2]) if len(row) > 2 else 0
                    except ValueError:
                        priority = 0
                    
                    try:
                        cursor.execute(
                            "INSERT OR REPLACE INTO container (container, train, priority) VALUES (?, ?, ?)",
                            (container, train, priority)
                        )
                        
                        records_inserted += 1
                    except sqlite3.Error as e:
                        print(f"Error inserting row {row}: {e}")
            
            conn.commit()
            conn.close()
            
            return records_inserted
    