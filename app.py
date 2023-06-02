import os
import csv
import pandas as pd
from flask import Flask, request, render_template
from werkzeug.utils import secure_filename
import mysql.connector
import re
import io
from dotenv import load_dotenv

app = Flask(__name__)

ALLOWED_EXTENSIONS = {'csv'}

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

load_dotenv()

def connect_to_mysql():
    return mysql.connector.connect(
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        host=os.getenv('MYSQL_HOST'),
        database=os.getenv('MYSQL_DATABASE'),
        auth_plugin='mysql_native_password'
    )

def retrieve_blueprints(network, search):
    url = os.getenv('URL')
    token = os.getenv('BLUEPRINTS_TOKEN')
    request = url + "?token=" + token + "&network_name="+ network + "&search=" + search
    df = pd.read_csv(request)
    insert_data_to_mysql(search, df)

def clean_column_name(column_name):
    column_name = column_name.replace(' ', '_')
    column_name = re.sub(r'[()]', '', column_name)
    return column_name

def recreate_table(table_name, column_names):
    conn = connect_to_mysql()
    cursor = conn.cursor()

    drop_table_query = f"DROP TABLE IF EXISTS {table_name}"
    cursor.execute(drop_table_query)

    column_names = [clean_column_name(column) for column in column_names]

    columns = ', '.join([f'`{column}` TEXT' for column in column_names])
    create_table_query = f"CREATE TABLE {table_name} ({columns})"
    cursor.execute(create_table_query)

    conn.commit()
    cursor.close()
    conn.close()

def insert_data_to_mysql(table_name, data):
    conn = connect_to_mysql()
    cursor = conn.cursor()

    data.columns = [clean_column_name(column) for column in data.columns]

    placeholders = ', '.join(['%s'] * len(data.columns))
    columns = ', '.join([f'`{column}`' for column in data.columns])
    insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
    
    data = data.fillna('')

    for _, row in data.iterrows():
        cursor.execute(insert_query, tuple(row))

    conn.commit()
    cursor.close()
    conn.close()
    conn.close()
    
def process_csv(filename):
    cleaned_data = []

    with open(filename, 'r', newline='', encoding='utf-8-sig', errors='replace') as csvfile:
        reader = csv.reader(csvfile)
        header = next(reader)
        num_fields = len(header)
        cleaned_data.append(header)

        for row in reader:
            if len(row) != num_fields:
                cleaned_row = row[:num_fields]
                cleaned_data.append(cleaned_row)
            else:
                cleaned_data.append(row)

    cleaned_df = pd.DataFrame(cleaned_data[1:], columns=cleaned_data[0])
    return cleaned_df

def print_table_structure_and_sample_data(table_name):
    conn = connect_to_mysql()
    cursor = conn.cursor()

    cursor.execute(f"DESCRIBE {table_name}")
    print("Table structure:")
    for row in cursor.fetchall():
        print(row)

    cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
    print("\nSample data:")
    for row in cursor.fetchall():
        print(row)

    cursor.close()
    conn.close()
    
@app.route('/', methods=['GET', 'POST'])
def index():
    if request.method == 'POST':
        uploaded_files = []
        for key in request.files.keys():
            file = request.files[key]
            if file.filename == '':
                continue

            if file and allowed_file(file.filename):
                filename = secure_filename(file.filename)
                file.save(os.path.join(os.getcwd(), filename))

                data = process_csv(filename)
                os.remove(filename)

                table_name = filename.rsplit('.', 1)[0]
                recreate_table(table_name, data.columns)
                insert_data_to_mysql(table_name, data)
                uploaded_files.append(table_name)

        if not uploaded_files:
            return render_template('index.html', error='No files selected or all files were of invalid types')

        return render_template('index.html', success=f'CSV data uploaded to MySQL database successfully for {", ".join(uploaded_files)}')
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)
