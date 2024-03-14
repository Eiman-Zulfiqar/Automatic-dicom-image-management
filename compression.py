import os
import shutil
import time
import gzip
import pydicom
import psycopg2
from datetime import datetime
import random

# Function to connect to PostgreSQL database
def connect_to_database():
    try:
        connection = psycopg2.connect(
            user="postgres",
            password="SQLfatima@31",
            host="localhost",
            port="5432",
            database="image"
        )
        return connection
    except (Exception, psycopg2.Error) as error:
        print(f"Error while connecting to PostgreSQL: {error}")
        return None

# Function to create table for storing image metadata
def create_metadata_table(connection):
    create_table_query = '''
        CREATE TABLE IF NOT EXISTS image_metadata (
            id SERIAL PRIMARY KEY,
            modality VARCHAR(50),
            filename VARCHAR(255) NOT NULL,
            filepath VARCHAR(255) NOT NULL,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            compressed BOOLEAN DEFAULT FALSE,
            patient_id VARCHAR(10),  -- New column for patient ID
            image BYTEA NOT NULL
        )
    '''
    try:
        cursor = connection.cursor()
        cursor.execute(create_table_query)
        connection.commit()
        print("Metadata table created successfully.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error creating metadata table: {error}")

# Function to generate a random patient ID
def generate_patient_id():
    # Generate a random 4-digit patient ID
    return str(random.randint(1000, 9999))

# Function to insert metadata into PostgreSQL table
def insert_metadata(connection, filename, filepath, modality, image_data, patient_id):
    # Adjust the filepath to be relative to the short-term directory
    short_term_filepath = os.path.join(short_term_directory, filename)
    
    insert_query = '''
        INSERT INTO image_metadata (filename, filepath, modality, patient_id, image)
        VALUES (%s, %s, %s, %s, %s)
    '''
    try:
        cursor = connection.cursor()
        cursor.execute(insert_query, (filename, short_term_filepath, modality, patient_id, psycopg2.Binary(image_data)))
        connection.commit()
        print(f"Metadata inserted for {filename}.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error inserting metadata: {error}")

# Function to compress DICOM image using gzip
def compress_dicom(dicom_path, compressed_path):
    try:
        with open(dicom_path, 'rb') as f_in:
            with gzip.open(compressed_path, 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)
    except Exception as e:
        print(f"Error compressing DICOM image: {e}")

# Function to move DICOM image to long-term storage and compress if needed
def move_to_long_term(dicom_path, long_term_directory, connection):
    filename = os.path.splitext(os.path.basename(dicom_path))[0]
    compressed_path = os.path.join(long_term_directory, f'{filename}.gz')

    # Compress the DICOM file
    compress_dicom(dicom_path, compressed_path)

    # Update metadata with compressed path before compression
    update_metadata(connection, dicom_path, compressed_path)

    # Remove the original DICOM file
    os.remove(dicom_path)
    print(f"Image {filename} moved and compressed to long-term storage.")

# Function to update image metadata in the database
def update_metadata(connection, original_path, new_path):
    update_query = '''
        UPDATE image_metadata
        SET filepath = %s, compressed = True
        WHERE filepath = %s
    '''
    try:
        cursor = connection.cursor()
        cursor.execute(update_query, (new_path, original_path))
        connection.commit()
        print(f"Metadata updated for {original_path}.")
    except (Exception, psycopg2.Error) as error:
        print(f"Error updating metadata: {error}")

# Rest of your code remains unchanged...

# Example usage
def main(input_directory, short_term_directory, long_term_directory):
    os.makedirs(input_directory, exist_ok=True)
    os.makedirs(short_term_directory, exist_ok=True)
    os.makedirs(long_term_directory, exist_ok=True)

    # Connect to PostgreSQL database
    connection = connect_to_database()
    if connection:
        # Create metadata table if not exists
        create_metadata_table(connection)

        # Initialize file timers for existing files in short-term directory
        file_timers = {}
        for dicom_file in os.listdir(short_term_directory):
            file_timers[dicom_file] = time.time()

        while True:
            # Move new DICOM images from input directory to short-term directory
            for dicom_file in os.listdir(input_directory):
                dicom_path = os.path.join(input_directory, dicom_file)
                if os.path.isfile(dicom_path) and dicom_file.endswith('.dcm'):
                    # Move the file to the short-term directory
                    shutil.move(dicom_path, os.path.join(short_term_directory, dicom_file))
                    
                    # Start timer for the new file
                    file_timers[dicom_file] = time.time()
                    
                    # Extract modality information from DICOM file
                    dataset = pydicom.dcmread(os.path.join(short_term_directory, dicom_file))
                    modality = dataset.Modality

                    # Read image data
                    with open(os.path.join(short_term_directory, dicom_file), 'rb') as file:
                        image_data = file.read()

                    # Generate a unique patient ID for each image
                    patient_id = generate_patient_id()

                    # Insert metadata for the moved file with modality information and image data
                    insert_metadata(connection, dicom_file, dicom_path, modality, image_data, patient_id)

            # Check for files in the short-term directory
            current_time = time.time()
            for dicom_file in os.listdir(short_term_directory):
                dicom_path = os.path.join(short_term_directory, dicom_file)
                if os.path.isfile(dicom_path):
                    if dicom_file in file_timers:
                        file_age = current_time - file_timers[dicom_file]
                        if file_age > 180:  # Delay of 3 minutes (180 seconds)
                            move_to_long_term(dicom_path, long_term_directory, connection)
                            del file_timers[dicom_file]

            # Sleep for a certain interval before checking again
            time.sleep(5)  # Check every 5 seconds (adjust as needed)

    # Close database connection
    if connection:
        connection.close()


if __name__ == "__main__":
    input_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1 - Copy\input'
    short_term_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1 - Copy\shortterm'
    long_term_directory = r'C:\Users\Eiman Zulfiqar\OneDrive\Desktop\Comp-M1 - Copy\longterm'

    main(input_directory, short_term_directory, long_term_directory)
