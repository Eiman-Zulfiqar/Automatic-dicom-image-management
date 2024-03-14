SELECT * FROM image_metadata

ALTER TABLE image_metadata
ADD COLUMN image BYTEA NOT NULL;

DELETE FROM image_metadata

SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = 'image_metadata';

ALTER TABLE image_metadata
ADD COLUMN patient_id INT UNIQUE NOT NULL;

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