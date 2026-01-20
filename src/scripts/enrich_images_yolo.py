from ultralytics import YOLO
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os
import logging

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load pre-trained YOLOv8 nano model (fast & good for objects like pill, cream, bottle)
model = YOLO("yolov8n.pt")  # COCO pre-trained (detects 80 classes)

# DB connection
engine = create_engine(f"postgresql+psycopg2://{os.getenv('POSTGRES_USER')}:{os.getenv('POSTGRES_PASSWORD')}@localhost:5432/medical_warehouse")

# Get messages with images from fct_messages
query = """
SELECT message_id, image_path
FROM fct_messages
WHERE image_path IS NOT NULL AND image_path != ''
"""
df = pd.read_sql(query, engine)

if df.empty:
    logging.info("No images found in fct_messages to enrich.")
    exit()

logging.info(f"Found {len(df)} images to process with YOLOv8.")

enrichments = []

for _, row in df.iterrows():
    img_path = Path(row['image_path'])
    message_id = row['message_id']

    if not img_path.exists():
        logging.warning(f"Image not found: {img_path}")
        continue

    try:
        results = model(img_path)  # run inference
        detected = []
        for result in results:
            for box in result.boxes:
                cls_id = int(box.cls)
                class_name = result.names[cls_id]
                detected.append(class_name)

        detected = list(set(detected))  # unique objects
        count = len(detected)

        enrichments.append({
            'message_id': message_id,
            'detected_objects': detected,
            'object_count': count
        })

        logging.info(f"Processed message {message_id}: {detected} ({count} objects)")

    except Exception as e:
        logging.error(f"Error processing {img_path}: {e}")

# Convert to DataFrame and update DB
# ... (keep all the code before the update part)

if enrichments:
    enrich_df = pd.DataFrame(enrichments)

    # Convert Python list to PostgreSQL array string format
    enrich_df['detected_objects'] = enrich_df['detected_objects'].apply(lambda x: '{' + ','.join(f'"{item}"' for item in x) + '}' if x else '{}')

    # Temporary table
    enrich_df.to_sql('yolo_temp', engine, if_exists='replace', index=False)

    with engine.connect() as conn:
        conn.execute(text("""
            UPDATE fct_messages f
            SET detected_objects = t.detected_objects::text[],
                object_count = t.object_count
            FROM yolo_temp t
            WHERE f.message_id = t.message_id
        """))
        conn.execute(text("DROP TABLE IF EXISTS yolo_temp"))
        conn.commit()

    logging.info(f"YOLOv8 enrichment complete! Updated {len(enrichments)} rows in fct_messages.")
else:
    logging.info("No valid detections to update.")