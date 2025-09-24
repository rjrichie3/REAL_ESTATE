import pandas as pd
from sqlalchemy import create_engine, text

# Load CSV
df = pd.read_csv("Luxury_Housing_Bangalore.csv")
df['Ticket_Price_Cr'] = df['Ticket_Price_Cr'].astype(str).str.replace('₹','').str.replace('Cr','').str.strip()
df['Ticket_Price_Cr'] = pd.to_numeric(df['Ticket_Price_Cr'], errors='coerce')
df['Unit_Size_Sqft'] = df['Unit_Size_Sqft'].fillna(df['Unit_Size_Sqft'].median())
df['Amenity_Score'] = df['Amenity_Score'].fillna(df['Amenity_Score'].median())
text_cols = ['Micro_Market','Project_Name','Developer_Name','Configuration','Possession_Status','Sales_Channel']
for col in text_cols:
    df[col] = df[col].str.strip().str.title()
df['Purchase_Quarter'] = pd.to_datetime(df['Purchase_Quarter'], errors='coerce')
df['Year'] = df['Purchase_Quarter'].dt.year
df['Quarter_Number'] = df['Purchase_Quarter'].dt.quarter
df['Price_per_Sqft'] = df['Ticket_Price_Cr']*10000000 / df['Unit_Size_Sqft']
df['Booking_Flag'] = df['Transaction_Type'].apply(lambda x: 1 if x.lower() == 'primary' else 0)
from textblob import TextBlob
df['Buyer_Comments'] = df['Buyer_Comments'].fillna('')
df['Buyer_Comment_Sentiment'] = df['Buyer_Comments'].apply(lambda x: TextBlob(x).sentiment.polarity)

# Save cleaned CSV
df.to_csv("Luxury_Housing_Bangalore1.csv", index=False)

# Database connection
user = 'root'
password = '08520852'
host = 'localhost'
database = 'luxury_real_estate_db'

engine = create_engine(f'mysql+pymysql://{user}:{password}@{host}/{database}')

# Debugging to confirm connection and upload
try:
    with engine.connect() as conn:
        # Use text() to run raw SQL statements
        result = conn.execute(text("SELECT DATABASE();"))
        for row in result:
            print("Connected to database:", row[0])

    print("Rows to upload:", len(df))
    df.to_sql('luxury_housing', con=engine, if_exists='replace', index=False)
    print("Data uploaded successfully!")

except Exception as e:
    print("An error occurred:", e)
