
import gspread
import pandas as pd
import logging
import os
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

# Scope required for gspread
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]

def update_sheet(df, spreadsheet_name="Stock Screener Top 120", json_keyfile="service_account.json"):
    """
    Updates a Google Sheet with the provided DataFrame.
    """
    if not os.path.exists(json_keyfile):
        logger.warning("Google Sheets key file '%s' not found. Skipping export.", json_keyfile)
        return

    try:
        logger.info("Connecting to Google Sheets...")
        creds = Credentials.from_service_account_file(json_keyfile, scopes=SCOPES)
        client = gspread.authorize(creds)

        # Open sheet or create if not exists
        try:
            sheet = client.open(spreadsheet_name)
        except gspread.SpreadsheetNotFound:
            logger.info("Spreadsheet '%s' not found. Creating it...", spreadsheet_name)
            sheet = client.create(spreadsheet_name)
            # Share with the user's email if possible? 
            # We can't know the email from the service account easily. 
            # We'll just print the URL.
            
        worksheet = sheet.get_worksheet(0) # First sheet
        
        # Clear existing content
        worksheet.clear()
        
        # Prepare Data for Export
        # 1. Reset Index (Ticker becomes column)
        export_df = df.copy().reset_index()
        
        # 2. US-only tickers — no suffix stripping needed
        if 'Ticker' in export_df.columns:
            export_df['Symbol'] = export_df['Ticker']
            export_df['Exchange'] = 'US'
            # Reorder
            cols = ['Symbol', 'Exchange'] + [c for c in export_df.columns if c not in ['Symbol', 'Exchange', 'Ticker']]
            export_df = export_df[cols]
            
        # 3. Handle NaN/Inf for JSON compliance
        export_df = export_df.fillna('')
        
        # 4. Convert all columns to string to avoid serialization errors?
        # gspread handles basic types mostly.
        
        # Upload
        logger.info("Uploading %d rows to Google Sheets...", len(export_df))
        worksheet.update([export_df.columns.values.tolist()] + export_df.values.tolist())
        
        logger.info("Google Sheet updated successfully: %s", sheet.url)
        
    except Exception as e:
        logger.error("Failed to update Google Sheet: %s", e)
