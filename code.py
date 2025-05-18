

import streamlit as st
import pandas as pd
import base64
import requests
import xml.etree.ElementTree as ET
from html import unescape
from bs4 import BeautifulSoup
import textwrap
from datetime import datetime
import os

# Constants
URL = "https://cdr.ffiec.gov/public/pws/webservices/retrievalservice.asmx"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
    "Content-Type": "application/soap+xml; charset=utf-8",
    "SOAPAction": "http://cdr.ffiec.gov/public/services/RetrieveFacsimile"
}

def make_soap_body(rssd_id: int, period_end_date: str, username: str, passphrase: str) -> str:
    """Build the SOAP envelope, injecting the RSSD ID and period end date dynamically."""
    raw = f"""\
    <?xml version="1.0" encoding="utf-8"?>
    <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                     xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                     xmlns:soap12="http://www.w3.org/2003/05/soap-envelope"
                     xmlns:wsa="http://www.w3.org/2005/08/addressing">
      <soap12:Header>
        <wsa:Action>http://cdr.ffiec.gov/public/services/RetrieveFacsimile</wsa:Action>
        <wsa:To>https://cdr.ffiec.gov/public/pws/webservices/retrievalservice.asmx</wsa:To>
        <wsse:Security xmlns:wsse="http://docs.oasis-open.org/wss/2004/01/oasis-200401-wss-wssecurity-secext-1.0.xsd">
          <wsse:UsernameToken>
            <wsse:Username>{username}</wsse:Username>
            <wsse:Password>{passphrase}</wsse:Password>
          </wsse:UsernameToken>
        </wsse:Security>
      </soap12:Header>
      <soap12:Body>
        <RetrieveFacsimile xmlns="http://cdr.ffiec.gov/public/services">
          <dataSeries>Call</dataSeries>
          <reportingPeriodEndDate>{period_end_date}</reportingPeriodEndDate>
          <fiIDType>ID_RSSD</fiIDType>
          <fiID>{rssd_id}</fiID>
          <facsimileFormat>XBRL</facsimileFormat>
        </RetrieveFacsimile>
      </soap12:Body>
    </soap12:Envelope>"""
    return textwrap.dedent(raw).strip()

def fetch_facsimile(url: str, headers: dict, body: str) -> requests.Response:
    response = requests.post(url, data=body, headers=headers)
    response.raise_for_status()
    return response

def parse_xbrl_to_dataframe(content: str, rssd_id: str) -> pd.DataFrame:
    content = unescape(content)
    soup = BeautifulSoup(content, "xml")
    xbrl_tag = soup.find("xbrl")
    if not xbrl_tag:
        raise ValueError("No <xbrl> element found in the file!")
    records = []
    for tag in xbrl_tag.find_all():
        if tag.has_attr("decimals"):
            fact_id = tag.name.split(":")[-1]
            records.append({
                "rssd_id": rssd_id,
                "id": fact_id,
                "value": tag.get_text(strip=True),
                "decimal": tag["decimals"]
            })
    return pd.DataFrame(records)

def get_mapping_dict():
    """Load and process the MDRM mapping file."""
    # Make sure the downloads/taxonomy/MDRM directory and MDRM_CSV.csv file exist
    # before attempting to read the CSV.
    mdrm_df = pd.read_csv(
        "downloads/taxonomy/MDRM/MDRM_CSV.csv",
        skiprows=1,
        dtype={"Mnemonic": str, "Item Code": str}
    )

    mdrm_df["metric"] = (
        mdrm_df["Mnemonic"].str.strip() +
        mdrm_df["Item Code"].str.zfill(4)
    )

    return pd.Series(
        mdrm_df["Item Name"].values,
        index=mdrm_df["metric"]
    ).to_dict()

def process_rssd_id(rssd_id: str, period_end_date: str, username: str, passphrase: str) -> pd.DataFrame:
    """Process a single RSSD ID and return its DataFrame."""
    try:
        # Make API request
        soap_body = make_soap_body(int(rssd_id), period_end_date, username, passphrase)
        response = fetch_facsimile(URL, HEADERS, soap_body)

        # Parse response
        namespaces = {
            "soap": "http://schemas.xmlsoap.org/soap/envelope/",
            "ns": "http://cdr.ffiec.gov/public/services"
        }
        root = ET.fromstring(response.text)
        result = root.find(".//ns:RetrieveFacsimileResult", namespaces)

        if result is None or not result.text:
            st.warning(f"No data found for RSSD ID: {rssd_id}")
            return None

        # Decode and parse XBRL
        decoded = base64.b64decode(result.text).decode('utf-8')
        return parse_xbrl_to_dataframe(decoded, rssd_id)

    except Exception as e:
        st.error(f"Error processing RSSD ID {rssd_id}: {str(e)}")
        return None

def main():
    st.title("Call Report Downloader")

    # Sidebar for credentials
    with st.sidebar:
        st.header("Credentials")
        username = st.text_input("Username:", value="mbambal")
        passphrase = st.text_input("Passphrase:", value="IuwnFdSSpFRzsRTX9dKx", type="password")

    # Main interface
    st.header("Input Parameters")

    # Input fields
    rssd_ids_input = st.text_input(
        "Enter RSSD IDs (comma-separated):",
        value="1842065",
        help="Enter multiple RSSD IDs separated by commas (e.g., 1842065, 1842066)"
    )

    period_end_date = st.date_input(
        "Select Period End Date:",
        value=datetime(2019, 3, 31),
        format="YYYY/MM/DD"
    )

    if st.button("Download Call Reports"):
        try:
            # Format date for API
            formatted_date = period_end_date.strftime("%Y/%m/%d")

            # Process RSSD IDs
            rssd_ids = [id.strip() for id in rssd_ids_input.split(',') if id.strip()]

            if not rssd_ids:
                st.error("Please enter at least one RSSD ID")
                return

            # Initialize empty list to store DataFrames
            all_dfs = []

            # Progress bar
            progress_bar = st.progress(0)
            status_text = st.empty()

            # Process each RSSD ID
            for i, rssd_id in enumerate(rssd_ids):
                status_text.text(f"Processing RSSD ID: {rssd_id}")

                # Process the RSSD ID
                df = process_rssd_id(rssd_id, formatted_date, username, passphrase)
                if df is not None:
                    all_dfs.append(df)

                # Update progress
                progress_bar.progress((i + 1) / len(rssd_ids))

            if not all_dfs:
                st.error("No data was retrieved for any RSSD ID.")
                return

            # Combine all DataFrames
            combined_df = pd.concat(all_dfs, ignore_index=True)

            # Display raw data
            st.subheader("Raw Data")
            st.dataframe(combined_df)

            # Apply mapping
            # Ensure the mapping file is available or handle the FileNotFoundError
            try:
                mapping_dict = get_mapping_dict()
                combined_df["label"] = combined_df["id"].map(mapping_dict).fillna("Unknown metric")

                # Display mapped data
                st.subheader("Mapped Data")
                st.dataframe(combined_df)
            except FileNotFoundError:
                st.warning("MDRM mapping file not found. Mapped data will not be available.")
                # If mapping file is not found, just display the raw data again
                st.subheader("Mapped Data (Mapping file not found)")
                st.dataframe(combined_df)


            # Save to CSV button
            csv = combined_df.to_csv(index=False)
            st.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"call_reports_{formatted_date.replace('/', '_')}.csv",
                mime="text/csv"
            )

        except Exception as e:
            st.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()