import pandas as pd
import requests

# Function to get the state FIPS code using a third-party API (Zippopotam)
def get_state_fips(zip_code):
    try:
        zippopotam_url = f"http://api.zippopotam.us/us/{zip_code}"
        response = requests.get(zippopotam_url)
        response.raise_for_status()  # Check for request errors
        data = response.json()
        state_abbreviation = data['places'][0]['state abbreviation']

        state_fips_mapping = {
            "AL": "01", "AK": "02", "AZ": "04", "AR": "05", "CA": "06", "CO": "08", "CT": "09",
            "DE": "10", "FL": "12", "GA": "13", "HI": "15", "ID": "16", "IL": "17", "IN": "18",
            "IA": "19", "KS": "20", "KY": "21", "LA": "22", "ME": "23", "MD": "24", "MA": "25",
            "MI": "26", "MN": "27", "MS": "28", "MO": "29", "MT": "30", "NE": "31", "NV": "32",
            "NH": "33", "NJ": "34", "NM": "35", "NY": "36", "NC": "37", "ND": "38", "OH": "39",
            "OK": "40", "OR": "41", "PA": "42", "RI": "44", "SC": "45", "SD": "46", "TN": "47",
            "TX": "48", "UT": "49", "VT": "50", "VA": "51", "WA": "53", "WV": "54", "WI": "55",
            "WY": "56"
        }
        return state_fips_mapping.get(state_abbreviation, None)
    
    except requests.exceptions.HTTPError as http_err:
        print(f"Error fetching state FIPS code for ZIP {zip_code}: {http_err}")
        return None

# Function to fetch census data for a ZIP code
def fetch_census_data_for_zip(zip_code, state_code, api_key):
    try:
        url = (f"https://api.census.gov/data/2019/acs/acs5?get=B01003_001E,B19013_001E,B01002_001E,"
               f"B15003_001E,B17001_002E,B02001_002E,B02001_003E,B02001_005E,B03002_012E,"
               f"B23025_003E,B23025_005E,B15003_017E,B25077_001E,B25003_002E,B25003_003E,"
               f"B19001_001E,B19001_002E,B19001_017E,B01001_001E,B01001_020E,B01001_044E"
               f"&for=zip%20code%20tabulation%20area:{zip_code}&in=state:{state_code}&key={api_key}")
        
        response = requests.get(url)
        response.raise_for_status()  # Check for request errors
        data = response.json()

        # Map variable codes to descriptive labels
        labels = {
            "B01003_001E": "Total Population",
            "B19013_001E": "Median Household Income",
            "B01002_001E": "Median Age",
            "B15003_001E": "Total Population Aged 25 and Over",
            "B15003_017E": "Bachelor's Degree or Higher",
            "B17001_002E": "Population Below Poverty Level",
            "B02001_002E": "White Population",
            "B02001_003E": "Black or African American Population",
            "B02001_005E": "Asian Population",
            "B03002_012E": "Hispanic or Latino Population",
            "B23025_003E": "Employed Population",
            "B23025_005E": "Unemployed Population",
            "B25077_001E": "Median Home Value",
            "B25003_002E": "Owner-Occupied Housing Units",
            "B25003_003E": "Renter-Occupied Housing Units",
            "B19001_001E": "Total Households",
            "B19001_002E": "Households Earning Less Than $10,000",
            "B19001_017E": "Households Earning More Than $200,000",
            "B01001_001E": "Total Population by Age",
            "B01001_020E": "Male Population Aged 25 to 29",
            "B01001_044E": "Female Population Aged 65 and Over",
            "zip code tabulation area": "ZIP Code"
        }

        # Extract and map the data to labels
        header = data[0]
        values = data[1]
        labeled_data = {labels.get(header[i], header[i]): values[i] for i in range(len(header))}
        
        # Overwrite 'ZIP Code' to ensure consistency
        labeled_data["ZIP Code"] = zip_code

        return labeled_data
    
    except requests.exceptions.HTTPError as http_err:
        return {"error": f"HTTP error occurred: {http_err}", "ZIP Code": zip_code}
    
    except Exception as err:
        return {"error": f"An error occurred: {err}", "ZIP Code": zip_code}

# Function to fetch and append data for multiple ZIP codes
def fetch_data_for_multiple_zip_codes(zip_codes, api_key):
    all_data = []
    
    # Ensure ZIP codes are 5-digit strings
    zip_codes = [str(z).zfill(5) for z in zip_codes]
    
    # Load already processed ZIP codes and census data
    try:
        processed_zips_df = pd.read_csv("proc_post.csv", dtype={'ZIP Code': str})
        processed_zips = processed_zips_df['ZIP Code'].tolist()
    except FileNotFoundError:
        processed_zips = []

    try:
        census_df = pd.read_csv("census_df.csv", dtype={'ZIP Code': str})
    except FileNotFoundError:
        census_df = pd.DataFrame()  # Create an empty DataFrame if file doesn't exist

    # Remove already processed ZIP codes from the list
    new_zip_codes = [z for z in zip_codes if z not in processed_zips]

    # Process only the new ZIP codes
    for zip_code in new_zip_codes:
        # Fetch FIPS code for the ZIP code
        state_fips = get_state_fips(zip_code)

        if state_fips:
            data = fetch_census_data_for_zip(zip_code, state_fips, api_key)
            all_data.append(data)
            processed_zips.append(zip_code)

            # Every 5 ZIP codes, save data
            if len(all_data) % 5 == 0:
                new_df = pd.DataFrame(all_data)
                new_df['ZIP Code'] = new_df['ZIP Code'].astype(str)
                census_df = pd.concat([census_df, new_df], ignore_index=True).drop_duplicates(subset=["ZIP Code"])

                # Save updated census data and processed ZIP codes
                census_df.to_csv("census_df.csv", index=False)
                pd.DataFrame({"ZIP Code": processed_zips}).drop_duplicates().to_csv("proc_post.csv", index=False)
                
                all_data = []  # Reset batch

    # Final save for remaining data
    if all_data:
        new_df = pd.DataFrame(all_data)
        new_df['ZIP Code'] = new_df['ZIP Code'].astype(str)
        census_df = pd.concat([census_df, new_df], ignore_index=True).drop_duplicates(subset=["ZIP Code"])
        census_df.to_csv("census_df.csv", index=False)
        pd.DataFrame({"ZIP Code": processed_zips}).drop_duplicates().to_csv("proc_post.csv", index=False)

    return census_df

# Example usage
api_key = ''  # Replace with your actual Census API key
zip_codes = ['22406', '90210', '10001','94568']  # Example ZIP codes
census_df = fetch_data_for_multiple_zip_codes(zip_codes, api_key)

