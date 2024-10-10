import requests
import os
from dotenv import load_dotenv

# Function to get the state FIPS code using a third-party API (Zippopotam)
def get_state_fips(zip_code):
    try:
        # Zippopotam API to get state information
        zippopotam_url = f"http://api.zippopotam.us/us/{zip_code}"
        response = requests.get(zippopotam_url)
        
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()

        # Parse the response to get the state abbreviation
        data = response.json()
        state_abbreviation = data['places'][0]['state abbreviation']

        # Mapping of state abbreviations to FIPS codes
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

        # Return the corresponding state FIPS code
        return state_fips_mapping.get(state_abbreviation, None)
    
    except requests.exceptions.HTTPError as http_err:
        print(f"Error fetching state FIPS code for ZIP {zip_code}: {http_err}")
        return None

# Function to fetch census data
def fetch_census_data_for_zip(zip_code, state_code, api_key):
    try:
        # Include the state code in the URL request
        url = (f"https://api.census.gov/data/2019/acs/acs5?get=B01003_001E,B19013_001E,B01002_001E,"
               f"B15003_001E,B17001_002E,B02001_002E,B02001_003E,B02001_005E,B03002_012E,"
               f"B23025_003E,B23025_005E,B15003_017E,B25077_001E,B25003_002E,B25003_003E,"
               f"B19001_001E,B19001_002E,B19001_017E,B01001_001E,B01001_020E,B01001_044E"
               f"&for=zip%20code%20tabulation%20area:{zip_code}&in=state:{state_code}&key={api_key}")
        
        # Sending the request
        response = requests.get(url)
        
        # Raise an exception if the request was unsuccessful
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()

        # Map the variable codes to descriptive labels
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

        # Extract the data and map it to labels
        header = data[0]  # The variable names
        values = data[1]  # The actual data

        # Create a dictionary with labeled output
        labeled_data = {labels.get(header[i], header[i]): values[i] for i in range(len(header))}
        
        return labeled_data
    
    except requests.exceptions.HTTPError as http_err:
        return {"error": f"HTTP error occurred: {http_err}"}
    
    except Exception as err:
        return {"error": f"An error occurred: {err}"}

def fetch_data_for_multiple_zip_codes(zip_codes, api_key):
    all_data = []
    
    # Loop through the list of ZIP codes and fetch data for each
    for zip_code in zip_codes:
        # Get the state FIPS code for the ZIP code
        state_fips = get_state_fips(zip_code)
        
        if state_fips:
            data = fetch_census_data_for_zip(zip_code, state_fips, api_key)
            all_data.append({zip_code: data})
        else:
            all_data.append({zip_code: {"error": "State FIPS code not found"}})
    
    return all_data

# Example usage
if __name__ == "__main__":
    # Replace with your actual U.S. Census API key
    api_key = ''
    # load_dotenv()
    # api_key = os.getenv('CENSUS_API_KEY')
    
    # List of ZIP codes you want to query
    zip_codes = ['22406', '90210', '10001']  # Example ZIP codes
    
    # Fetch the census data for all ZIP codes
    all_census_data = fetch_data_for_multiple_zip_codes(zip_codes, api_key)
    
    # Print the results
    for zip_data in all_census_data:
        print(zip_data)
