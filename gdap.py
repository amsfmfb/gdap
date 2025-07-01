#!/usr/bin/env python3
"""
Geocode Districts for Active Participants Script

This script uses an Excel file report which queries all active participants with address data and add these columns:
1. Geocoding (lat/lon)
2. San Francisco Supervisorial Districts
3. Marin County Supervisor Districts
4. Congressional Districts
5. Census PUMA Districts
6. Census Tracts
7. Census Blocks
8. California State Assembly Districts
9. California State Senate Districts
10. Geocoding Status
11. Last Updated Timestamp

It uses the Nominatim geocoding service for address lookup and various APIs for district look
"""

import pandas as pd # type: ignore
import requests # type: ignore
import json
import time
from geopy.geocoders import Nominatim # type: ignore
from geopy.exc import GeocoderTimedOut, GeocoderServiceError # type: ignore
import logging
from datetime import datetime
import os

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class DistrictLookup:
    def __init__(self, excel_file_path):
        self.excel_file_path = excel_file_path
        self.df = None
        self.geolocator = Nominatim(user_agent="district_lookup_v1.0")
        self.session = requests.Session()
        
        # Limit Rate of API calls 
        self.geocode_delay = 1.1  # Nominatim use policy: 1 second between requests 
        self.api_delay = 0.5
        
    def load_data(self):
        """Load Excel file into DataFrame."""
        try:
            self.df = pd.read_excel(self.excel_file_path)
            logger.info(f"Loaded {len(self.df)} records from {self.excel_file_path}")
            
            # Add columns for new geo data
            new_columns = [
                'Latitude', 'Longitude', 'Geocoded_Address',
                'SF_Supervisorial_District',
                'Marin_Supervisor_District',
                'Congressional_District',
                'Census_PUMA', 'Census_Tract', 'Census_Block',
                'CA_Assembly_District',
                'CA_Senate_District',
                'Geocoding_Status', 'Last_Updated'
            ]
            
            for col in new_columns:
                if col not in self.df.columns:
                    self.df[col] = None
                    
            return True
            
        except Exception as e:
            logger.error(f"Error loading Excel: {e}")
            return False
    
    def geocode_address(self, address, city, zip_code):
        """Geocode to get latlon coordinates."""
        try:
            # Build full address
            full_address = f"{address}, {city}, CA {zip_code}"
            
            time.sleep(self.geocode_delay)
            
            location = self.geolocator.geocode(full_address, timeout=10)
            
            if location:
                return {
                    'latitude': location.latitude,
                    'longitude': location.longitude,
                    'formatted_address': location.address,
                    'status': 'Success'
                }
            else:
                return {'status': 'Not Found'}
                
        except (GeocoderTimedOut, GeocoderServiceError) as e:
            logger.warning(f"Geocoding error for {full_address}: {e}")
            return {'status': f'Error: {str(e)}'}
        except Exception as e:
            logger.error(f"Unexpected geocoding error: {e}")
            return {'status': f'Unexpected Error: {str(e)}'}
    
    def get_sf_supervisorial_district(self, lat, lon):
        """Get SF Supervisorial District."""
        try:
            time.sleep(self.api_delay)
            
            url = "https://services3.arcgis.com/iOy5B2EVhg9OAGCE/arcgis/rest/services/Supervisor_Districts/FeatureServer/0/query"
            
            params = {
                'where': '1=1',
                'geometry': f'{lon},{lat}',
                'geometryType': 'esriGeometryPoint',
                'spatialRel': 'esriSpatialRelWithin',
                'outFields': '*',
                'returnGeometry': 'false',
                'f': 'json'
            }
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            if 'features' in data and data['features']:
                feature = data['features'][0]
                attributes = feature['attributes']
                
                return {
                    'district': attributes.get('DISTRICT', 'Unknown'),
                }
            
            return {'district': None}
            
        except Exception as e:
            logger.warning(f"SF District lookup error: {e}")
            return {'district': 'Error'}
    
    def get_marin_supervisor_district(self, lat, lon):
        """Get Marin County Supervisor District."""
        try:
            time.sleep(self.api_delay)
            
            # Marin County GIS services
            url = "https://gis.marincounty.org/server/rest/services/Boundaries/Supervisor_Districts/MapServer/0/query"
            
            params = {
                'where': '1=1',
                'geometry': f'{lon},{lat}',
                'geometryType': 'esriGeometryPoint',
                'spatialRel': 'esriSpatialRelWithin',
                'outFields': '*',
                'returnGeometry': 'false',
                'f': 'json'
            }
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            if 'features' in data and data['features']:
                feature = data['features'][0]
                attributes = feature['attributes']
                
                return {
                    'district': attributes.get('DISTRICT', 'Unknown'),
                }
            return {'district': None}
            
        except Exception as e:
            logger.warning(f"Marin District lookup error: {e}")
            return {'district': 'Error'}
    
    def get_census_data(self, lat, lon):
        """Get Census PUMA, Census Tract, and Census Block."""
        try:
            time.sleep(self.api_delay)
            
            # Use Census Geocoding API
            url = "https://geocoding.geo.census.gov/geocoder/geographies/coordinates"
            
            params = {
                'x': lon,
                'y': lat,
                'benchmark': 'Public_AR_Current',
                'vintage': 'Current_Current',
                'format': 'json'
            }
            
            response = self.session.get(url, params=params, timeout=20)
            response.raise_for_status()
            
            data = response.json()
            
            if 'result' in data and 'geographies' in data['result']:
                geographies = data['result']['geographies']
                
                # Extract different geographic levels
                puma = None
                tract = None
                block = None
                
                # PUMA (Public Use Microdata Area)
                if 'Public Use Microdata Areas' in geographies:
                    puma_data = geographies['Public Use Microdata Areas']
                    if puma_data:
                        puma = puma_data[0].get('PUMA', None)
                
                # Census Tract
                if 'Census Tracts' in geographies:
                    tract_data = geographies['Census Tracts']
                    if tract_data:
                        tract = tract_data[0].get('TRACT', None)
                
                # Census Block
                if 'Census Blocks' in geographies:
                    block_data = geographies['Census Blocks']
                    if block_data:
                        block = block_data[0].get('BLOCK', None)
                
                return {
                    'puma': puma,
                    'tract': tract,
                    'block': block
                }
            
            return {'puma': None, 'tract': None, 'block': None}
            
        except Exception as e:
            logger.warning(f"Census data lookup error: {e}")
            return {'puma': 'Error', 'tract': 'Error', 'block': 'Error'}
    
    def get_political_districts(self, lat, lon):
        """Get Congressional, Assembly, and Senate districts."""
        try:
            time.sleep(self.api_delay)
            
            # Use FCC API for political districts
            url = "https://geo.fcc.gov/api/census/area"
            
            params = {
                'lat': lat,
                'lon': lon,
                'format': 'json'
            }
            
            response = self.session.get(url, params=params, timeout=15)
            response.raise_for_status()
            
            data = response.json()
            
            if 'results' in data and data['results']:
                result = data['results'][0]
                
                return {
                    'congressional': result.get('congress_district', None),
                    'assembly': result.get('state_lower_district', None),
                    'senate': result.get('state_upper_district', None)
                }
            
            return {'congressional': None, 'assembly': None, 'senate': None}
            
        except Exception as e:
            logger.warning(f"Political districts lookup error: {e}")
            return {'congressional': 'Error', 'assembly': 'Error', 'senate': 'Error'}

    def process_records(self):
        """Process all records in the DataFrame."""
        if self.df is None:
            logger.error("No data loaded. Please run load_data() first.")
            return False
        
        total_records = len(self.df)
        processed = 0
        
        logger.info(f"Starting to process {total_records} records...")
        
        for index, row in self.df.iterrows():
            try:
                logger.info(f"Processing record {index + 1}/{total_records}")
                
                # Skip if has coordinates
                if pd.notna(row.get('Latitude')) and pd.notna(row.get('Longitude')):
                    logger.info(f"Record {index + 1} already has coordinates, skipping geocoding")
                    lat, lon = row['Latitude'], row['Longitude']
                else:
                    # Geocode address
                    address = row.get('Person Address', '')
                    city = row.get('Person city', '')
                    zip_code = row.get('Person Zip Code', '')
                    
                    if not all([address, city, zip_code]):
                        logger.warning(f"Missing address data for record {index + 1}")
                        self.df.at[index, 'Geocoding_Status'] = 'Missing Address Data'
                        continue
                    
                    geocode_result = self.geocode_address(address, city, zip_code)
                    
                    if geocode_result['status'] == 'Success':
                        lat = geocode_result['latitude']
                        lon = geocode_result['longitude']
                        
                        self.df.at[index, 'Latitude'] = lat
                        self.df.at[index, 'Longitude'] = lon
                        self.df.at[index, 'Geocoded_Address'] = geocode_result['formatted_address']
                        self.df.at[index, 'Geocoding_Status'] = 'Success'
                    else:
                        self.df.at[index, 'Geocoding_Status'] = geocode_result['status']
                        logger.warning(f"Geocoding failed for record {index + 1}: {geocode_result['status']}")
                        continue
                
                # Get SF Sups District
                if 'san francisco' in str(city).lower():
                    sf_district = self.get_sf_supervisorial_district(lat, lon)
                    self.df.at[index, 'SF_Supervisorial_District'] = sf_district['district']
                    self.df.at[index, 'SF_Supervisor'] = sf_district['supervisor']
                
                # Get Marin County Sups District
                if any(marin_city in str(city).lower() for marin_city in ['san rafael', 'novato', 'mill valley', 'tiburon', 'sausalito', 'corte madera', 'larkspur', 'fairfax', 'san anselmo', 'ross', 'kentfield', 'belvedere']):
                    marin_district = self.get_marin_supervisor_district(lat, lon)
                    self.df.at[index, 'Marin_Supervisor_District'] = marin_district['district']
                    self.df.at[index, 'Marin_Supervisor'] = marin_district['supervisor']
                
                # Get Census data
                census_data = self.get_census_data(lat, lon)
                self.df.at[index, 'Census_PUMA'] = census_data['puma']
                self.df.at[index, 'Census_Tract'] = census_data['tract']
                self.df.at[index, 'Census_Block'] = census_data['block']
                
                # Get Political districts
                political_districts = self.get_political_districts(lat, lon)
                self.df.at[index, 'Congressional_District'] = political_districts['congressional']
                self.df.at[index, 'CA_Assembly_District'] = political_districts['assembly']
                self.df.at[index, 'CA_Senate_District'] = political_districts['senate']
                
                # Update timestamp
                self.df.at[index, 'Last_Updated'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                
                processed += 1
                
                # Save progress every 10 records
                if processed % 10 == 0:
                    self.save_progress()
                    logger.info(f"Progress saved. Processed {processed}/{total_records} records")
                
            except Exception as e:
                logger.error(f"Error processing record {index + 1}: {e}")
                self.df.at[index, 'Geocoding_Status'] = f'Processing Error: {str(e)}'
                continue
        
        logger.info(f"Completed. Successfully processed {processed}/{total_records} records")
        return True
    
    def save_progress(self):
        """Save progress to temp file."""
        try:
            temp_filename = f"temp_geocode_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
            self.df.to_excel(temp_filename, index=False)
            logger.info(f"Progress saved to {temp_filename}")
        except Exception as e:
            logger.error(f"Error saving progress: {e}")
    
    def export_results(self, output_filename=None):
        """Export to Excel."""
        if output_filename is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_filename = f"Geocoded_ActiveParticipants_{timestamp}.xlsx"
        
        try:
            self.df.to_excel(output_filename, index=False)
            logger.info(f"Results exported to {output_filename}")
            return output_filename
        except Exception as e:
            logger.error(f"Error exporting: {e}")
            return None

def main():
    """Main function to run the district lookup process."""
    
    # Update excel file path
    excel_file_path = "excel_filepath.xlsx" 
    
    # Initialize the lookup class
    lookup = DistrictLookup(excel_file_path)
    
    # Load data
    if not lookup.load_data():
        logger.error("Failed to load data. Exiting.")
        return
    
    # Process records
    if not lookup.process_records():
        logger.error("Failed to process records.")
        return
    
    # Export results
    output_file = lookup.export_results()
    if output_file:
        logger.info(f"Process completed successfully. Output saved to: {output_file}")
    else:
        logger.error("Failed to export results.")

if __name__ == "__main__":
    main()