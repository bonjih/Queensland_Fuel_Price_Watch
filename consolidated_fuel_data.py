# consolidated_fuel_data.py

from datetime import datetime
import pandas as pd


def consolidate_fuel_tables(frames):
    df_site_details = frames.get('site_details', pd.DataFrame())
    df_site_prices = frames.get('site_prices', pd.DataFrame())
    df_brands = frames.get('brands', pd.DataFrame()).rename(columns={'Name': 'Brand'})
    df_fuel_type = frames.get('fuel_type', pd.DataFrame())
    df_geographic_regions = frames.get('geographic_regions', pd.DataFrame())

    # Merge site details with site prices on 'S' (SiteID) and 'SiteId'
    merged_df = pd.merge(df_site_details, df_site_prices, left_on='S', right_on='SiteId', how='inner')

    columns_to_drop = ['G4', 'G5', 'MO', 'MC', 'TO', 'TC', 'WO', 'WC', 'THO', 'THC', 'FO',
                       'FC', 'SO', 'SC', 'SUO', 'SUC', 'CollectionMethod', 'SiteId', 'M', 'GPI']
    merged_df = merged_df.drop(columns=columns_to_drop, errors='ignore')

    # Merge with geographic regions on 'G1' and 'GeoRegionId', renaming 'Name' to 'Suburb'
    merged_df = pd.merge(merged_df, df_geographic_regions, left_on='G1', right_on='GeoRegionId', how='inner')
    merged_df = merged_df.rename(columns={'Name': 'Suburb'})

    # Merge with brands on 'B' and 'BrandId'
    merged_df = pd.merge(merged_df, df_brands, left_on='B', right_on='BrandId', how='inner')

    # Merge with fuel types on 'FuelId'
    merged_df = pd.merge(merged_df, df_fuel_type, on='FuelId', how='inner').rename(columns={'Name': 'FuelType'})

    merged_df = merged_df.drop(['B', 'Abbrev', 'G1', 'G2', 'G3', 'GeoRegionId', 'GeoRegionLevel', 'GeoRegionParentId'
                                ], axis=1, errors='ignore')

    column_renames = {
        'S': 'SiteId', 'A': 'Address', 'N': 'Name', 'P': 'Postcode',
        'Price': 'FuelPrice'
    }
    merged_df = merged_df.rename(columns=column_renames)

    merged_df['Updated'] = datetime.now()

    return merged_df
