import requests
from concurrent.futures import ThreadPoolExecutor

import global_conf_variables

params = global_conf_variables.ParamsDict()

brands = params.get_value('brands')
geographic_regions = params.get_value('geographic_regions')
fuel_type = params.get_value('fuel_type')
site_details = params.get_value('site_details')
sites_prices = params.get_value('sites_prices')
country_id = params.get_value('country_id')
region_level = params.get_value('region_level')
region_id = params.get_value('region_id')


class FuelDataAPI:
    def __init__(self, token):
        self.headers = {
            'Authorization': f"FPDAPI SubscriberToken={token}",
            'Content-Type': "application/json",
            'cache-control': "no-cache",
        }
        self.urls = {
            "brands": brands,
            "geographic_regions": geographic_regions,
            "fuel_type": fuel_type,
            "site_details": site_details,
            "site_prices": sites_prices
        }
        self.param_1 = {"countryId": country_id}
        self.param_2 = {"countryId": country_id, "geoRegionLevel": region_level, "geoRegionId": region_id}

    def make_request(self, key):
        """Performs the request for the given URL key."""
        if key in ["site_details", "site_prices"]:
            param = self.param_2
        else:
            param = self.param_1
        url = self.urls[key]
        response = requests.get(url, headers=self.headers, params=param)
        return key, response.json()

    def fetch_all_data(self):
        """Fetches all API data concurrently using threads."""
        with ThreadPoolExecutor(max_workers=5) as executor:
            future_to_key = {executor.submit(self.make_request, key): key for key in self.urls}
            results = {}
            for future in future_to_key:
                key = future_to_key[future]
                try:
                    key, data = future.result()
                    results[key] = data
                except Exception as e:
                    results[key] = f"Request failed: {str(e)}"
        return results

