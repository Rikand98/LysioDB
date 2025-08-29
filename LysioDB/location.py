import polars as pl
import pyreadstat as pystat
from geopy.geocoders import Nominatim, TomTom, Photon
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from geopy.distance import geodesic, great_circle
from typing import Optional, Tuple, Union
import time
from thefuzz import fuzz
import os
import requests
import urllib.parse
import json
import random


class Location:
    def __init__(self, database):
        """
        Initialize the Location class with a reference to the database.

        Args:
            database: Instance of the Database class providing access to data and config.
        """
        self.database = database
        print("Initialization of Location object complete.")

    def _cast_to_struct(self, df: pl.DataFrame, col: str) -> pl.DataFrame:
        """
        Casts a column to a struct if it is a stringified JSON, otherwise ensures it is a struct.

        Args:
            df (pl.DataFrame): DataFrame containing the column.
            col (str): Column name to cast.

        Returns:
            pl.DataFrame: DataFrame with the column cast to struct.
        """
        if col not in df.columns:
            return df
        if df[col].dtype == pl.String:
            print(
                f"Warning: '{col}' is a string column. Attempting to parse as JSON to struct."
            )
            try:
                return df.with_columns(
                    pl.col(col).str.replace_all("'", '"').str.json_decode().alias(col)
                )
            except Exception as e:
                raise ValueError(f"Failed to parse '{col}' as JSON to struct: {e}")
        elif not isinstance(df[col], pl.datatypes.struct):
            raise ValueError(
                f"Column '{col}' must be a struct with {{latitude: Float64, longitude: Float64}} or a stringified JSON."
            )
        return df

    def process_addresses(
        database_path: str,
        address_col: str = "address",
        coordinate_col: str = "coordinate",
        geocoder: str = "photon",
        user_agent: str = "LysioDB_Geocoding",
        api_key: Optional[str] = None,
        batch_size: int = 100,
        max_retries: int = 3,
        sleep_seconds: float = 0.2,
        append_city: str = ", Lund, Sweden",
        fallback_coords: Tuple[float, float] = (55.703443, 13.1898098),
        path: str = "coords_db.sav",
    ) -> pl.DataFrame:
        """
        Processes addresses from a DataFrame column, geocoding them using TomTom, ArcGIS, Nominatim, Photon, or Woosmap,
        and adds a coordinate column with (latitude, longitude) structs.

        Args:
            address_col (str): Column name containing addresses.
            coordinate_col (str): Name for the new coordinate column (struct with latitude, longitude).
            geocoder_service (str): Geocoder to use ('tomtom', 'arcgis', 'nominatim', 'photon', 'woosmap').
            user_agent (str): Identifier for Nominatim/Photon APIs.
            api_key (str, optional): API key for TomTom, ArcGIS, or Woosmap.
            batch_size (int): Number of addresses to process before logging.
            max_retries (int): Max retries for failed requests.
            sleep_seconds (float): Delay between requests (0.2 for most, 1.0 for Nominatim/Photon).
            append_city (str): String to append to addresses.
            fallback_coords (tuple): (lat, lon) for failed geocodes.

        Returns:
            pl.DataFrame: Updated DataFrame with coordinate_col (struct: {latitude: Float64, longitude: Float64}).
        """
        print(
            f"\n--- Processing addresses in column '{address_col}' using {geocoder} ---"
        )

        database_df_pd, database_meta = pystat.read_sav(database_path)
        database_df = pl.from_pandas(database_df_pd)

        if address_col not in database_df.columns:
            raise ValueError(f"Address column '{address_col}' not found in DataFrame.")

        # Initialize geocoder
        if geocoder == "tomtom":
            api_key = api_key or os.getenv("TOMTOM_API_KEY")
            if not api_key:
                raise ValueError(
                    "TomTom requires an API key (set TOMTOM_API_KEY env var)."
                )
            geolocator = TomTom(api_key=api_key)
            sleep_seconds = 0.2  # ~5 req/s
        elif geocoder == "nominatim":
            geolocator = Nominatim(user_agent=user_agent)
            sleep_seconds = 1.0  # 1 req/s
        elif geocoder == "photon":
            geolocator = Photon(user_agent=user_agent)
            sleep_seconds = 1.0  # Throttled, ~1–2 req/s
        else:
            raise ValueError(
                f"Unsupported geocoder: {geocoder}. Choose 'tomtom', 'arcgis', 'nominatim', 'photon', or 'woosmap'."
            )

        def geocode_single(address, retries=max_retries):
            try:
                full_address = (
                    f"{address}{append_city}" if address and address.strip() else ""
                )
                if not full_address:
                    return None, None
                location = geolocator.geocode(full_address, timeout=10)
                if location:
                    return location.latitude, location.longitude
                return None, None
            except (GeocoderTimedOut, GeocoderUnavailable) as e:
                if retries > 0:
                    time.sleep(sleep_seconds * 2)
                    return geocode_single(address, retries - 1)
                print(f"Failed to geocode '{address}' after retries: {e}")
                return None, None

        # Check limits
        addresses = database_df[address_col].unique().to_list()
        if geocoder == "tomtom" and len(addresses) > 2500:
            print(
                f"Warning: {len(addresses)} addresses exceed TomTom free tier (2,500/day)."
            )
        elif geocoder in ("nominatim", "photon") and len(addresses) > 1000:
            print(
                f"Warning: {len(addresses)} addresses may be throttled by {geocoder} public API."
            )

        geocoded_results = {addr: (None, None) for addr in addresses}
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i : i + batch_size]
            for addr in batch:
                lat, lon = geocode_single(addr)
                geocoded_results[addr] = (lat, lon)
                time.sleep(sleep_seconds)
            print(
                f"Processed batch {i // batch_size + 1}/{len(addresses) // batch_size + 1}"
            )

        df = database_df.with_columns(
            pl.col(address_col)
            .map_elements(
                lambda x: {
                    "latitude": geocoded_results.get(x, (None, None))[0],
                    "longitude": geocoded_results.get(x, (None, None))[1],
                },
                return_dtype=pl.Struct(
                    {"latitude": pl.Float64, "longitude": pl.Float64}
                ),
            )
            .alias(coordinate_col)
        )

        null_mask = (
            pl.col(coordinate_col).struct.field("latitude").is_null()
            | pl.col(coordinate_col).struct.field("longitude").is_null()
        )
        if df.filter(null_mask).shape[0] > 0:
            print("Applying fuzzy matching fallback for failed geocodes")
            reference_addresses = {
                "Lilla Fiskaregatan 12, Lund": (55.703443, 13.1898098),
            }
            for ref_addr, (ref_lat, ref_lon) in reference_addresses.items():
                df = df.with_columns(
                    pl.when(
                        null_mask
                        & (
                            pl.col(address_col).map_elements(
                                lambda x: fuzz.ratio(x, ref_addr) > 85
                            )
                        )
                    )
                    .then(pl.lit({"latitude": ref_lat, "longitude": ref_lon}))
                    .otherwise(pl.col(coordinate_col))
                    .alias(coordinate_col)
                )

        df = df.with_columns(
            pl.col(coordinate_col)
            .map_elements(
                lambda x: {
                    "latitude": x["latitude"]
                    if x["latitude"] is not None
                    else fallback_coords[0],
                    "longitude": x["longitude"]
                    if x["longitude"] is not None
                    else fallback_coords[1],
                },
                return_dtype=pl.Struct(
                    {"latitude": pl.Float64, "longitude": pl.Float64}
                ),
            )
            .alias(coordinate_col)
        )

        pystat.write_sav(
            df.to_pandas(),
            path,
            column_labels=database_meta.column_names_to_labels,
            variable_value_labels=database_meta.variable_value_labels,
        )
        print(
            f"Geocoding complete. Added column: '{coordinate_col}' (struct: {{latitude, longitude}})."
        )
        return df

    def calculate_distance(
        self,
        coord_col: str = "coordinate",
        reference_point: Union[Tuple[float, float], str] = (55.703443, 13.1898098),
        geocoder_service: str = "photon",
        api_key: Optional[str] = None,
        user_agent: str = "LysioDB_Geocoding",
        distance_col: str = "distance_meters",
        method: str = "geodesic",
        max_retries: int = 3,
        sleep_seconds: float = 0.2,
    ) -> pl.DataFrame:
        """
        Calculates distances from a reference point (lat/lon tuple or address) to coordinates in coord_col using
        geopy's geodesic or great_circle method.

        Args:
            coord_col (str): Column name with coordinate structs ({latitude: Float64, longitude: Float64}).
            reference_point (Union[Tuple[float, float], str]): Reference point as (lat, lon) or address string.
            geocoder_service (str): Geocoder to use for reference_point if address ('tomtom', 'arcgis', 'nominatim', 'photon', 'woosmap').
            user_agent (str): Identifier for Nominatim/Photon APIs.
            api_key (str, optional): API key for TomTom, ArcGIS, or Woosmap.
            distance_col (str): Name for the new distance column (in meters).
            method (str): Distance calculation method ('geodesic' or 'great_circle').
            max_retries (int): Max retries for geocoding reference_point if address.
            sleep_seconds (float): Delay for geocoding requests (0.2 for most, 1.0 for Nominatim/Photon).

        Returns:
            pl.DataFrame: Updated DataFrame with distance_col (in meters).
        """
        print(
            f"\n--- Calculating distances from reference point to '{coord_col}' using {method} method ---"
        )

        if coord_col not in self.database.df.columns:
            raise ValueError(f"Coordinate column '{coord_col}' not found in DataFrame.")

        if not isinstance(self.database.df[coord_col], pl.datatypes.Struct):
            self.database.df = self._cast_to_struct(self.database.df, coord_col)

        if isinstance(reference_point, str):
            if geocoder_service == "tomtom":
                api_key = api_key or os.getenv("TOMTOM_API_KEY")
                if not api_key:
                    raise ValueError(
                        "TomTom requires an API key (set TOMTOM_API_KEY env var)."
                    )
                geolocator = TomTom(api_key=api_key)
                sleep_seconds = 0.2
            elif geocoder_service == "nominatim":
                geolocator = Nominatim(user_agent=user_agent)
                sleep_seconds = 1.0
            elif geocoder_service == "photon":
                geolocator = Photon(user_agent=user_agent)
                sleep_seconds = 1.0
            else:
                raise ValueError(
                    f"Unsupported geocoder: {geocoder_service}. Choose 'tomtom', 'arcgis', 'nominatim', 'photon', or 'woosmap'."
                )

            def geocode_reference(address, retries=max_retries):
                try:
                    location = geolocator.geocode(address, timeout=10)
                    if location:
                        return location.latitude, location.longitude
                    raise ValueError(
                        f"Failed to geocode reference address '{address}'."
                    )
                except (GeocoderTimedOut, GeocoderUnavailable) as e:
                    if retries > 0:
                        time.sleep(sleep_seconds * 2)
                        return geocode_reference(address, retries - 1)
                    raise ValueError(
                        f"Failed to geocode '{address}' after retries: {e}"
                    )

            reference_lat, reference_lon = geocode_reference(reference_point)
        else:
            reference_lat, reference_lon = reference_point

        if method not in ("geodesic", "great_circle"):
            raise ValueError(
                f"Unsupported distance method: {method}. Choose 'geodesic' or 'great_circle'."
            )

        distance_func = geodesic if method == "geodesic" else great_circle
        df = self.database.df.with_columns(
            pl.col(coord_col)
            .map_elements(
                lambda x: distance_func(
                    (x["latitude"], x["longitude"]), (reference_lat, reference_lon)
                ).meters
                if x["latitude"] is not None and x["longitude"] is not None
                else None,
                return_dtype=pl.Float64,
            )
            .alias(distance_col)
        )

        # Fill null distances (if any coordinates were null)
        df = df.with_columns(pl.col(distance_col).fill_null(0))

        self.database.df = df
        print(
            f"Distance calculation complete. Added column: '{distance_col}' (meters)."
        )
        return df

    def get_postnummer(
        database_path: str,
        address_col: str = "adress",
        city_col: str = "city",
        postnummer_col: str = "postnummer",
        geocoder: str = "photon",
        user_agent: str = "LysioDB_Geocoding",
        api_key: Optional[str] = None,
        batch_size: int = 100,
        max_retries: int = 3,
        sleep_seconds: float = 0.2,
        fallback_postnummer: str = "223 50",
        path: str = "processed_addresses_with_postnummer.xlsx",
    ) -> pl.DataFrame:
        """
        Retrieves postnummer for addresses in an Excel file using TomTom or Photon geocoding services,
        adding a postnummer column. Uses address and city columns directly without expansion.

        Args:
            database_path (str): Path to the Excel file containing addresses.
            address_col (str): Column name containing addresses (e.g., 'adress').
            city_col (str): Column name containing city names (e.g., 'city').
            postnummer_col (str): Name for the new postnummer column (string).
            geocoder (str): Geocoder to use ('tomtom' or 'photon').
            user_agent (str): Identifier for Photon API.
            api_key (str, optional): API key for TomTom.
            batch_size (int): Number of addresses to process before logging.
            max_retries (int): Max retries for failed requests.
            sleep_seconds (float): Delay between requests (0.2 for TomTom, 1.0 for Photon).
            fallback_postnummer (str): Fallback postnummer for failed lookups.
            path (str): Path to save the output Excel file.

        Returns:
            pl.DataFrame: Updated DataFrame with postnummer_col (string).
        """
        print(
            f"\n--- Retrieving postnummer for addresses in column '{address_col}' using {geocoder} ---"
        )

        # Read Excel file
        df = pl.read_excel(database_path)

        if address_col not in df.columns:
            raise ValueError(f"Address column '{address_col}' not found in DataFrame.")
        if city_col not in df.columns:
            raise ValueError(f"City column '{city_col}' not found in DataFrame.")

        # Initialize geocoder
        if geocoder == "tomtom":
            api_key = api_key or os.getenv("TOMTOM_API_KEY")
            if not api_key:
                raise ValueError(
                    "TomTom requires an API key (set TOMTOM_API_KEY env var)."
                )
            geolocator = TomTom(api_key=api_key)
            sleep_seconds = 0.2  # ~5 req/s
        elif geocoder == "photon":
            geolocator = Photon(user_agent=user_agent)
            sleep_seconds = 1.0  # Throttled, ~1–2 req/s
        else:
            raise ValueError(
                f"Unsupported geocoder: {geocoder}. Choose 'tomtom' or 'photon'."
            )

        def get_postnummer_single(row, retries=max_retries):
            address = row[address_col]
            city = row[city_col]
            try:
                full_address = (
                    f"{address}, {city}, Sverige"
                    if address and city and address.strip() and city.strip()
                    else ""
                )
                if not full_address:
                    return None
                location = geolocator.geocode(full_address, timeout=10)
                if location and hasattr(location, "raw"):
                    if geocoder == "tomtom":
                        return location.raw.get("address", {}).get("postalCode")
                    elif geocoder == "photon":
                        return location.raw.get("properties", {}).get("postcode")
                return None
            except (GeocoderTimedOut, GeocoderUnavailable) as e:
                if retries > 0:
                    time.sleep(sleep_seconds * 2)
                    return get_postnummer_single(row, retries - 1)
                print(
                    f"Failed to retrieve postnummer for '{full_address}' after retries: {e}"
                )
                return None

        # Check API limits
        addresses = df.select([address_col, city_col]).unique().to_dicts()
        if geocoder == "tomtom" and len(addresses) > 2500:
            print(
                f"Warning: {len(addresses)} addresses exceed TomTom free tier (2,500/day)."
            )
        elif geocoder == "photon" and len(addresses) > 1000:
            print(
                f"Warning: {len(addresses)} addresses may be throttled by Photon public API."
            )

        # Batch process addresses
        postnummer_results = {
            (row[address_col], row[city_col]): None for row in addresses
        }
        for i in range(0, len(addresses), batch_size):
            batch = addresses[i : i + batch_size]
            for row in batch:
                postnummer = get_postnummer_single(row)
                postnummer_results[(row[address_col], row[city_col])] = postnummer
                time.sleep(sleep_seconds)
            print(
                f"Processed batch {i // batch_size + 1}/{len(addresses) // batch_size + 1}"
            )

        # Add postnummer column
        df = df.with_columns(
            pl.struct([address_col, city_col])
            .map_elements(
                lambda x: postnummer_results.get(
                    (x[address_col], x[city_col]), fallback_postnummer
                ),
                return_dtype=pl.Utf8,
            )
            .alias(postnummer_col)
        )

        # Save to Excel
        df.write_excel(path)
        print(
            f"Postnummer retrieval complete. Added column: '{postnummer_col}' (string). Saved to '{path}'."
        )
        return df

    def ratsit_adresses(
        database_path: str,
        address_col: str = "Adress",
        output_columns: list = [
            "Förnamn",
            "Efternamn",
            "Namn",
            "Ålder",
            "StreetAddress",
            "City",
        ],
        api_url: str = "https://www.ratsit.se/api/search/combined",
        batch_size: int = 100,
        min_sleep_seconds: float = 1.0,
        max_sleep_seconds: float = 5.0,
        append_city: str = ", Lund, Sweden",
        path: str = "register_2.xlsx",
    ) -> pl.DataFrame:
        """
        Processes addresses from an Excel file, expands ranges and letter suffixes, and enriches them with person data
        from the Ratsit API, adding columns for first name, last name, full name, and age.

        Args:
            database_path (str): Path to the Excel file containing addresses.
            address_col (str): Column name containing addresses (e.g., 'Adress3').
            output_columns (list): List of columns to add (defaults to ['Förnamn', 'Efternamn', 'Namn', 'Ålder']).
            api_url (str): Ratsit API endpoint URL.
            batch_size (int): Number of addresses to process before logging.
            min_sleep_seconds (float): Minimum delay between requests.
            max_sleep_seconds (float): Maximum delay between requests.
            append_city (str): String to append to addresses for better API accuracy.
            path (str): Path to save the output Excel file.

        Returns:
            pl.DataFrame: DataFrame with expanded addresses and enriched person data.
        """
        print(
            f"\n--- Enriching addresses in column '{address_col}' with Ratsit API ---"
        )

        # Read Excel file
        df = pl.read_excel(database_path)

        if address_col not in df.columns:
            raise ValueError(f"Address column '{address_col}' not found in DataFrame.")

        # Define Ratsit API headers
        BASE_HEADERS = {
            "Host": "www.ratsit.se",
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.15; rv:138.0) Gecko/20100101 Firefox/138.0",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br, zstd",
            "Content-Type": "application/json",
            "Origin": "https://www.ratsit.se",
            "Sec-GPC": "1",
            "Connection": "keep-alive",
            "Cookie": "CookieConsent={stamp:%27Au+YpcG0LQVoaoC3uNDZ4ksyS7E7ucaVKj27Cj9Pff+F7uFQjiO7KQ==%27%2Cnecessary:true%2Cpreferences:false%2Cstatistics:false%2Cmarketing:false%2Cmethod:%27explicit%27%2Cver:3%2Cutc:1746610013653%2Cregion:%27se%27}; __eoi=ID=bdb222ff22ee4430:T=1746610015:RT=1746610886:S=AA-AfjbpuaFjSuDTzV2XchsqWVgB; _return_url=%7B%22Url%22%3A%22%2Fsok%2Fperson%3Fvem%3DAcaciagatan%25201%2520LIMHAMN%22%2C%22Text%22%3A%22s%5Cu00F6ket%22%7D",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "TE": "trailers",
        }

        def search_ratsit_for_person(address, max_retries=3):
            """
            Searches Ratsit API for persons associated with a given address across all pages.

            Args:
                address (str): The address to search for.

            Returns:
                tuple: (list of person dicts, status message).
            """
            if not address or not address.strip():
                return [], "Empty or invalid address"

            all_persons_data = []
            current_page = 1
            page_count = 1
            status_message = "Success"

            full_address = f"{address}{append_city}"

            while current_page <= page_count:
                payload = {
                    "who": full_address,
                    "age": ["16", "120"],
                    "phoneticSearch": True,
                    "companyName": "",
                    "orgNr": "",
                    "firstName": "",
                    "lastName": "",
                    "personNumber": "",
                    "phone": "",
                    "address": "",
                    "postnr": "",
                    "postort": "",
                    "page": current_page,
                    "url": f"/sok/person?vem={urllib.parse.quote(full_address)}&m=0&k=0&r=0&er=0&b=0&eb=0&amin=16&amax=120&fon=1&page={current_page}",
                }

                for attempt in range(max_retries):
                    try:
                        response = requests.post(
                            api_url, headers=BASE_HEADERS, json=payload, timeout=15
                        )
                        response.raise_for_status()
                        data = response.json()

                        person_data = data.get("person", {})
                        pager_data = person_data.get("pager", {})

                        if current_page == 1:
                            page_count = pager_data.get("pageCount", 1)
                            total_hits_found = person_data.get("totalHits", 0)

                        persons_on_page = person_data.get("hits", [])
                        all_persons_data.extend(persons_on_page)

                        print(
                            f"  Fetched page {current_page}/{page_count} for {full_address}. "
                            f"Found {len(persons_on_page)} individuals on this page. "
                            f"Total collected: {len(all_persons_data)}."
                        )

                        break  # Success, exit retry loop

                    except requests.exceptions.RequestException as e:
                        if attempt < max_retries - 1:
                            time.sleep(2)
                            continue
                        status_message = f"Request failed for page {current_page} of address '{full_address}': {e}"
                        print(status_message)
                        return all_persons_data, status_message

                    except json.JSONDecodeError:
                        status_message = f"JSON decode error for page {current_page} of address '{full_address}'."
                        print(status_message)
                        return all_persons_data, status_message

                    except KeyError as e:
                        status_message = f"Key error parsing JSON for page {current_page} of address '{full_address}': {e}"
                        print(status_message)
                        return all_persons_data, status_message

                current_page += 1
                if current_page <= page_count:
                    page_delay = random.uniform(min_sleep_seconds, max_sleep_seconds)
                    print(
                        f"  Waiting {page_delay:.2f} seconds before fetching next page..."
                    )
                    time.sleep(page_delay)

            if not all_persons_data and status_message == "Success":
                status_message = f"No persons found at address '{full_address}'"
                print(status_message)

            return all_persons_data, status_message

        # Process addresses
        addresses = df[address_col].unique().to_list()
        final_rows = []

        for i, address in enumerate(addresses):
            if i % batch_size == 0 and i > 0:
                print(
                    f"Processed batch {i // batch_size}/{len(addresses) // batch_size + 1}"
                )

            # Filter original rows for this address
            original_rows = df.filter(pl.col(address_col) == address)
            if original_rows.is_empty():
                original_rows = df.filter(pl.col(address_col) == address)

            persons, status = search_ratsit_for_person(address)

            if persons:
                for person in persons:
                    row = original_rows.to_dicts()[0].copy()
                    row["Förnamn"] = f"{person.get('firstName', '')}".strip()
                    row["Efternamn"] = f"{person.get('lastName', '')}".strip()
                    row["Namn"] = (
                        f"{person.get('firstName', '')} {person.get('lastName', '')}".strip()
                    )
                    row["Ålder"] = str(person.get("age", "N/A"))
                    row["StreetAddress"] = f"{person.get('streetAddress', '')}".strip()
                    row["City"] = f"{person.get('city', '')}".strip()
                    final_rows.append(row)
            else:
                row = original_rows.to_dicts()[0].copy()
                row["Förnamn"] = "No persons found"
                row["Efternamn"] = "No persons found"
                row["Namn"] = "No persons found"
                row["Ålder"] = "N/A"
                row["StreetAddress"] = "No persons found"
                row["City"] = "No persons found"
                row["Status"] = status
                final_rows.append(row)

            if i < len(addresses) - 1:
                sleep_duration = random.uniform(min_sleep_seconds, max_sleep_seconds)
                print(f"Waiting {sleep_duration:.2f} seconds before next address...")
                time.sleep(sleep_duration)

        # Create final DataFrame
        if final_rows:
            df_final = pl.DataFrame(final_rows)
        else:
            print("No data collected. Creating an empty DataFrame.")
            schema = {
                **{col: pl.String for col in df.columns},
                **{col: pl.String for col in output_columns},
                "Status": pl.String,
            }
            df_final = pl.DataFrame({}, schema=schema)

        # Save to Excel
        df_final.write_excel(path)
        print(
            f"Ratsit enrichment complete. Added columns: {output_columns}. Saved to '{path}'."
        )
        return df_final
