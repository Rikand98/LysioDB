import polars as pl
import pyreadstat as pystat
from geopy.geocoders import Nominatim, TomTom, Photon
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
from geopy.distance import geodesic, great_circle
from typing import Optional, Tuple, Union
import time
from thefuzz import fuzz
import os


class Geo:
    def __init__(self, database):
        """
        Initialize the Geo class with a reference to the database.

        Args:
            database: Instance of the Database class providing access to data and config.
        """
        self.database = database
        print("Initialization of Geo object complete.")

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
        print(df)
        print(
            f"Distance calculation complete. Added column: '{distance_col}' (meters)."
        )
        return df
