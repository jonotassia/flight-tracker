# Amadeus Python API: https://github.com/amadeus4dev/amadeus-python

import amadeus
import logging
import json
import pandas as pd
import boto3
from matplotlib import pyplot as plt

logger = logging.Logger("logger")
logger.setLevel(logging.DEBUG)

ssm = boto3.client("ssm")

client_id = ssm.get_parameter(Name="AMADEUS_CLIENT_ID", WithDecryption=True)['Parameter']['Value']
client_secret = ssm.get_parameter(Name="AMADEUS_CLIENT_SECRET", WithDecryption=True)['Parameter']['Value']

amadeus = amadeus.Client(client_id=client_id, client_secret=client_secret, logger=logger)


def find_flights(origin, destination, departure_date, return_date, num_adults):
    """
    Finds best priced flights from Amadeus GDS. If a file has already been created for a specific search, it will read
    from disk rather than pinging the API. Otherwise, it will write a new file when the API is queried.
    :param origin: Origin of flight
    :param destination: Destination of flight
    :param departure_date: Date of departure in format "YYYY-MM-DD"
    :param return_date: Date of return in format "YYYY-MM-DD"
    :param num_adults: Number of adults on flight
    :return: JSON with flight details
    """
    # TODO: Handle stale data
    file_path = f"./data/query/{origin}-{destination}-D{departure_date}-R{return_date}-Ad{num_adults}.json"
    try:
        with open(file_path, "r") as file:
            print("Loading from file...")
            return json.load(file)
    except (FileNotFoundError):
        print("Querying database...")

    flights = amadeus.shopping.flight_offers_search.get(originLocationCode=origin,
                                                        destinationLocationCode=destination,
                                                        departureDate=departure_date,
                                                        returnDate=return_date,
                                                        adults=num_adults,
                                                        currencyCode="GBP")

    with open(f"{file_path}", "w") as file:
        json.dump(flights.data, file)

    return flights.data


def parse_flights(flights, granular=0):
    """
    Cleans flight details from JSON and outputs a data frame with relevant details.
    :param flights: JSON with flight details
    :param granular: If set to 1, it will return a more granular dataset including each segment. If 0, it will return 1 row per itinerary
    :return: Pandas dataframe with flight details
    """
    flights_data = []

    # Populate dataframe with data from Amadeus
    if granular:
        flights_df = pd.DataFrame(columns=["ID", "Airline", "Number of Stops", "Origin", "Destination",
                                           "Departure", "Arrival", "Duration", "Baggage", "Price", "Currency"])

        for flight in flights:
            for num_itin, itinerary in enumerate(flight["itineraries"]):
                for num_seg, segment in enumerate(itinerary["segments"]):
                    flights_data.append({
                        "ID": flight["id"],
                        "Airline Code": segment["carrierCode"],
                        "Number of Stops": segment["numberOfStops"],
                        "Origin": segment["departure"]["iataCode"],
                        "Destination": segment["arrival"]["iataCode"],
                        "Departure": segment["departure"]["at"],
                        "Arrival": segment["arrival"]["at"],
                        "Duration": itinerary["duration"],
                        "Baggage": len(flight["travelerPricings"][0]["fareDetailsBySegment"][(num_itin-1) * (num_seg-1)]["includedCheckedBags"]),
                        "Cabin": flight["travelerPricings"][0]["fareDetailsBySegment"][(num_itin-1) * (num_seg-1)]["cabin"],
                        "Price": flight["price"]["grandTotal"],
                        "Currency": flight["price"]["currency"]
                    })

    else:
        flights_df = pd.DataFrame(columns=["ID", "Airline", "Origin", "Destination",
                                           "Departure", "Arrival", "Price", "Currency"])

        for flight in flights:
            # Grab length of itineraries and segments for return so that we can grab only the final legs
            len_return_itineraries = len(flight["itineraries"])-1
            len_return_segments = len(flight["itineraries"][len_return_itineraries]["segments"])-1

            # Add to dataset
            flights_data.append({
                "ID": flight["id"],
                "Airline Code": flight["validatingAirlineCodes"][0],
                "Origin": flight["itineraries"][0]["segments"][0]["departure"]["iataCode"],
                "Destination": flight["itineraries"][len_return_itineraries]["segments"][0]["departure"]["iataCode"],
                "Departure": flight["itineraries"][0]["segments"][0]["departure"]["at"],
                "Arrival": flight["itineraries"][len_return_itineraries]["segments"][len_return_segments]["arrival"]["at"],
                "Price": flight["price"]["grandTotal"],
                "Currency": flight["price"]["currency"]
            })

    # Append list to dataframe and return
    flights_df = flights_df.append(flights_data)

    # Reset index and match up airlines to have human-readable names
    flights_df.set_index("ID")

    airline_df = pd.read_csv("./data/airline_codes.csv")
    flights_df = pd.merge(flights_df, airline_df, on="Airline Code", how="left")
    flights_df["Airline_x"] = flights_df["Airline_y"]
    flights_df.rename(columns={"Airline_x": "Airline"}, inplace=True)
    flights_df.drop(columns=["Airline_y"], inplace=True)

    return flights_df


def parse_date_range(origin, destination, departure_range, return_range, num_adults):
    """
    :param origin: Origin of flight
    :param destination: Destination of flight
    :param departure_range: Range of departure dates
    :param return_range: Range of return dates
    :param num_adults: Number of adults on flight
    :return:
    """
    flights_df = pd.DataFrame(columns=["ID", "Airline", "Number of Stops", "Origin", "Destination",
                                       "Departure", "Arrival", "Duration", "Baggage", "Price", "Currency"])

    for departure_date in departure_range:
        # Convert departure date to string
        departure_date = departure_date.strftime("%Y-%m-%d")

        for return_date in return_range:
            # Convert return date to string
            return_date = return_date.strftime("%Y-%m-%d")

            # Query database
            flights = find_flights(origin, destination, departure_date, return_date, num_adults)

            # Generate flights dataframe
            flights_df = flights_df.append(parse_flights(flights))

    return flights_df


def generate_price_by_date_range(flights_df):
    """
    Generates a new data frame simplified for price calculations.
    :param flights_df: Data frame of flights to clean
    :return: Simplified price dataframe
    """
    # Convert price to float to prepare for plotting
    flights_df["Price"] = flights_df["Price"].astype(float)
    flights_df = flights_df.set_index("ID")

    # Create new dataframe for price analysis with only required rows included
    price_df = flights_df.drop(columns=["Number of Stops", "Currency", "Duration", "Baggage", "Airline Code"])

    # Split Departure and Arrival times so that they only include date
    price_df["Departure"] = price_df["Departure"].apply(lambda x: x.split('T')[0])
    price_df["Arrival"] = price_df["Arrival"].apply(lambda x: x.split('T')[0])

    return price_df


def graph_price_by_date_range(price_df, departure_range_length, ax=None, lowest_price=True):
    """
    Graphs average price of flights based on range of departure dates and return dates.
    :param flights_df: Data frame of flights to clean
    :param departure_range: Length of departure date range for appropriate graph attribution
    :param ax: If generating a multiplot graph, allow for passing in an axis for plotting
    :param lowest_price: If True, generates graph based on lowest price rather than trended by depart date
    :return: None
    """
    # Group by mean price per departure and return
    grouped_price = price_df.groupby(["Departure", "Arrival"])["Price"].mean()
    grouped_price = pd.DataFrame(grouped_price)
    grouped_price = grouped_price.reset_index()

    # Concatenate Departure and Arrival for graphing
    grouped_price["Date Range"] = grouped_price["Departure"] + ": " + grouped_price["Arrival"]

    # Graph average price per departure/arrival date
    if not ax:
        fig, ax = plt.subplots(figsize=(14, 10))

    ax.tick_params(axis="x", labelrotation=90)

    if lowest_price:
        # Highlight minimum values
        sorted_price = grouped_price.sort_values("Price")

        # Define price threshold
        price_threshold = sorted_price.mean() - sorted_price["Price"].std()

        # Use departure range as index to color so that all flights on a given departure date are colored the same
        for row_num in range(len(grouped_price) - 1):
            if grouped_price.iloc[row_num]["Price"] <= float(price_threshold):
                ax.bar(grouped_price.iloc[row_num]["Date Range"], grouped_price.iloc[row_num]["Price"], color="green")
            else:
                ax.bar(grouped_price.iloc[row_num]["Date Range"], grouped_price.iloc[row_num]["Price"], color="blue")

    else:
        # Use departure range as index to color so that all flights on a given departure date are colored the same
        prev_rows = 0
        color_list = ["#e60049", "#0bb4ff", "#50e991", "#e6d800", "#9b19f5", "#ffa300", "#dc0ab4", "#b3d4ff", "#00bfa0"]
        for index, row_num in enumerate(range(departure_range_length - 1, len(grouped_price) - 1, departure_range_length - 1)):
            ax.bar(grouped_price.iloc[prev_rows:row_num]["Date Range"], grouped_price.iloc[prev_rows:row_num]["Price"],
                   color=color_list[index])
            prev_rows = row_num


if __name__ == "__main__":
    # Define query parameters
    origin = "SYD"
    destination = "LON"
    departure_date = "2023-03-15"
    return_date = "2023-07-26"
    num_adults = 2

    # Query database
    flights = find_flights(origin, destination, departure_date, return_date, num_adults)

    # Generate and save dataframe
    file_path = f"./data/output/{origin}-{destination}-D{departure_date}-R{return_date}-Ad{num_adults}.csv"
    flights_df = parse_flights(flights, granular=1)
    flights_df.to_csv(file_path)


