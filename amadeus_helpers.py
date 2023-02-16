# Amadeus Python API: https://github.com/amadeus4dev/amadeus-python

import amadeus
import logging
import json
import pandas as pd
import boto3

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


def clean_flights(flights):
    """
    Cleans flight details from JSON and outputs a data frame with relevant details.
    :param flights: JSON with flight details
    :return: Pandas dataframe with flight details
    """
    flights_df = pd.DataFrame(columns=["ID", "Airline", "Number of Stops", "Origin", "Destination",
                                       "Departure", "Arrival", "Duration", "Baggage", "Price", "Currency"])

    flights_data = []

    # Populate dataframe with data from Amadeus
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
    flights_df = clean_flights(flights)
    flights_df.to_csv(file_path)


