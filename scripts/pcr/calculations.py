from typing import List


def calculate_points(data, expiry_date):

    pe_values: List[dict] = [data['PE'] for data in data['records']['data'] if
                             "PE" in data and data['expiryDate'].lower() == expiry_date.lower()]
    points: float = pe_values[0]['underlyingValue']
    if points == 0:
        for item in pe_values:
            if item['underlyingValue'] != 0:
                points = item['underlyingValue']
                break

    return points


def calculate_pcr_around_threshold(data, market_price, sigma, expiry_date):
    change_sum = {"call_change": 0, "put_change": 0}

    for item in data['records']['data']:
        if item["expiryDate"] == expiry_date:
            current_strike = item["strikePrice"]

            # Calculate distance from reference strike (positive for above, negative for below)
            distance = (current_strike - market_price) * 100 / (sigma * market_price)  # Normalize to strike units

            if ('CE' in item) and abs(distance) <= 1.5:
                change_sum["call_change"] += item['CE']['changeinOpenInterest']
            if ('PE' in item) and abs(distance) <= 1.5:
                change_sum["put_change"] += item['PE']['changeinOpenInterest']

    if change_sum["put_change"] == 0 and change_sum["call_change"] == 0:
        return 1
    elif change_sum["put_change"] < 0 and change_sum["call_change"] < 0:
        return round(change_sum["call_change"] / change_sum["put_change"], 2)
    elif change_sum["put_change"] <= 0 and change_sum["call_change"] >= 0:
        return 0.1
    elif change_sum["put_change"] >= 0 and change_sum["call_change"] <= 0:
        return 10
    else:
        return round(change_sum["put_change"] / change_sum["call_change"], 2)


def calculate_pcr(data, threshold, expiry_date):
    call_oi = None
    put_oi = None
    for item in data['records']['data']:
        if item['expiryDate'] == expiry_date and float(item['strikePrice']) == float(threshold):
            if 'CE' in item:
                call_oi = item['CE']['openInterest']
            if 'PE' in item:
                put_oi = item['PE']['openInterest']

    if call_oi is not None and call_oi != 0:
        return round(put_oi / call_oi, 2)
    else:
        return


def calculate_thresholds(spot_price):
    # Define ranges and corresponding step magnitudes
    ranges_and_steps = [(50, 5), (100, 10), (800, 50), (3000, 100), (5000, 200), (20000, 500), (50000, 1000)]

    # Determine the appropriate step magnitude for the given stock price
    for price_limit, step_magnitude in ranges_and_steps:
        if spot_price < price_limit:
            break
    else:
        step_magnitude = 2000  # Default step for prices above the highest range

    # Calculate thresholds
    lower_threshold = spot_price - (spot_price % step_magnitude)
    upper_threshold = lower_threshold + step_magnitude

    distance1 = abs(spot_price - lower_threshold)
    distance2 = abs(spot_price - upper_threshold)

    if distance1 < distance2:
        return lower_threshold
    else:
        return upper_threshold

