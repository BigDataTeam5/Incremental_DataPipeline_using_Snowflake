def co2_percent_change(previous_value, current_value):
    """
    Compute the daily percent change in CO2 ppm values.
    If previous_value is None or zero (or if current_value is None), return 0.0.
    """
    # Check for None or invalid values
    if previous_value is None or current_value is None:
        return 0.0
    try:
        prev = float(previous_value)
        curr = float(current_value)
    except Exception:
        return 0.0

    # Avoid division by zero
    if prev == 0:
        return 0.0

    percent_change = ((curr - prev) / prev) * 100
    return percent_change

def main(previous_value, current_value):
    return co2_percent_change(previous_value, current_value)

# For local debugging:
if __name__ == '__main__':
    import sys
    if len(sys.argv) == 3:
        prev_val = float(sys.argv[1]) if sys.argv[1].lower() != 'none' else None
        curr_val = float(sys.argv[2]) if sys.argv[2].lower() != 'none' else None
        result = main(prev_val, curr_val)
        print(f"Daily CO₂ Percent Change: {result:.2f}%")
    else:
        print("Usage: python co2_daily_percent_change_udf.py <previous_value> <current_value>")
