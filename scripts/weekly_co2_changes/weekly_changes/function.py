def co2_weekly_percent_change(previous_week_value, current_value):
    """
    Compute the weekly percent change in CO₂ ppm values.
    If the previous week's value is None (or zero) or the current value is None,
    return 0.0 to avoid errors.
    """
    # Check if either value is None or if previous is 0 (avoid division by zero)
    if previous_week_value is None or current_value is None:
        return 0.0
    try:
        prev = float(previous_week_value)
        curr = float(current_value)
    except Exception:
        return 0.0

    if prev == 0:
        return 0.0

    percent_change = ((curr - prev) / prev) * 100
    return percent_change

def main(previous_week_value, current_value):
    return co2_weekly_percent_change(previous_week_value, current_value)

# For local debugging
if __name__ == '__main__':
    import sys
    if len(sys.argv) == 3:
        prev_week_value = float(sys.argv[1]) if sys.argv[1].lower() != 'none' else None
        curr_value = float(sys.argv[2]) if sys.argv[2].lower() != 'none' else None
        print(f"Weekly CO₂ Percent Change: {main(prev_week_value, curr_value):.2f}%")
    else:
        print("Usage: python co2_weekly_percent_change_udf.py <previous_week_value> <current_value>")
