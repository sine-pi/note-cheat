import pandas as pd
import datetime
import re
# Function to load a CSV file into a DataFrame

from dateutil import parser


def detect_date_format(date_string):
    try:
        parsed_date = parser.parse(date_string, fuzzy=True)
        return parsed_date.strftime("On %B %d %Y")
    except ValueError:
        return "Unknown date format"


def date_format_std(date_string):
    try:
        parsed_date = parser.parse(date_string, fuzzy=True)
        return parsed_date.strftime("%d/%m/%Y")
    except ValueError:
        return "Unknown date format"


def detect_range_date_format(date):
    date = date.replace(",", "")
    text = f'"{date}"'
    print(text)

    pattern = r'"(.*?)"'
    date_range = ''
    match = re.search(pattern, text)
    if match:
        date_range = match.group(1)

    dates = re.split(r'\s+to\s+', date_range)

    # Keep month from first date
    month = dates[0].split(' ')[0]

    # Use day and year from second date
    day, year = dates[1].replace(',', '').split(' ')

    date_from = detect_date_format(f"{dates[0]} {year}")[3:]
    date_to = detect_date_format(f"{month} {day} {year}")[3:]

    return date_from, date_to


def load_csv_to_dataframe(file_path):
    try:
        df = pd.read_csv(file_path)
        return df
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None


# Function to query a DataFrame by condition
def query_dataframe(df, condition):
    try:
        result_df = df[condition]
        return result_df
    except KeyError:
        print("Error: The DataFrame does not contain the specified columns.")
        return None


def ans_case_with_vectordb(output, db):
    if isinstance(output, str):
        return output
    
    collection_name = output.case_type
    line_name = output.line_name
    start_date = output.start_date.strftime(
        "%B %d %Y") if output.start_date else ""
    end_date = output.end_date.strftime(
        "%B %d %Y") if output.end_date else ""
    
    if end_date:
        date = f"From {start_date} to {end_date}"
    else:
        date = f"On {start_date}"

    query_text = f"{date}, in the production line {line_name}"
    print(f"Query: {query_text}")

    return db.query_by_text(collection_name, query_text)

def ans_case(output):
    if isinstance(output, str):
        return output
    
    case_type = output.case_type
    line_name = output.line_name
    start_date = output.start_date.strftime(
        "%B %d %Y") if output.start_date else None
    prev_start_date = (output.start_date - datetime.timedelta(days=1)).strftime(
        "%B %d %Y") if output.start_date else None
    end_date = output.end_date.strftime(
        "%B %d %Y") if output.end_date else None

    if case_type == 'greeting':
        return """I am DensoGPT, would you like to ask me about something like:\n
                    1: Which equipment had the longest downtime on June 01, 2023, for the "OCV1" line?
                    2: What caused the longest downtime on June 01, 2023, for the "OCV1" line?
                    ...
                    I can answer you clearly base on my knowledge. Thank you!"""

    elif case_type == 'top1_equipment':
        df = load_csv_to_dataframe(
            "pipeline/data_embed/case1_Top1Equipment.csv")
        condition = (df['date'] == f"On {start_date}") & (
            df['production_line'].str.lower() == line_name.lower())
        try:
            result = query_dataframe(df, condition)
            if float(result["ratio"].iloc[0]) == 0.0:
                return f"""{result["date"].iloc[0]}, the production line {result["production_line"].iloc[0]} stopped."""
            result_format = f"""{result["date"].iloc[0]}, in the production line {result["production_line"].iloc[0]}, Equipment(s) "{result["equipment"].iloc[0]}" had the greatest downtime with {result["ratio"].iloc[0]}%."""
        except:
            return "I apologize, but it seems that the information you are looking for is not present within the data that has been provided to me."
        return result_format

    elif case_type == 'top1_cause':
        df = load_csv_to_dataframe("pipeline/data_embed/case2_Top1Cause.csv")
        condition = (df['date'] == f"On {start_date}") & (
            df['production_line'].str.lower() == line_name.lower())
        try:
            result = query_dataframe(df, condition)
            if float(result["ratio"].iloc[0]) == 0.0:
                return f"""{result["date"].iloc[0]}, the production line {result["production_line"].iloc[0]} stopped."""
            result_format = f"""{result["date"].iloc[0]}, in the production line {result["production_line"].iloc[0]}, The cause(s) "{result["cause"].iloc[0]}" caused the greatest downtime with {result["ratio"].iloc[0]}%."""
        except:
            return "I apologize, but it seems that the information you are looking for is not present within the data that has been provided to me."
        return result_format

    elif case_type == 'top5_equipment':
        df = load_csv_to_dataframe(
            "pipeline/data_embed/case3_Top5Equipment.csv")
        condition = (df['date'] == f"On {start_date}") & (
            df['production_line'].str.lower() == line_name.lower())
        try:
            result = query_dataframe(df, condition)
            if str(result["_top5"].iloc[0]).count("0.0 %") == 5:
                return f"""{result["date"].iloc[0]}, the production line {result["production_line"].iloc[0]} stopped."""
            extra = ""
            if len(str(result["_top5"].iloc[0]).split("\n")) < 6: extra = "No more equiqment has downtime."
            n_equiq = len(str(result["_top5"].iloc[0]).split("\n")) - 1
            result_format = f"""{result["date"].iloc[0]}, in the production line {result["production_line"].iloc[0]}, these are {n_equiq} Equipment(s) has the most downtime:
{result["_top5"].iloc[0]}
{extra}"""
        except:
            return "I apologize, but it seems that the information you are looking for is not present within the data that has been provided to me."
        return result_format

    elif case_type == 'increased_dt':
        df = load_csv_to_dataframe(
            "pipeline/data_embed/case5_Top1IncreasingEquipment.csv")
        condition = (df['start_date'] == start_date) & (
            df['end_date'] == end_date) & (df['production_line'].str.lower() == line_name.lower())
        try:
            result = query_dataframe(df, condition)
            if result["this_week_time"].iloc[0] == 0.0 and result["this_week_time"].iloc[0] == 0.0:
                return f"""The production line {result["production_line"].iloc[0]} stopped in the last 2 weeks."""
            result_format = f"""In the production line {result["production_line"].iloc[0]} in this week (from {start_date} to {end_date}), the equipment "{result["equipment"].iloc[0]}" has increased downtime percentage to the most. Total downtime percentage in the last week was {result["last_week_time"].iloc[0]}%, in this week it is {result["this_week_time"].iloc[0]}%, increased by {result["ratio"].iloc[0]}%."""
        except Exception as e:
            print(e)
            return "I apologize, but it seems that the information you are looking for is not present within the data that has been provided to me."
        return result_format

    elif case_type == "cause_overall":
        df = load_csv_to_dataframe(
            "pipeline/data_embed/case8_Top1CauseOverall.csv")
        condition = (df['month'] == start_date.split()[0])
        try:
            result = query_dataframe(df, condition)
            result_format = f"""In {result["month"].iloc[0]}, the production line {result["production_line"].iloc[0]} had the lowest aggregate equipment efficiency of {result["production_efficiency"].iloc[0]}%, with the cause {result["cause"].iloc[0]} triggering the longest downtime, affecting total {result["average_stop_percentage"].iloc[0]}% in the whole month."""
        except:
            return "I apologize, but it seems that the information you are looking for is not present within the data that has been provided to me."
        return result_format

    elif case_type == "production_line":
        df = load_csv_to_dataframe(
            "pipeline/data_embed/case9_Top1ProductionLine.csv")
        condition = (df['start_date'] == start_date) & (df['end_date'] == end_date)
        try:
            result = query_dataframe(df, condition)
            result_format = f"""In last week's production results (from {start_date} to {end_date}), the average OEE of production line {result["production_line"].iloc[0]} is the lowest ({result["percentage"].iloc[0]} %)."""
        except:
            return "I apologize, but it seems that the information you are looking for is not present within the data that has been provided to me."
        return result_format

    elif case_type == "production_amount":
        df = load_csv_to_dataframe(
            "pipeline/data_embed/case10_Top1ProductionAmount.csv")
        condition = (df['start_date'] == start_date) & (df['end_date'] == end_date)
        try:
            result = query_dataframe(df, condition)
            result_format = f"""In last week's production results (from {start_date} to {end_date}), Production line {result["production_line"].iloc[0]} has the highest production volume ({result["production_amount"].iloc[0]})."""
        except:
            return "I apologize, but it seems that the information you are looking for is not present within the data that has been provided to me."
        return result_format

    elif case_type == "top5_line":
        df = load_csv_to_dataframe(
            "pipeline/data_embed/case11_top5productionline.csv")
        condition = (df['start_day'] == start_date) & (
            df['end_day'] == end_date)
        try:
            result = query_dataframe(df, condition)
            extra = ""
            if len(str(result["_top5"].iloc[0]).split("\n")) < 6: extra = "Other production lines stopped last week."
            n_equiq = len(str(result["_top5"].iloc[0]).split("\n")) - 1
            result_format = f"""In last week's production results (from {start_date} to {end_date}), these are {n_equiq} lines had the lowest average OEE:
{result["_top5"].iloc[0]}
{extra}"""
        except:
            return "I apologize, but it seems that the information you are looking for is not present within the data that has been provided to me."
        return result_format

    elif case_type == "top3_worst":
        df = load_csv_to_dataframe(
            "pipeline/data_embed/case14_top3worstproductionline.csv")
        condition = (df['start_day'] == start_date) & (
            df['end_day'] == end_date)
        try:
            result = query_dataframe(df, condition)
            result_format = f"""In last week's production results (from {start_date} to {end_date}), the following lines had the average OEE didn't meet the target:
{result["_top3"].iloc[0]}"""
        except:
            return "I apologize, but it seems that the information you are looking for is not present within the data that has been provided to me."
        return result_format
    
    elif case_type == "average_to":
        df = load_csv_to_dataframe(
            "pipeline/data_embed/case15_AccumulatedTime.csv")
        try:
            if start_date.split(" ")[1] == "01":
                condition = (df['production_line'].str.lower() == line_name.lower()) & (
                    df['end_date'] == end_date)
                result = query_dataframe(df, condition)
                ratio = result["operating_time"].iloc[0] / result["total_time"].iloc[0]
            else:
                prev_condition = (df['production_line'].str.lower() == line_name.lower()) & (df['end_date'] == prev_start_date)
                prev_result = query_dataframe(df, prev_condition)
                end_condition = (df['production_line'].str.lower() == line_name.lower()) & (df['end_date'] == end_date)
                end_result = query_dataframe(df, end_condition)
                ratio = (end_result["operating_time"].iloc[0] - prev_result["operating_time"].iloc[0]) / (end_result["total_time"].iloc[0] - prev_result["total_time"].iloc[0])
            result_format = f"""The average OEE of the {line_name} production line from {start_date} to {end_date} is {round(float(ratio)*100, 2)}%."""
        except Exception as e:
            print(e)
            return "I apologize, but it seems that the information you are looking for is not present within the data that has been provided to me."
        return result_format
    else:
        return """I think there some thing wrong ! Please try again or contact for support."""

if __name__ == "__main__":

    # file_path = "case3_Top5Equipment.csv"  # Replace with the path to your CSV file
    # data_df = load_csv_to_dataframe(file_path)

    input = """top5_equipment|06/06/2023| "OCV1"]"""
    result = ans_case(input)

    if result is not None:
        print("Filtered DataFrame:")
        print(result)
