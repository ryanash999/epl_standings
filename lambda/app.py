#import json
import os
import re
import requests
import boto3
import yaml
import datetime
import jinja2
import pytz

S3_YEAR_FOLDER = "2020-2021"
TEAMS = [
    "Arsenal",
    "Aston Villa",
    "Brighton and Hove Albion",
    "Burnley",
    "Chelsea",
    "Crystal Palace",
    "Everton",
    "Fulham",
    "Leeds United",
    "Leicester City",
    "Liverpool",
    "Manchester City",
    "Manchester United",
    "Newcastle United",
    "Sheffield United",
    "Southampton",
    "Tottenham Hotspur",
    "West Bromwich Albion",
    "West Ham United",
    "Wolverhampton Wanderers"
]

API_TEAM_MAP = {
    "Leicester": "Leicester City",
    "Leeds": "Leeds United",
    "Wolves": "Wolverhampton Wanderers",
    "West Brom": "West Bromwich Albion",
    "Sheffield Utd": "Sheffield United",
    "Newcastle": "Newcastle United",
    "West Ham": "West Ham United",
    "Tottenham": "Tottenham Hotspur",
    "Brighton": "Brighton and Hove Albion",
}

THIS_DIR = os.path.dirname(os.path.abspath(__file__))

def lambda_handler(event, context):
    standings = get_api_data()
    player_data = get_s3_player_data()
    validate(player_data)
    points = calculate_points(standings, player_data)
    send_to_s3(points)


def send_to_s3(points):
    tz = pytz.timezone('US/Central')
    date_updated = datetime.datetime.now(tz).strftime('%Y-%m-%d %H:%M:%S')
    template = jinja2.Environment(loader=jinja2.FileSystemLoader(THIS_DIR))
    render_vars = {
        "timestamp": date_updated,
        "standings": points
    }

    with open("/tmp/index.html", "w") as result_file:
        result_file.write(
                template.get_template("template.html.j2")
                .render(render_vars))

    s3 = boto3.resource('s3')
    s3.Bucket("smolich-epl").upload_file(
        "/tmp/index.html", "index.html", ExtraArgs={
            'ContentType': 'text/html', 'ACL': 'public-read'})


def calculate_points(standings, data):
    points = {}
    sorted_points = []

    for player in data:
        points[player] = 0

    for obj in standings:
        for player in data:
            i = data[player].index(obj['name'])
            #print (f"{obj['name']} - {i}")
            p = abs(i-int(obj['rank']))
            points[player] += int(p)

    for k, v in sorted(points.items(), key=lambda item: item[1]):
        tmp = {
            "player": k,
            "points": v
        }
        sorted_points.append(tmp)

    print(yaml.dump(sorted_points))
    return(sorted_points)


def validate(player_info):
    for player in player_info:
        for team in player_info[player]:
            if team not in TEAMS:
                print(f"ERROR: invalid team player='{player}' team='{team}'")


def get_api_data():
    standings = []
    url = "https://api-football-v1.p.rapidapi.com/v2/leagueTable/2790"
    headers = {
        'x-rapidapi-host': "api-football-v1.p.rapidapi.com",
        'x-rapidapi-key': "a4eb4c472cmshf493e8af833b91dp19da5ajsn295c912b4083"
    }
    response = requests.get(url, headers=headers).json()

    for obj in response['api']['standings'][0]:
        team_name = ""

        # Update name because API and our sheets have different values
        if obj['teamName'] in API_TEAM_MAP:
            # print(f"Override: {obj['teamName']}")
            team_name = API_TEAM_MAP[obj['teamName']]
        else:
            team_name = obj['teamName']

        # print(f"{team_name} - rank={obj['rank']}")

        tmp = {
            "name": team_name,
            "rank": obj['rank'],
            "points": obj['points']
        }
        standings.append(tmp)

    return standings


def validate_team(player, team):
    if team not in TEAMS:
        print(f"ERROR: team={team} is not valid for player {player}")


def get_s3_player_data():
    player_info = {}
    s3_client = boto3.client("s3")
    list_objects = s3_client.list_objects_v2(
        Bucket="smolich-epl",
        Prefix=S3_YEAR_FOLDER
    )

    for player_object in list_objects['Contents']:
        if not re.search(r'yaml', player_object['Key']):
           continue

        s3_result = s3_client.get_object(Bucket='smolich-epl', Key=player_object['Key'])
        s3_text = s3_result["Body"].read().decode()
        player_data = yaml.load(s3_text)
        player_info[player_data['name']] = player_data['standings']

    return player_info


def load_player_data(file_name):
    try:
        with open(file_name) as f:
            try:
                player_config = yaml.safe_load(f)
            except yaml.scanner.ScannerError as e:
                print(f"Could not load yaml from config: {e}")
    except FileNotFoundError as e:
        print(f"Could not find config.yaml: {e}")
    else:
        return player_config


if __name__ == "__main__":
    lambda_handler({}, "")
