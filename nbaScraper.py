import requests
import pandas as pd
import time
import os

"""
Scrapes all NBA regular-season games for the 2019-2020 season from data.nba.net
and saves detailed box score data to a CSV file.

--------------------------------------------------------------------------------
HOW IT WORKS:
1) We first hit the SCHEDULE_ENDPOINT to get all game IDs (and dates) for 2019-20.
2) For each game ID, we build the BOXSCORE_ENDPOINT URL and fetch the boxscore.
3) We parse the JSON to extract player-level stats, then accumulate in a pandas DataFrame.
4) Finally, we save everything to 'nba_boxscores_2019_20.csv' locally.

TIPS:
- The script can be easily adapted for other seasons by changing "2019" in the URLs.
- Consider adding error-handling or retry logic for production use.
"""

# -----------------------------------------------------------------------------
# 1. ENDPOINTS
# -----------------------------------------------------------------------------
SEASON = "2019"  # 2019-20 season
SCHEDULE_ENDPOINT = f"https://data.nba.net/prod/v2/{SEASON}/schedule.json"
BOXSCORE_ENDPOINT_TEMPLATE = "https://data.nba.net/prod/v1/{date}/{gameId}_boxscore.json"

# -----------------------------------------------------------------------------
# 2. SCRAPE SCHEDULE FOR GAME IDS
# -----------------------------------------------------------------------------
def get_game_ids_for_season(season=SEASON):
    """
    Fetch the schedule JSON for a given season from data.nba.net,
    parse out all REGULAR SEASON game IDs + game dates.
    Returns a list of (game_id, yyyymmdd) tuples.
    """
    print(f"[INFO] Fetching schedule for season={season} from data.nba.net...")
    resp = requests.get(SCHEDULE_ENDPOINT)
    resp.raise_for_status()
    data = resp.json()

    # "league" -> "standard" -> list of games
    all_games = data["league"]["standard"]

    season_game_ids = []
    for g in all_games:
        # Skip if not regular season (could be Preseason or Playoffs in this feed)
        if g.get("seasonStageId") != 2:  # 2 = Regular Season
            continue

        game_id = g["gameId"]
        start_date_eastern = g["startDateEastern"]  # 'YYYYMMDD' string
        season_game_ids.append((game_id, start_date_eastern))

    print(f"[INFO] Found {len(season_game_ids)} REGULAR SEASON games for {season}-{int(season)+1}.")
    return season_game_ids

# -----------------------------------------------------------------------------
# 3. SCRAPE BOXSCORE FOR EACH GAME
# -----------------------------------------------------------------------------
def scrape_boxscore(game_id, yyyymmdd):
    """
    Given a game_id and the yyyymmdd date string, build the boxscore endpoint URL,
    request the JSON data, and parse player stats into a Pandas DataFrame.
    Returns a DataFrame with all players' stats for that game.
    """
    url = BOXSCORE_ENDPOINT_TEMPLATE.format(date=yyyymmdd, gameId=game_id)
    resp = requests.get(url)
    resp.raise_for_status()
    data = resp.json()

    # data["stats"] -> "activePlayers" holds a list of dicts with player stats
    # If it's an unfinished or postponed game, it might be empty or missing.
    game_stats = data.get("stats", {})
    players = game_stats.get("activePlayers", [])

    if not players:
        # Could be a canceled/postponed game
        return pd.DataFrame()

    # We'll store the full row data
    rows = []
    for p in players:
        row = {
            "GAME_ID": game_id,
            "GAME_DATE": yyyymmdd,
            "PLAYER_ID": p.get("personId"),
            "PLAYER_NAME": p.get("firstName") + " " + p.get("familyName"),
            "TEAM_ID": p.get("teamId"),
            "TEAM_ABBREVIATION": p.get("teamTricode"),
            "MIN": p.get("min", 0),
            "PTS": p.get("points", 0),
            "REB": p.get("totReb", 0),
            "AST": p.get("assists", 0),
            "STL": p.get("steals", 0),
            "BLK": p.get("blocks", 0),
            "FGM": p.get("fgm", 0),
            "FGA": p.get("fga", 0),
            "FG_PCT": p.get("fgp", 0),
            "FG3M": p.get("tpm", 0),
            "FG3A": p.get("tpa", 0),
            "FG3_PCT": p.get("tpp", 0),
            "FTM": p.get("ftm", 0),
            "FTA": p.get("fta", 0),
            "FT_PCT": p.get("ftp", 0),
            "PLUS_MINUS": p.get("plusMinus", 0),
            "PF": p.get("pFouls", 0),
            "TO": p.get("turnovers", 0),
        }
        rows.append(row)

    df = pd.DataFrame(rows)
    return df

# -----------------------------------------------------------------------------
# 4. MAIN SCRIPT
# -----------------------------------------------------------------------------
def main():
    # A) Get all game IDs
    season_game_ids = get_game_ids_for_season(season=SEASON)

    # Prepare a list to accumulate DataFrames for each game
    all_game_dfs = []

    # B) Iterate and scrape
    for idx, (game_id, date_str) in enumerate(season_game_ids, start=1):
        print(f"[SCRAPE] [{idx}/{len(season_game_ids)}] GameID={game_id} Date={date_str}")
        game_df = scrape_boxscore(game_id, date_str)

        # Only append if there's data
        if not game_df.empty:
            all_game_dfs.append(game_df)

        # Sleep briefly to be polite, reduce the risk of rate limiting
        time.sleep(0.6)

    # C) Combine all games into one DataFrame
    if all_game_dfs:
        final_df = pd.concat(all_game_dfs, ignore_index=True)
    else:
        print("[WARNING] No data scraped. Possibly no valid regular-season games found.")
        final_df = pd.DataFrame()

    # D) Save to CSV
    out_csv = "nba_boxscores_2019_20.csv"
    final_df.to_csv(out_csv, index=False)
    print(f"[DONE] Saved {len(final_df)} player-boxscore rows to '{out_csv}'.")

if __name__ == "__main__":
    # Create an output folder if desired
    # os.makedirs("scraped_data", exist_ok=True)
    main()
