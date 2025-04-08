import os
import warnings
warnings.filterwarnings("ignore")

import pandas as pd
import numpy as np

###############################################################################
# 1) FILE PATHS - ADJUST IF NEEDED
###############################################################################
REG_BOX_PART1 = r"C:\Users\sskub\OneDrive\Desktop\betting\regular_season_box_scores_2010_2024_part_1.csv"
REG_BOX_PART2 = r"C:\Users\sskub\OneDrive\Desktop\betting\regular_season_box_scores_2010_2024_part_2.csv"
REG_BOX_PART3 = r"C:\Users\sskub\OneDrive\Desktop\betting\regular_season_box_scores_2010_2024_part_3.csv"

TEAM_TOT_CSV   = r"C:\Users\sskub\OneDrive\Desktop\betting\regular_season_totals_2010_2024.csv"

###############################################################################
# 2) LOADING PARTIAL PLAYER BOX SCORES
###############################################################################
def load_player_box_scores():
    print("[INFO] Loading partial CSVs for PLAYER box scores...")
    df_p1 = pd.read_csv(REG_BOX_PART1)
    df_p2 = pd.read_csv(REG_BOX_PART2)
    df_p3 = pd.read_csv(REG_BOX_PART3)

    df = pd.concat([df_p1, df_p2, df_p3], ignore_index=True)
    
    # Clean up season_year
    df["season_year"] = df["season_year"].astype(str).str[:4]
    df["season_year"] = pd.to_numeric(df["season_year"], errors="coerce")
    df.dropna(subset=["season_year"], inplace=True)
    df["season_year"] = df["season_year"].astype(int)

    # Convert game_date
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")

    # Make sure 'matchup' is a string
    df["matchup"] = df["matchup"].astype(str).fillna("")

    df.sort_values(by=["gameId", "teamId", "personId"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(f"[INFO] Player box shape: {df.shape}")
    return df

###############################################################################
# 3) LOADING TEAM TOTALS (2 rows per game)
###############################################################################
def load_team_totals():
    print("[INFO] Loading CSV for team totals (two rows per game).")
    df = pd.read_csv(TEAM_TOT_CSV)

    df["SEASON_YEAR"] = df["SEASON_YEAR"].astype(str).str[:4]
    df["SEASON_YEAR"] = pd.to_numeric(df["SEASON_YEAR"], errors="coerce")
    df.dropna(subset=["SEASON_YEAR"], inplace=True)
    df["SEASON_YEAR"] = df["SEASON_YEAR"].astype(int)

    df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
    df["MATCHUP"] = df["MATCHUP"].astype(str).fillna("")

    # We assume there's a "WL" column for each row to indicate if that row's team won or lost.

    df.sort_values(by=["GAME_ID", "TEAM_ID"], inplace=True)
    df.reset_index(drop=True, inplace=True)
    print(f"[INFO] Team totals shape: {df.shape}")
    return df

###############################################################################
# 4) FILTER YEAR RANGE
###############################################################################
def filter_year_range_player(df, start_year, end_year):
    return df[(df["season_year"] >= start_year) & (df["season_year"] <= end_year)].copy()

def filter_year_range_team(df, start_year, end_year):
    return df[(df["SEASON_YEAR"] >= start_year) & (df["SEASON_YEAR"] <= end_year)].copy()

###############################################################################
# 5) MAIN SCRIPT
###############################################################################
def main():
    mode = input("Enter mode: 'PLAYER' or 'TEAM': ").strip().upper()
    if mode not in ["PLAYER", "TEAM"]:
        print("[ERROR] Invalid mode. Please enter 'PLAYER' or 'TEAM'.")
        return

    # Year range
    try:
        start_year = int(input("Enter start year (e.g., 2015): "))
        end_year   = int(input("Enter end year   (e.g., 2024): "))
    except ValueError:
        print("[ERROR] Invalid years. Please enter numeric values for years.")
        return

    # Over/Under line
    try:
        ou_line = float(input("Enter Over/Under line (e.g. 220.5): "))
    except ValueError:
        print("[ERROR] Invalid Over/Under line. Please enter a numeric value.")
        return

    if mode == "PLAYER":
        # PLAYER mode implementation (unchanged)
        # [Code omitted for brevity; refer to previous versions]
        print("[INFO] PLAYER mode is not modified in this iteration.")
        return  # Exit as focus is on TEAM mode

    else:
        # mode == "TEAM"
        # 1) Load team totals
        df_team = load_team_totals()
        df_team = filter_year_range_team(df_team, start_year, end_year)
        if df_team.empty:
            print(f"[ERROR] No team data from {start_year}-{end_year}.")
            return

        # 2) Ask for the team abbreviations
        teamA = input("Enter your team abbreviation (e.g. CLE): ").strip().upper()
        teamB = input("Enter opponent team abbreviation (e.g. GSW): ").strip().upper()

        # 3) Filter for games between teamA and teamB
        # Find GAME_IDs where both teamA and teamB participated
        games_teamA = set(df_team[df_team["TEAM_ABBREVIATION"] == teamA]["GAME_ID"])
        games_teamB = set(df_team[df_team["TEAM_ABBREVIATION"] == teamB]["GAME_ID"])
        common_game_ids = games_teamA.intersection(games_teamB)

        if not common_game_ids:
            print(f"[ERROR] No games found between '{teamA}' and '{teamB}' from {start_year} to {end_year}.")
            return

        # Extract the rows for these games
        df_matchups = df_team[df_team["GAME_ID"].isin(common_game_ids)].copy()

        # Ensure that each GAME_ID has exactly two entries (teamA and teamB)
        df_matchups = df_matchups.groupby("GAME_ID").filter(lambda x: set(x["TEAM_ABBREVIATION"]) == {teamA, teamB})

        if df_matchups.empty:
            print(f"[ERROR] No valid matchups found between '{teamA}' and '{teamB}' from {start_year} to {end_year}.")
            return

        # Sum the total points per game
        grouped = df_matchups.groupby("GAME_ID", as_index=False).agg({
            "PTS": "sum",
            "SEASON_YEAR": "first",  # Assuming SEASON_YEAR is the same for both teams in a game
            "MATCHUP": lambda x: list(x)
        })
        grouped.rename(columns={"PTS":"game_total_points", "MATCHUP": "matchups"}, inplace=True)

        # Determine Over or Under
        grouped["Over/Under"] = grouped["game_total_points"].apply(lambda x: "Over" if x > ou_line else "Under")
        over_count = (grouped["Over/Under"] == "Over").sum()
        total_count = len(grouped)
        prob_over = over_count / total_count if total_count > 0 else 0.0

        # Determine winners and home/away status
        def determine_winner_and_location(row):
            team_rows = df_matchups[df_matchups["GAME_ID"] == row["GAME_ID"]]
            teamA_row = team_rows[team_rows["TEAM_ABBREVIATION"] == teamA].iloc[0]
            teamB_row = team_rows[team_rows["TEAM_ABBREVIATION"] == teamB].iloc[0]

            # Determine who won
            winner = teamA if teamA_row["WL"] == "W" else teamB

            # Determine home/away
            # If "vs." in teamA's matchup, they are home; "@" means away
            if "vs." in teamA_row["MATCHUP"]:
                location = "Home"
            else:
                location = "Away"

            # Retrieve SEASON_YEAR
            season_year = teamA_row["SEASON_YEAR"]

            return pd.Series([winner, location, season_year])

        grouped[['Winner', 'Location', 'Season_Year']] = grouped.apply(determine_winner_and_location, axis=1)

        # Calculate win counts
        teamA_wins = (grouped["Winner"] == teamA).sum()
        teamB_wins = (grouped["Winner"] == teamB).sum()

        # Calculate home and away wins for each team
        home_wins_teamA = ((grouped["Winner"] == teamA) & (grouped["Location"] == "Home")).sum()
        away_wins_teamA = ((grouped["Winner"] == teamA) & (grouped["Location"] == "Away")).sum()

        home_wins_teamB = ((grouped["Winner"] == teamB) & (grouped["Location"] == "Home")).sum()
        away_wins_teamB = ((grouped["Winner"] == teamB) & (grouped["Location"] == "Away")).sum()

        # Display summary
        print(f"\n=== TEAM MATCHUP: {teamA} vs {teamB} in {start_year}-{end_year} ===")
        print(f"Total games analyzed: {total_count}")
        print(f"Times Over {ou_line}: {over_count} => Over Probability: {prob_over:.3f}\n")

        print(f"Win Statistics:")
        print(f"{teamA} Wins: {teamA_wins} ({teamA_wins / total_count:.3f} probability)")
        print(f"{teamB} Wins: {teamB_wins} ({teamB_wins / total_count:.3f} probability)\n")

        print(f"Home/Away Win Breakdown:")
        print(f"{teamA} Home Wins: {home_wins_teamA}")
        print(f"{teamA} Away Wins: {away_wins_teamA}\n")
        print(f"{teamB} Home Wins: {home_wins_teamB}")
        print(f"{teamB} Away Wins: {away_wins_teamB}\n")

        # Provide betting recommendations
        print(f"=== Betting Recommendations ===")
        
        # Recommendation for Over/Under
        if prob_over > 0.6:
            ou_recommendation = "Over"
            ou_confidence = "strong"
        elif prob_over > 0.5:
            ou_recommendation = "Over"
            ou_confidence = "moderate"
        elif prob_over < 0.4:
            ou_recommendation = "Under"
            ou_confidence = "strong"
        elif prob_over < 0.5:
            ou_recommendation = "Under"
            ou_confidence = "moderate"
        else:
            ou_recommendation = "Neutral"
            ou_confidence = "no clear trend"

        if ou_recommendation != "Neutral":
            print(f"- The probability of the game going Over {ou_line} is {prob_over:.2f}. It's recommended to bet on **{ou_recommendation}** with {ou_confidence} confidence.")
        else:
            print(f"- The Over/Under probability is {prob_over:.2f}. It's recommended to **hold** your bet as there's no clear trend.")

        # Recommendation for Winning Team
        if teamA_wins / total_count > teamB_wins / total_count + 0.1:
            win_recommendation = teamA
            win_confidence = "high"
        elif teamB_wins / total_count > teamA_wins / total_count + 0.1:
            win_recommendation = teamB
            win_confidence = "high"
        elif teamA_wins / total_count > teamB_wins / total_count + 0.05:
            win_recommendation = teamA
            win_confidence = "moderate"
        elif teamB_wins / total_count > teamA_wins / total_count + 0.05:
            win_recommendation = teamB
            win_confidence = "moderate"
        else:
            win_recommendation = "Neither team has a clear advantage"
            win_confidence = "low"

        if win_recommendation in [teamA, teamB]:
            print(f"- Based on historical performance, it's recommended to bet on **{win_recommendation}** to win with {win_confidence} confidence.")
        else:
            print(f"- Based on historical performance, there's no clear advantage. It's recommended to **consider other factors** before placing a bet.")

        # Optional: Display detailed game results
        show_details = input("Do you want to see detailed game results? (yes/no): ").strip().lower()
        if show_details in ['yes', 'y']:
            detailed_results = grouped[['GAME_ID', 'Season_Year', 'game_total_points', 'Over/Under', 'Winner', 'Location']]
            detailed_results.rename(columns={
                'game_total_points': 'Total_Points',
                'Location': 'Home/Away',
                'Season_Year': 'Year'
            }, inplace=True)
            print("\n=== Detailed Game Results ===")
            print(detailed_results.to_string(index=False))

if __name__ == "__main__":
    main()
