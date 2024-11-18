from flask import Flask, jsonify, send_from_directory
import boto3
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
matplotlib.use('Agg')  # Use non-GUI backend
import seaborn as sns
import json
from datetime import datetime, timezone
import os
from flask_cors import CORS  # To handle CORS
import logging

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Initialize S3 client
s3 = boto3.client('s3')
bucket_name = 'football-data-pipeline-rohan'

def fetch_data_from_s3(file_key):
    """
    Fetches JSON data from S3 and returns a pandas DataFrame.
    """
    try:
        logger.info(f"Attempting to fetch data from S3 with key: {file_key}")
        response = s3.get_object(Bucket=bucket_name, Key=file_key)
        data = json.loads(response['Body'].read().decode('utf-8'))
        if 'matches' in data:
            df = pd.json_normalize(data['matches'])
            df["utcDate"] = pd.to_datetime(df["utcDate"])
            logger.info(f"DataFrame for {file_key} created successfully with shape {df.shape}")
            return df
        else:
            logger.warning(f"'matches' key not found in {file_key}")
            return None
    except s3.exceptions.NoSuchKey:
        logger.error(f"No such key: {file_key}")
        return None
    except Exception as e:
        logger.error(f"Error fetching data from S3 for key {file_key}: {e}")
        return None

def create_all_visualizations(df):
    """
    Creates and saves all required visualizations.
    Returns a list of image filenames and the league standings.
    """
    try:
        logger.info("Starting visualization creation...")
        os.makedirs("static/images", exist_ok=True)
        image_files = []

        # 1. PIE CHART of home wins, away wins, and draws
        match_outcomes = df['score.winner'].value_counts()
        logger.info(f"Match Outcomes: {match_outcomes.to_dict()}")
        colors = ['#4CAF50', '#FF6347', '#87CEEB']
        plt.figure(figsize=(8, 8))
        match_outcomes.plot(
            kind='pie',
            autopct='%1.1f%%',
            startangle=140,
            labels=['Home Win', 'Away Win', 'Draw'],
            colors=colors
        )
        plt.title("Match Outcomes Distribution", fontsize=16, fontweight='bold')
        plt.ylabel('')
        piechart_filename = "piechart.png"
        plt.savefig(f"static/images/{piechart_filename}", bbox_inches='tight', pad_inches=0.1)
        plt.close()
        image_files.append(piechart_filename)
        logger.info(f"Saved {piechart_filename}")

        # 2. GOALS PER MATCHDAY
        goals_per_matchday = df.groupby('matchday')[['score.fullTime.home', 'score.fullTime.away']].sum()
        logger.info(f"Goals per Matchday: {goals_per_matchday.to_dict()}")
        plt.figure(figsize=(12, 6))
        goals_per_matchday.plot(
            kind='bar',
            stacked=True,
            color=['#4CAF50', '#FF6347']
        )
        plt.title("Goals Scored per Matchday", fontsize=16, fontweight='bold')
        plt.xlabel("Matchday", fontsize=12)
        plt.ylabel("Goals", fontsize=12)
        plt.xticks(rotation=45)
        plt.legend(["Home Goals", "Away Goals"], loc='upper right')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        goal_matchday_filename = "goal_matchday.png"
        plt.savefig(f"static/images/{goal_matchday_filename}", bbox_inches='tight', pad_inches=0.1)
        plt.close()
        image_files.append(goal_matchday_filename)
        logger.info(f"Saved {goal_matchday_filename}")

        # 3. TOTAL GOALS PER TEAM
        team_goals_home = df.groupby('homeTeam.shortName')['score.fullTime.home'].sum()
        team_goals_away = df.groupby('awayTeam.shortName')['score.fullTime.away'].sum()
        team_goals = team_goals_home.add(team_goals_away, fill_value=0)
        team_goals = team_goals.sort_values(ascending=True)
        plt.figure(figsize=(10, 12))
        team_goals.plot(
            kind='barh',
            color='#87CEEB'
        )
        plt.title("Total Goals Scored by Each Team", fontsize=16, fontweight='bold')
        plt.xlabel("Total Goals", fontsize=12)
        plt.ylabel("Team", fontsize=12)
        plt.grid(axis='x', linestyle='--', alpha=0.7)
        goal_perteam_filename = "goal_perteam.png"
        plt.savefig(f"static/images/{goal_perteam_filename}", bbox_inches='tight', pad_inches=0.1)
        plt.close()
        image_files.append(goal_perteam_filename)
        logger.info(f"Saved {goal_perteam_filename}")

        # 4. HOME VS AWAY WINNING PERCENTAGE BY MATCHDAY
        home_wins = df[df['score.winner'] == 'HOME_TEAM'].groupby('matchday').size()
        away_wins = df[df['score.winner'] == 'AWAY_TEAM'].groupby('matchday').size()
        total_matches = df.groupby('matchday').size()
        home_win_rate = (home_wins / total_matches).fillna(0) * 100
        away_win_rate = (away_wins / total_matches).fillna(0) * 100
        logger.info(f"Home Win Rate: {home_win_rate.to_dict()}, Away Win Rate: {away_win_rate.to_dict()}")
        plt.figure(figsize=(12, 6))
        home_win_rate.plot(
            label='Home Win Rate',
            marker='o',
            color='#4CAF50',
            linewidth=2
        )
        away_win_rate.plot(
            label='Away Win Rate',
            marker='o',
            color='#FF6347',
            linewidth=2
        )
        plt.title("Home vs Away Winning Percentage by Matchday", fontsize=16, fontweight='bold')
        plt.xlabel("Matchday", fontsize=12)
        plt.ylabel("Winning Percentage (%)", fontsize=12)
        plt.legend()
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        home_vs_away_filename = "HomevsAway.png"
        plt.savefig(f"static/images/{home_vs_away_filename}", bbox_inches='tight', pad_inches=0.1)
        plt.close()
        image_files.append(home_vs_away_filename)
        logger.info(f"Saved {home_vs_away_filename}")

        # 5. DISTRIBUTION OF MATCHES BY TIME OF DAY
        df['hour'] = df['utcDate'].dt.hour
        df['time_of_day'] = pd.cut(
            df['hour'],
            bins=[0, 12, 18, 24],
            labels=['Morning', 'Afternoon', 'Evening'],
            right=False
        )
        logger.info(f"Time of Day Distribution: {df['time_of_day'].value_counts().to_dict()}")
        plt.figure(figsize=(8, 6))
        df['time_of_day'].value_counts().plot(
            kind='bar',
            color='#87CEEB',
            edgecolor='black'
        )
        plt.title("Distribution of Matches by Time of Day", fontsize=16, fontweight='bold')
        plt.xlabel("Time of Day", fontsize=12)
        plt.ylabel("Number of Matches", fontsize=12)
        plt.xticks(rotation=0)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        morning_vs_evening_filename = "MorningvsEvening.png"
        plt.savefig(f"static/images/{morning_vs_evening_filename}", bbox_inches='tight', pad_inches=0.1)
        plt.close()
        image_files.append(morning_vs_evening_filename)
        logger.info(f"Saved {morning_vs_evening_filename}")

        # 6. MATCH TIME HEATMAP
        df['day_of_week'] = df['utcDate'].dt.day_name()
        heatmap_data = df.pivot_table(
            index='day_of_week',
            columns='hour',
            aggfunc='size',
            fill_value=0
        )
        logger.info(f"Heatmap Data: {heatmap_data.to_dict()}")
        plt.figure(figsize=(14, 8))
        sns.heatmap(
            heatmap_data,
            cmap="YlGnBu",
            annot=True,
            fmt="d",
            cbar_kws={'label': 'Number of Matches'}
        )
        plt.title("Heatmap of Matches by Day and Time", fontsize=16, fontweight='bold')
        plt.xlabel("Hour of Day", fontsize=12)
        plt.ylabel("Day of Week", fontsize=12)
        plt.xticks(rotation=45)
        plt.yticks(rotation=0)
        heatmap_filename = "heatmap.png"
        plt.savefig(f"static/images/{heatmap_filename}", bbox_inches='tight', pad_inches=0.1)
        plt.close()
        image_files.append(heatmap_filename)
        logger.info(f"Saved {heatmap_filename}")

        # 7. LEAGUE STANDINGS TABLE
        logger.info("Computing league standings...")
        df['home_points'] = df['score.winner'].apply(
            lambda x: 3 if x == 'HOME_TEAM' else (1 if x == 'DRAW' else 0)
        )
        df['away_points'] = df['score.winner'].apply(
            lambda x: 3 if x == 'AWAY_TEAM' else (1 if x == 'DRAW' else 0)
        )
        df['home_goal_difference'] = df['score.fullTime.home'] - df['score.fullTime.away']
        df['away_goal_difference'] = df['score.fullTime.away'] - df['score.fullTime.home']

        home_stats = df.groupby('homeTeam.shortName').agg(
            total_points=('home_points', 'sum'),
            total_goals_scored=('score.fullTime.home', 'sum'),
            total_goals_conceded=('score.fullTime.away', 'sum'),
            goal_difference=('home_goal_difference', 'sum')
        )

        away_stats = df.groupby('awayTeam.shortName').agg(
            total_points=('away_points', 'sum'),
            total_goals_scored=('score.fullTime.away', 'sum'),
            total_goals_conceded=('score.fullTime.home', 'sum'),
            goal_difference=('away_goal_difference', 'sum')
        )

        team_stats = home_stats.add(away_stats, fill_value=0)
        team_stats = team_stats.sort_values(by=['total_points', 'goal_difference'], ascending=[False, False])

        # Reset index and rename the team column
        standings_df = team_stats.reset_index().rename(columns={'homeTeam.shortName': 'team'})

        # Handle cases where 'homeTeam.shortName' may not exist after aggregation
        if 'homeTeam.shortName' not in standings_df.columns:
            standings_df = standings_df.rename(columns={'index': 'team'})

        standings = standings_df.to_dict(orient='records')
        logger.info(f"Standings computed: {standings}")

        return image_files, standings
    except Exception as e:
        logger.error(f"Error creating visualizations: {e}")
        return [], []

def prepare_frontend_data(image_files, standings):
    """
    Prepares the data to be sent to the frontend.
    Returns a dictionary with image URLs and standings.
    """
    try:
        image_urls = [f"/static/images/{img}" for img in image_files]
        return {"images": image_urls, "standings": standings}
    except Exception as e:
        logger.error(f"Error preparing data for frontend: {e}")
        return {"images": [], "standings": []}

@app.route('/api/league/<league_code>/visualizations', methods=['GET'])
def get_visualizations(league_code):
    """
    Endpoint to fetch visualizations and standings for a given league.
    """
    try:
        current_date = datetime.now(timezone.utc).strftime('%Y%m%d')
        file_key = f"{league_code}_matches_{current_date}.json"
        df = fetch_data_from_s3(file_key)
        if df is not None:
            image_files, standings = create_all_visualizations(df)
            frontend_data = prepare_frontend_data(image_files, standings)
            return jsonify(frontend_data)
        else:
            # Attempt to fetch the latest available file
            logger.info(f"Attempting to fetch the latest file for league {league_code}")
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=f"{league_code}_matches_")
            files = response.get('Contents', [])
            if not files:
                raise FileNotFoundError(f"No files found for league {league_code} in S3 bucket.")

            # Sort files by last modified date and get the latest one
            latest_file = max(files, key=lambda x: x['LastModified'])['Key']
            logger.info(f"Fetching latest file: {latest_file}")
            df = fetch_data_from_s3(latest_file)
            if df is None:
                raise ValueError("Data format is incorrect in the latest file.")
            image_files, standings = create_all_visualizations(df)
            frontend_data = prepare_frontend_data(image_files, standings)
            return jsonify(frontend_data)
    except FileNotFoundError as fnf_error:
        logger.error(fnf_error)
        return jsonify({"error": str(fnf_error)}), 404
    except Exception as e:
        logger.error(f"Error fetching visualizations for {league_code}: {e}")
        return jsonify({"error": str(e)}), 500

@app.route('/static/images/<path:filename>')
def serve_image(filename):
    """
    Serves images from the static/images directory.
    """
    try:
        return send_from_directory('static/images', filename)
    except Exception as e:
        logger.error(f"Error serving image {filename}: {e}")
        return jsonify({"error": "Image not found."}), 404

@app.route('/')
def health_check():
    """
    Health check endpoint.
    """
    return jsonify({"status": "Backend is running."}), 200

if __name__ == '__main__':
    app.run(debug=True)
