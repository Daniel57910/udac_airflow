f_songplay_insert = '''INSERT INTO f_songplay 
  (start_time, 
  user_id,
  level,
  song_key,
  artist_key,
  session_id , 
  location ,
  user_agent) 
SELECT
  log_staging.timestamp,
  d_app_user.app_user_key,
  log_staging.level,
  d_song.song_key,
  d_artist.artist_key,
  log_staging.session_id,
  log_staging.location,
  log_staging.user_agent
FROM
  log_staging
LEFT JOIN d_artist on d_artist.artist_name = log_staging.artist
LEFT JOIN d_song on d_song.artist_id = d_artist.artist_id
INNER JOIN d_app_user on d_app_user.first_name = log_staging.first_name and d_app_user.last_name = log_staging.last_name'''
