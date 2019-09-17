song_staging_columns = [
'artist_id' ,
'artist_latitude' ,
'artist_location' ,
'artist_longitude' ,
'artist_name' ,
'duration' ,
'num_songs' ,
'song_id' ,
'title' ,
'year'
]

d_artist_columns = [
'artist_id' ,
'artist_latitude' ,
'artist_location' ,
'artist_longitude' ,
'artist_name' ,
]

d_song_columns = [
'song_id',
'title',
'duration',
'year',
'artist_id'
]

log_staging_columns = [
'artist' ,
'auth' ,
'firstName' ,
'gender' ,
'itemInSession' , 
'lastName' ,
'length',
'level' ,
'location' ,
'method' ,
'page' ,
'registration',
'sessionId' ,
'song' ,
'status' ,
'ts',
'userAgent' ,
'userId'
]

d_timestamp_columns = [
'year',
'month',
'day',
'minute',
'second',
'hour',
'weekday',
'timestamp',
]

d_app_user_columns = [
  'userId',
  'firstName',
  'lastName',
  'gender',
  'level'
]

all_tables = [
  'log_staging',
  'song_staging',
  'd_artist',
  'd_song',
  'd_app_user',
  'd_timestamp',
  'f_songplay'
]