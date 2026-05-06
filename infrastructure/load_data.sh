curl --location-trusted -u root: \
    -H "label:load_tracks_$(date +%s)" \
    -H "column_separator:," \
    -H "enclose:\"" \
    -H "skip_header:1" \
    -H "max_filter_ratio:0.1" \
    -H "columns: tmp_index, track_id, artists, album_name, track_name, popularity, tmp_duration, tmp_explicit, tmp_dance, tmp_energy, tmp_key, tmp_loud, tmp_mode, tmp_speech, tmp_acoustic, tmp_instrumental, tmp_liveness, tmp_valence, track_tempo, tmp_signature, track_genre, artist_name=artists" \
    -T /tmp/dataset.csv \
    http://starrocks-fe:8030/api/music_db/dim_track/_stream_load
