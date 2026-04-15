import spotipy, csv, time, os
from spotipy.oauth2 import SpotifyClientCredentials
from config import CLIENT_ID, CLIENT_SECRET, CHECKPOINT_FILE, CHECKPOINT_DONE

COLS = [
    "track_id", "name", "artist", "artist_id", "all_artists", "artist_count",
    "album", "album_id", "album_type", "release_date", "duration_ms",
    "explicit", "track_number", "popularity", "isrc", "album_total_tracks"
]


def search(sp, q, limit, offset=0):
    #https://spotipy.readthedocs.io/en/2.26.0/index.html#spotipy.client.Spotify.search
    return sp.search(q=q, type="track", limit=limit, offset=offset, market="US")


def parse_one_track(track):
    if "artists" in track:
        artists = track["artists"]
    if "album" in track:
        album = track["album"]
    return {
        "track_id": track["id"],
        "name": track["name"],
        "artist": artists[0]["name"] if artists else "",
        "artist_id": artists[0]["id"] if artists else "",
        "all_artists": "; ".join(x["name"] for x in artists),
        "artist_count": len(artists),
        "album": album["name"],
        "album_id": album["id"],
        "album_type": album["album_type"],
        "release_date": album["release_date"],
        "duration_ms": track["duration_ms"],
        "explicit": track["explicit"],
        "track_number": track["track_number"],
        "popularity": track.get("popularity") if track.get("popularity") != "" else "",
        "isrc": track.get("external_ids", {}).get("isrc", ""),
        "album_total_tracks": album.get("total_tracks", ""),
    }


def cast_track(row):
    row["artist_count"] = int(row["artist_count"]) if row["artist_count"] else 0
    row["duration_ms"] = int(row["duration_ms"]) if row["duration_ms"] else 0
    row["track_number"] = int(row["track_number"]) if row["track_number"] else 1
    row["explicit"] = row["explicit"] == "True"
    row["popularity"] = int(row["popularity"]) if row["popularity"] != "" else ""
    row["album_total_tracks"] = int(row["album_total_tracks"]) if row["album_total_tracks"] else ""
    return row


def load_checkpoint():
    tracks = {}
    done = set()
    if os.path.exists(CHECKPOINT_FILE):
        with open(CHECKPOINT_FILE, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                tracks[row["track_id"]] = cast_track(row)
    if os.path.exists(CHECKPOINT_DONE):
        with open(CHECKPOINT_DONE, encoding="utf-8") as f:
            for line in f:
                prefix, year = line.strip().split(" ")
                done.add((prefix, int(year)))
    return tracks, done


def save_checkpoint(tracks, done):
    with open(CHECKPOINT_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=COLS, restval="")
        writer.writeheader()
        for track in tracks.values():
            writer.writerow(track)
    with open(CHECKPOINT_DONE, "w", encoding="utf-8") as f:
        for prefix, year in done:
            f.write(f"{prefix} {year}\n")



def main(total_tracks=15000, prefixes = list("abcdefghijklmnopqrstuvwxyz1234567890"), lim = 10, delay = 1.5, y=(1970, 2027)):
    years = range(y[0], y[1])
    err_counter = 0
    sp = spotipy.Spotify(client_credentials_manager=SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET), requests_timeout=5, retries=1)

    tracks, done = load_checkpoint()
    print(f"Starting. Preloaded {len(tracks)} tracks")
    for prefix in prefixes:
        for year in years:
            if len(tracks) >= total_tracks:
                break
            if (prefix, year) in done:
                continue
            q = f"{prefix} year:{year}"
            api_ok = False
            for offset in range(0, 1000, lim):
                if len(tracks) >= total_tracks:
                    break
                result = search(sp=sp, q=q, limit=lim, offset=offset)
                if result is None:
                    err_counter += 1
                    if err_counter >= 5:
                        print(f"Error occurred, retrying after 30 seconds...")
                        err_counter = 0
                        time.sleep(30)
                        continue
                time.sleep(delay)
                if not result:
                    break
                api_ok = True
                items = result.get("tracks", {}).get("items", [])
                if not items:
                    break
                for item in items:
                    track = parse_one_track(item)
                    if track and track["track_id"] not in tracks:
                        tracks[track["track_id"]] = track
                        print(f"Collected {track['name']}, got ({len(tracks)}) total")
                    else:
                        print(f"Skipping duplicate {track["name"]}")
                if len(items) < lim:
                    break
            if api_ok:
                done.add((prefix, year))
                save_checkpoint(tracks, done)
            print(f"{len(tracks)} tracks collected")
        if len(tracks) >= total_tracks:
            break

    save_checkpoint(tracks, done)
    print(f"Finished with total {len(tracks)} tracks")


if __name__ == "__main__":
    main()
