"""
Geohash demo: Redis GEO commands for location-based lookups.

Redis stores coordinates as a sorted set using the geohash encoding —
each location is encoded into a 52-bit integer score so standard sorted
set operations (range queries, nearest-neighbour search) work on geo data.

Covers:
  GEOADD    — store lat/lon for a named location
  GEODIST   — straight-line distance between two stored points
  GEOPOS    — retrieve stored coordinates
  GEOSEARCH — find all points within a radius of a given centre
"""
import redis

KEY = "coffee_shops"

SHOPS = [
    ("Blue Bottle",     37.7941,  -122.3953),
    ("Sightglass",      37.7786,  -122.4056),
    ("Ritual Roasters", 37.7585,  -122.4111),
    ("Four Barrel",     37.7652,  -122.4215),
    ("Verve",           37.7869,  -122.4023),
]

# Search centre: roughly Union Square, San Francisco
SEARCH_LAT  =  37.7879
SEARCH_LON  = -122.4075
SEARCH_NAME = "Union Square"


def main():
    client = redis.Redis(host="localhost", port=6379, decode_responses=True)
    client.delete(KEY)

    # ── GEOADD ──────────────────────────────────────────────────────────────
    print("\n--- Adding locations (GEOADD) ---\n")
    for name, lat, lon in SHOPS:
        client.geoadd(KEY, [lon, lat, name])
        print(f"  Added: {name:20s} ({lat}, {lon})")

    # ── GEOPOS ──────────────────────────────────────────────────────────────
    print("\n--- Retrieving stored coordinates (GEOPOS) ---\n")
    names = [s[0] for s in SHOPS]
    positions = client.geopos(KEY, *names)
    for name, pos in zip(names, positions):
        lon_r, lat_r = pos
        print(f"  {name:20s} → ({float(lat_r):.4f}, {float(lon_r):.4f})")

    # ── GEODIST ─────────────────────────────────────────────────────────────
    print("\n--- Distances from Blue Bottle (GEODIST) ---\n")
    origin = "Blue Bottle"
    for name, _, _ in SHOPS[1:]:
        dist = client.geodist(KEY, origin, name, unit="km")
        print(f"  {origin} → {name:20s}  {dist:.3f} km")

    # ── GEOSEARCH ───────────────────────────────────────────────────────────
    radius_km = 2.0
    print(f"\n--- Shops within {radius_km} km of {SEARCH_NAME} (GEOSEARCH) ---\n")
    results = client.geosearch(
        KEY,
        longitude=SEARCH_LON,
        latitude=SEARCH_LAT,
        radius=radius_km,
        unit="km",
        sort="ASC",
        withcoord=True,
        withdist=True,
    )
    if results:
        for name, dist, (lon_r, lat_r) in results:
            print(f"  {name:20s}  {float(dist):.3f} km  ({float(lat_r):.4f}, {float(lon_r):.4f})")
    else:
        print("  No shops found in radius.")

    print()


if __name__ == "__main__":
    main()
