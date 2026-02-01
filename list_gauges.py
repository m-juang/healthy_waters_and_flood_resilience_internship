import json

data = json.load(open(r'outputs\rain_gauges\20260121-20260122\raw\rain_gauges_traces_alarms.json'))

# List gauges with approximate coordinates based on name
# These are well-known Auckland locations
KNOWN_LOCATIONS = {
    "Swanson": (174.58, -36.87),
    "Henderson": (174.63, -36.88),
    "New Lynn": (174.68, -36.91),
    "Mangere": (174.79, -36.97),
    "Mt Roskill": (174.74, -36.91),
    "Avondale": (174.70, -36.90),
    "Takapuna": (174.77, -36.79),
    "Albany": (174.72, -36.73),
    "Kumeu": (174.55, -36.78),
    "Whenuapai": (174.63, -36.79),
    "Birkdale": (174.71, -36.80),
    "Rosedale": (174.73, -36.74),
    "Papakura": (174.94, -37.07),
    "Drury": (174.97, -37.10),
    "Piha": (174.47, -36.95),
    "Waitakere": (174.55, -36.90),
    "Waiatarua": (174.55, -36.93),
    "Oratia": (174.58, -36.92),
    "Albert Park": (174.77, -36.85),
    "Okahu Bay": (174.79, -36.86),
    "Mt Albert": (174.72, -36.89),
    "Nihotupu": (174.51, -36.96),
    "Huia": (174.52, -37.00),
    "Pakuranga": (174.88, -36.91),
    "Manukau": (174.86, -36.99),
    "Botanic": (174.91, -36.98),
    "Clevedon": (175.03, -36.98),
}

print("Gauges with estimated coordinates:\n")
print(f"{'ID':<10} {'Name':<50} {'Est. Lon':>10} {'Est. Lat':>10}")
print("-" * 85)

for g in data:
    gauge = g['gauge']
    gid = gauge['id']
    name = gauge['name']
    
    # Find matching location
    lon, lat = None, None
    for loc, coords in KNOWN_LOCATIONS.items():
        if loc.lower() in name.lower():
            lon, lat = coords
            break
    
    if lon and lat:
        print(f"{gid:<10} {name[:49]:<50} {lon:>10.2f} {lat:>10.2f}")
