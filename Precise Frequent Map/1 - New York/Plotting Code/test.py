import json

# Plot boroughs.
with open("json/BoroughOutline.json") as json_file:
    json_data = json.load(json_file)
for f in json_data['features']:
    coords = f['geometry']['coordinates']
    print(len(coords[0]))
    x = [c[0] for c in coords]
    y = [c[1] for c in coords]
    print(x)