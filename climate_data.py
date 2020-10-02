import requests, zlib, json, csv, sys, traceback
import numpy as np
from tqdm import tqdm
from collections import defaultdict 
from multiprocessing import Pool
from shapely.geometry import Point, shape

#  DATA SOURCING:
#    ghcnd-stations.txt from https://docs.opendata.aws/noaa-ghcn-pds/readme.html
#    us-county-boundaries.csv from https://public.opendatasoft.com/explore/dataset/us-county-boundaries/export/
#    neighbors-states.csv from https://raw.githubusercontent.com/ubikuity/List-of-neighboring-states-for-each-US-state/master/neighbors-states.csv

# General approach: incrementally produce useful datasets in a
# useful format (JSON, or .npy if space constrained) to prevent
# repeating computationally expensive work.

# Reformat GHCND stations as json
def us_ghcnd_stations_to_json():
    with open('extra_data/ghcnd-stations.txt') as f:
        raw = f.read().split('\n')
    
    us_only = [r.split() for r in raw if r.startswith('US')]
    
    stations = {}
    for code, lat, long, _, state, *_ in us_only:
        stations[code.upper()] = {'coord':(float(lat),float(long)), 'state':state}
    
    with open('extra_data/ghcnd-stations-us.json', 'w') as f:
        json.dump(stations, f)

# Returns a dictionary of neighboring states for each state
def get_state_neighbors():
    neighbors = defaultdict(lambda: set())
    with open('extra_data/neighbors-states.csv') as f:
        c = csv.reader(f)
        next(c)
        
        for state, neighbor in c:
            neighbors[state].add(neighbor)
            neighbors[neighbor].add(state)
     
    return {n:list(o) for n,o in neighbors.items()}
     
# Reformats US county boundary data into JSON for easier reading
def us_county_boundaries_to_json():
    states = {}
    csv.field_size_limit(sys.maxsize)

    with open('extra_data/us-county-boundaries.csv') as f:
        c = csv.reader(f, delimiter=';')
        
        next(c)
        
        for line in tqdm(c):
            county_ns  = int(line[2]+line[3])
            
            if (county_ns == 2013):
                print(county_ns)
                print(line[2:7])
                sys.exit()
            state      = line[8]

            polygon    = json.loads(line[1])  
            
            if state in states:
                states[state][county_ns] = polygon
            
            else:
                states[state] = {county_ns:polygon}
    
    print('Dumping...')
    with open('extra_data/us-county-boundaries.json', 'w') as f:
        json.dump(states, f)

# Check if a given lat long is in a polygon, used for assigning
# weather stations to counties
def in_polygon(latlong, polygon):
    try:
        p, po = Point(latlong[1],latlong[0]), shape(polygon)
        inside = po.contains(p)
    except Exception as e:
        traceback.print_exc()

        print(polygon[0], len(polygon))
        sys.exit()
    
    return inside

# Computes the county for each weather station by checking
# all counties within the state, and neighboring states if that
# does not succeed (as some stations are mislabeled). This takes
# a while to run.
def generate_ghcnd_to_county():
    print('Loading data...')
    
    with open('extra_data/ghcnd-stations-us.json') as f:
        stations = json.load(f)
    
    with open('extra_data/us-county-boundaries.json') as f:
        counties = json.load(f)
    
    state_neighbors = get_state_neighbors()
        
    # stations[GHCND] = {'state':STATE, 'coord':[lat,long]}
    # counties[STATE] = {COUNTY_ID: POLYGON}
    # state_neighbors[STATE] = [BORDERING_STATE, BORDERING_STAT]

    # Our goal: for each ghcnd station, produce the appropriate county ID
    mapping = {}
    for ghcnd, station in tqdm(stations.items()):
        found_match = False
        
        if station['state'] not in counties:
            # likely outer islands, just skip for now
            continue
        
        for state in [station['state']] + state_neighbors[station['state']]:
            for county_id, polygon in counties[state].items():
                if in_polygon(station['coord'], polygon):
                    mapping[ghcnd.upper()] = int(county_id)
                    found_match = True
                    break
            
            if found_match:
                break
        
        if not found_match:
            print(f'No county match for {ghcnd}, skipping.')

    
    with open('extra_data/ghcnd-to-county-id.json', 'w') as f:
        json.dump(mapping, f)
            
# Pull NOAA daily data for given year and return as CSV string
def fetch_year_csv_str(year):
    url = f'https://www1.ncdc.noaa.gov/pub/data/ghcn/daily/by_year/{year}.csv.gz'
    
    response = requests.get(url).content
    decompressed = zlib.decompress(response, 15+32)
    return decompressed.decode('utf-8')

# Generate monthly aggregate data for a given year
def process_year(year):    
    csv_year = fetch_year_csv_str(year).strip().split('\n')
    
    with open('extra_data/ghcnd-to-county-id.json') as f:
        station_to_county = json.load(f)
    
    r = csv.reader(csv_year)
    counties = sorted(list(set(station_to_county.values())))
    county_index = {county:i for i, county in enumerate(counties)}
    
    # Raw Fields:
    #   TMIN (10ths of degrees C)
    #   TMAX (10ths of degrees C)
    #   PRCP (10ths of mm)
    #   SNOW (snowfall mm)
    #   SNWD (snow depth mm)
    
    fields = ['TMIN', 'TMAX', 'PRCP', 'SNOW', 'SNWD']
            
    props = np.zeros((12, len(counties), len(fields)), dtype=np.float32)
    prop_counts = np.zeros((12, len(counties), len(fields)), dtype=int)
            
    for station, date_string, prop, value, *_ in r:
        if not station.upper() in station_to_county:
            continue
        
        station = station.upper()
        year, month, day = int(date_string[:4]), int(date_string[4:6]), int(date_string[6:8])
        date_int = int(date_string)
        
        if not prop in fields:
            continue
        
        # Now storing degrees C and cm
        value = float(value) / 10
                
        county = county_index[station_to_county[station.upper()]]
        
        props[month-1, county, fields.index(prop)] += value
        prop_counts[month-1, county, fields.index(prop)] += 1
    
    props /= prop_counts
    
    return props

# Generates full multi-year dataset by calling process_year
# in parallel using Multiprocessing.  Warning: this method takes
# a while to run (~hour), and requires a fair amount of memory
# (I saturated 12GB RAM and 16GB swap using 6 cores).
def make_county_weather_data(cores=6):    
    start_year = 1900
    end_year   = 2016
    with Pool(processes=cores) as pool:
        all_data = list(tqdm(pool.imap(
            process_year, range(start_year,end_year+1)
        ), total=end_year-start_year+1))
            
    all_data = np.concatenate(all_data, axis=0)
    
    print(f'Computed shape: {all_data.shape}')
    
    with open(f'extra_data/monthly_county_weather_{start_year}_to_{end_year}.npy', 'wb') as f:
        np.save(f, all_data)
    
    
if __name__ == '__main__':
    make_county_weather_data()