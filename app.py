import os
import sys

# Workaround for protobuf compatibility issue
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

from flask import Flask, render_template, jsonify, send_from_directory
import requests
from datetime import datetime
import threading
import time
import json

app = Flask(__name__)

# Try to import protobuf with error handling
try:
    # Try importing the protobuf module
    from google.transit import gtfs_realtime_pb2
    from google.protobuf.json_format import MessageToDict
    PROTOBUF_AVAILABLE = True
    print("✅ Protobuf module loaded successfully")
except ImportError as e:
    print(f"⚠️ Protobuf import error: {e}")
    print("   Will use mock data mode")
    PROTOBUF_AVAILABLE = False
except Exception as e:
    print(f"⚠️ Error loading protobuf: {e}")
    print("   Will use mock data mode")
    PROTOBUF_AVAILABLE = False

# Configuration
TTC_API_URL = "https://bustime.ttc.ca/gtfsrt/vehicles"
UPDATE_INTERVAL = 30  # seconds

# In-memory storage for bus data
bus_data = {
    'vehicles': [],
    'last_updated': None,
    'error': None,
    'mode': 'realtime' if PROTOBUF_AVAILABLE else 'mock',
    'available_routes': set()
}

# Lock for thread-safe updates
data_lock = threading.Lock()

def fetch_ttc_data():
    """Fetch data from TTC API with fallback mechanisms"""
    if not PROTOBUF_AVAILABLE:
        return None, set()
    
    try:
        print(f"🔍 Fetching data from TTC API...")
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/x-protobuf'
        }
        
        response = requests.get(TTC_API_URL, headers=headers, timeout=10)
        
        if response.status_code != 200:
            print(f"❌ TTC API returned status: {response.status_code}")
            return None, set()
        
        # Try to parse as protobuf
        try:
            feed = gtfs_realtime_pb2.FeedMessage()
            feed.ParseFromString(response.content)
            
            vehicles = []
            available_routes = set()
            
            for entity in feed.entity:
                if entity.HasField("vehicle"):
                    vehicle_dict = MessageToDict(
                        entity.vehicle,
                        preserving_proto_field_name=True,
                        including_default_value_fields=True
                    )
                    
                    # Ensure all required fields exist
                    vehicle_info = vehicle_dict.get('vehicle', {})
                    trip_info = vehicle_dict.get('trip', {})
                    position_info = vehicle_dict.get('position', {})
                    
                    # Get real vehicle ID
                    vehicle_id = vehicle_info.get('id', 'Unknown')
                    if vehicle_id == 'Unknown':
                        continue
                    
                    # Get route ID
                    route_id = trip_info.get('routeId', 'Unknown')
                    if route_id != 'Unknown':
                        available_routes.add(route_id)
                    
                    # Get coordinates
                    lat = position_info.get('latitude')
                    lon = position_info.get('longitude')
                    
                    if lat is None or lon is None:
                        continue
                    
                    # Get bearing and speed
                    bearing = position_info.get('bearing', 0)
                    speed = position_info.get('speed', 0)
                    
                    # Get occupancy status
                    occupancy = vehicle_dict.get('occupancyStatus', 'UNKNOWN')
                    
                    # Get timestamp
                    timestamp = vehicle_dict.get('timestamp')
                    if timestamp:
                        try:
                            timestamp = datetime.fromtimestamp(int(timestamp)).isoformat()
                        except:
                            timestamp = datetime.now().isoformat()
                    else:
                        timestamp = datetime.now().isoformat()
                    
                    # Create flat structure for frontend
                    processed_vehicle = {
                        'vehicle_id': vehicle_id,
                        'vehicle_label': vehicle_info.get('label', f'Bus {vehicle_id}'),
                        'route_id': route_id,
                        'latitude': float(lat),
                        'longitude': float(lon),
                        'speed_mps': float(speed) if speed else 0.0,
                        'bearing': float(bearing) if bearing else 0.0,
                        'timestamp': timestamp,
                        'occupancy_status': occupancy,
                        'trip_id': trip_info.get('tripId', ''),
                        'start_time': trip_info.get('startTime', ''),
                        'schedule_relationship': trip_info.get('scheduleRelationship', 'SCHEDULED')
                    }
                    
                    vehicles.append(processed_vehicle)
            
            print(f"✅ Successfully parsed {len(vehicles)} vehicles from TTC feed")
            return vehicles, available_routes
            
        except Exception as parse_error:
            print(f"⚠️ Failed to parse protobuf: {parse_error}")
            return None, set()
            
    except requests.RequestException as e:
        print(f"⚠️ Network error: {e}")
        return None, set()
    except Exception as e:
        print(f"⚠️ Unexpected error: {e}")
        return None, set()

def generate_mock_buses():
    """Generate realistic mock bus data for Toronto"""
    import random
    vehicles = []
    available_routes = set()
    
    # Toronto center coordinates
    toronto_center = (43.6532, -79.3832)
    
    # Common TTC routes (matching actual TTC routes)
    routes = ['5', '6', '7', '8', '9', '10', '11', '12', '14', '16', '17', '19', '20', 
              '21', '22', '23', '24', '25', '26', '29', '32', '34', '35', '36', '37',
              '38', '39', '40', '41', '42', '43', '44', '45', '46', '47', '48', '49',
              '50', '51', '52', '53', '54', '55', '56', '57', '59', '60', '61', '63',
              '65', '66', '67', '68', '69', '70', '71', '72', '73', '74', '75', '76',
              '77', '78', '79', '80', '81', '82', '83', '84', '85', '86', '87', '88',
              '89', '90', '91', '92', '94', '95', '96', '97', '98', '99', '100', 
              '101', '102', '103', '104', '105', '106', '107', '108', '109', '110',
              '111', '112', '113', '115', '116', '117', '118', '119', '120', '121',
              '122', '123', '124', '125', '126', '127', '129', '130', '131', '132',
              '133', '134', '135', '139', '141', '142', '143', '144', '145', '146',
              '147', '148', '149', '150', '151', '152', '153', '154', '155', '160',
              '161', '162', '165', '167', '168', '169', '171', '172', '176', '189',
              '190', '191', '192', '193', '195', '196', '199', '224', '300', '301',
              '304', '306', '307', '310', '312', '315', '320', '322', '324', '325',
              '329', '332', '334', '335', '336', '337', '339', '341', '343', '352',
              '353', '354', '363', '365', '384', '385', '395', '396', '501', '502',
              '503', '504', '505', '506', '509', '510', '511', '512']
    
    # Generate 80-120 mock buses
    num_buses = random.randint(80, 120)
    
    print(f"🎭 Generating {num_buses} mock buses...")
    
    for i in range(num_buses):
        # Spread buses around Toronto
        lat = toronto_center[0] + random.uniform(-0.15, 0.15)  # ~15km radius
        lon = toronto_center[1] + random.uniform(-0.15, 0.15)
        
        # Keep within reasonable Toronto bounds
        lat = max(43.58, min(43.86, lat))
        lon = max(-79.63, min(-79.12, lon))
        
        route = random.choice(routes)
        available_routes.add(route)
        
        # Generate realistic vehicle ID (matching TTC format)
        vehicle_id = f"{random.randint(1000, 9999)}"
        
        vehicle = {
            'vehicle_id': vehicle_id,
            'vehicle_label': f'Bus {vehicle_id}',
            'route_id': route,
            'latitude': lat,
            'longitude': lon,
            'speed_mps': random.uniform(0, 12),  # 0-12 m/s (0-43 km/h)
            'bearing': random.uniform(0, 360),
            'timestamp': datetime.now().isoformat(),
            'occupancy_status': random.choice(['EMPTY', 'MANY_SEATS_AVAILABLE', 
                                               'FEW_SEATS_AVAILABLE', 'FULL', 'STANDING_ROOM_ONLY', 'UNKNOWN']),
            'trip_id': f'trip_{i:04d}',
            'start_time': '',
            'schedule_relationship': 'SCHEDULED'
        }
        vehicles.append(vehicle)
    
    return vehicles, available_routes

def update_mock_buses():
    """Update mock bus positions with realistic movement"""
    import random
    
    with data_lock:
        if not bus_data['vehicles']:
            vehicles, routes = generate_mock_buses()
            bus_data['vehicles'] = vehicles
            bus_data['available_routes'] = routes
        else:
            for bus in bus_data['vehicles']:
                # Move bus based on its speed and direction
                speed_mps = bus['speed_mps']
                bearing = bus['bearing']
                
                # Convert speed to degrees (rough approximation)
                if speed_mps > 0:
                    # Convert m/s to degrees (roughly: 1 m/s ≈ 0.00001 degrees)
                    distance_deg = speed_mps * 0.00001 * UPDATE_INTERVAL
                    
                    # Convert bearing to radians
                    bearing_rad = bearing * 3.14159 / 180
                    
                    # Calculate new position
                    delta_lat = distance_deg * random.uniform(0.5, 1.5)
                    delta_lon = distance_deg * random.uniform(0.5, 1.5)
                    
                    # Apply movement based on bearing
                    bus['latitude'] += delta_lat * random.uniform(-1, 1)
                    bus['longitude'] += delta_lon * random.uniform(-1, 1)
                    
                    # Keep within Toronto bounds
                    bus['latitude'] = max(43.58, min(43.86, bus['latitude']))
                    bus['longitude'] = max(-79.63, min(-79.12, bus['longitude']))
                
                # Update bearing slightly
                bus['bearing'] = (bus['bearing'] + random.uniform(-10, 10)) % 360
                
                # Update speed (buses accelerate and decelerate)
                speed_change = random.uniform(-1, 1)
                new_speed = max(0, min(15, bus['speed_mps'] + speed_change))
                bus['speed_mps'] = new_speed
                
                # Update timestamp
                bus['timestamp'] = datetime.now().isoformat()

def fetch_bus_data():
    """Main function to fetch or generate bus data"""
    print(f"\n🔄 Updating bus data... (Mode: {bus_data['mode']})")
    
    if PROTOBUF_AVAILABLE and bus_data['mode'] == 'realtime':
        vehicles, routes = fetch_ttc_data()
        
        if vehicles is not None:
            with data_lock:
                bus_data['vehicles'] = vehicles
                bus_data['available_routes'] = routes
                bus_data['last_updated'] = datetime.now()
                bus_data['error'] = None
                bus_data['mode'] = 'realtime'
            return
    
    # If we get here, use mock data
    if bus_data['mode'] == 'realtime':
        print("⚠️ Switching to mock data mode")
    
    # Update mock buses
    update_mock_buses()
    
    with data_lock:
        bus_data['last_updated'] = datetime.now()
        bus_data['error'] = "Using realistic simulation data"
        bus_data['mode'] = 'mock'
    
    print(f"   Mock buses active: {len(bus_data['vehicles'])}")

def update_data_loop():
    """Background thread to periodically update bus data"""
    print("🚀 Starting data update thread...")
    while True:
        try:
            fetch_bus_data()
            time.sleep(UPDATE_INTERVAL)
        except Exception as e:
            print(f"❌ Error in update loop: {e}")
            time.sleep(10)  # Wait a bit before retrying

# Flask Routes
@app.route('/')
def index():
    """Render the main map page"""
    return render_template('index.html')

@app.route('/api/live-buses')
def get_buses():
    """API endpoint to get current bus data"""
    with data_lock:
        # Get query parameters
        route_filter = request.args.get('route', '').strip()
        
        vehicles = bus_data['vehicles']
        
        # Apply route filter if specified
        if route_filter and route_filter != 'all':
            filtered_vehicles = []
            for vehicle in vehicles:
                if vehicle['route_id'] == route_filter:
                    filtered_vehicles.append(vehicle)
            vehicles = filtered_vehicles
        
        return jsonify({
            'vehicles': vehicles,
            'last_updated': bus_data['last_updated'].isoformat() if bus_data['last_updated'] else None,
            'error': bus_data['error'],
            'count': len(vehicles),
            'mode': bus_data['mode']
        })

@app.route('/api/live-routes')
def get_live_routes():
    """API endpoint to get list of routes with active buses"""
    with data_lock:
        routes = list(bus_data['available_routes'])
        
        # Sort routes numerically
        def route_sort_key(route):
            try:
                return int(route)
            except ValueError:
                return float('inf')
        
        routes.sort(key=route_sort_key)
        
        return jsonify({
            'routes': routes,
            'count': len(routes),
            'last_updated': bus_data['last_updated'].isoformat() if bus_data['last_updated'] else None
        })

@app.route('/api/stats')
def get_stats():
    """API endpoint for statistics"""
    with data_lock:
        # Count buses per route
        route_counts = {}
        for vehicle in bus_data['vehicles']:
            route_id = vehicle.get('route_id', 'Unknown')
            route_counts[route_id] = route_counts.get(route_id, 0) + 1
        
        # Sort routes by count
        sorted_routes = dict(sorted(route_counts.items(), 
                                   key=lambda x: x[1], reverse=True)[:10])
        
        # Calculate average speed
        total_speed = 0
        moving_buses = 0
        for vehicle in bus_data['vehicles']:
            speed = vehicle.get('speed_mps', 0)
            if speed > 0:
                total_speed += speed
                moving_buses += 1
        
        avg_speed_kmh = (total_speed * 3.6 / moving_buses) if moving_buses > 0 else 0
        
        return jsonify({
            'total_buses': len(bus_data['vehicles']),
            'routes_active': len(route_counts),
            'top_routes': sorted_routes,
            'last_updated': bus_data['last_updated'].isoformat() if bus_data['last_updated'] else None,
            'mode': bus_data['mode'],
            'avg_speed_kmh': round(avg_speed_kmh, 1),
            'moving_buses': moving_buses
        })

@app.route('/api/mode/<new_mode>')
def set_mode(new_mode):
    """API endpoint to switch between realtime and mock modes"""
    with data_lock:
        if new_mode in ['realtime', 'mock']:
            bus_data['mode'] = new_mode
            if new_mode == 'mock':
                vehicles, routes = generate_mock_buses()
                bus_data['vehicles'] = vehicles
                bus_data['available_routes'] = routes
            return jsonify({'status': 'success', 'mode': new_mode})
        return jsonify({'status': 'error', 'message': 'Invalid mode'})

@app.route('/health')
def health():
    """Health check endpoint"""
    with data_lock:
        return jsonify({
            'status': 'healthy',
            'buses_available': len(bus_data['vehicles']),
            'last_updated': bus_data['last_updated'].isoformat() if bus_data['last_updated'] else None,
            'mode': bus_data['mode'],
            'error': bus_data['error'],
            'protobuf_available': PROTOBUF_AVAILABLE
        })

@app.route('/<path:path>')
def serve_static(path):
    """Serve static files"""
    return send_from_directory('.', path)

if __name__ == '__main__':
    print("=" * 60)
    print("🚍 TTC Bus Tracker - Starting up...")
    print("=" * 60)
    
    print(f"\n📊 System Information:")
    print(f"   Python: {sys.version}")
    print(f"   Protobuf Available: {PROTOBUF_AVAILABLE}")
    
    if not PROTOBUF_AVAILABLE:
        print(f"\n⚠️  IMPORTANT: Protobuf module not available")
        print(f"   To enable real-time data from TTC, install:")
        print(f"   pip install protobuf==3.20.3 gtfs-realtime-bindings==0.0.7")
        print(f"   Using realistic simulation mode for now.")
    
    # Initial data fetch
    fetch_bus_data()
    
    # Start background update thread
    update_thread = threading.Thread(target=update_data_loop, daemon=True)
    update_thread.start()
    
    print(f"\n🌐 Web Interface: http://localhost:5000")
    print(f"📈 Stats API: http://localhost:5000/api/stats")
    print(f"🚌 Live Buses API: http://localhost:5000/api/live-buses")
    print(f"🛣️ Live Routes API: http://localhost:5000/api/live-routes")
    print(f"❤️  Health: http://localhost:5000/health")
    print(f"🔄 Data updates every {UPDATE_INTERVAL} seconds")
    print("\n📱 Press Ctrl+C to stop")
    print("=" * 60)
    
    # Run Flask app
    app.run(
        host='0.0.0.0',
        port=5000,
        debug=False,  # Set to False for production
        use_reloader=False
    )