
from math import cos 
from datetime import datetime

points = []
friends = {}


def enqueue_point(userId, lon, lat, timestamp):

    # First we convert the the coordinates into meters
    # for ease of calculations

    lon_meters = lon * (40075 * 1000) * (cos(lat) / 360)
    lat_meters = lat * (111.32 * 1000)

    # Now add them to elastic search
    event = {}
    event['userId'] = userId
    event['lon'] = lon_meters
    event['lat'] = lat_meters
    event['timestamp'] = timestamp

    points.append(event)

def get_points(userId):
    points_found = []
    for event in points:
        if event['userId'] != userId:
            continue
        diff = event['timestamp'] - datetime.now() / 3600
        if diff <= 24:
            points.append(event)
    return points_found



def get_friends(userId, point, valid_axis_modifer):
    friend_points = []

    # 1) Figure out the range of points that are 
    # garanteed in range by using the 50m radius 
    # circle from the point to find the 
    # dimensions of the inscribed square. 
    # This allows us to quickly evaluate ~64%
    # of possible intersection coordinates since 
    # the inscribed square is covers ~64% of the 
    # circle of possible insections O(1)

    valid_upper_lon = point.lon + valid_axis_modifer
    valid_lower_lon = point.lon - valid_axis_modifer
    valid_upper_lat = point.lat + valid_axis_modifer
    valid_lower_lat = point.lat - valid_axis_modifer

    friends_list = friends[userId]
    for point in points:
        if point['userId'] not in friends_list:
            continue
        if valid_lower_lat <= point.lat and point.lat <= valid_upper_lat:
            if valid_lower_lon <= point.lon and point.lon <= valid_upper_lon:
                friend_points.append(point['userId'])

    return friend_points

def get_possible_intersections(userId, point, valid_axis_modifer):
    friend_points = []

    valid_upper_lon = point.lon + 50
    valid_lower_lon = point.lon - 50
    valid_upper_lat = point.lat + 50
    valid_lower_lat = point.lat - 50

    friends_list = friends[userId]
    for point in points:
        if point['userId'] not in friends_list:
            continue
        if valid_lower_lat <= point.lat and point.lat <= valid_upper_lat:
            if valid_lower_lon <= point.lon and point.lon <= valid_upper_lon:
                friend_points.append(point)

    return friend_points




def subscribe_to_input():
    # subscribe to Kafka stream
    # this function won't actually get called
    while True:
         # Periodically check for new points
         # and add them

        new_points = []
        for point in new_points:
            enqueue_point(point.userId, point.lon, point.lat, point.timestamp)

def initialize_relationships():
    friends[1] = [2,7,10]
    friends[2] = [1,5]
    friends[3] = [8,9]
    friends[4] = [5,9]
    friends[5] = [2,4]
    friends[6] = [7]
    friends[7] = [1,6,10]
    friends[8] = [3,10]
    friends[9] = [3,4]
    friends[10] = [1,7,8]


