from datetime import datetime
from math import sqrt
from flask import Flask, request, jsonify
from backend import enqueue_point, points, friends, initialize_relationships, get_points, get_friend_points, get_friends
app = Flask(__name__)

# This flask file is going to hold the endpoints 
# To scale it appropriately I'd containerize 
# this code into a Docker setup that would manage
# multiple instances on this python code as needed
@app.route('/', methods = ['GET', 'POST'])
def post_locations_get_intersections():
    if request.method == 'POST':
        timestamp = datetime.now()
        json = request.get_json()
        userId = json['userId']
        lon = json['lon']
        lat = json['lat']
        add_user_location(userId, lon, lat, timestamp)
        resp = jsonify(userId=userId, longitude=lon, latitude=lat)
        resp.status_code = 202
        return resp 
    elif request.method == 'GET':
        userId = request.args.get('userId')
        intersections = get_intersections(userId)
        resp = jsonify(items=intersections)
        resp.status_code = 200
        return resp 

def add_user_location(userId, lon, lat, timestamp):
    # This is where I use a streaming 
    # service such as Kafka to send a msg with
    # a location update at the current time
    enqueue_point(userId, lon, lat, timestamp)
    

def get_intersections(userId):
    # This is where I would use elasticsearch
    # to search up all coordinates made by 
    # the user in the last 24 hours 

    user_points = get_points(userId) #elasticsearch result
    intersections = []
    last_intersections = {}

    # For each point found O(n)
    square_side_length = 50 * sqrt(2)
    valid_axis_modifer = square_side_length / 2

    for point in user_points:

        # 2) Using elasticsearch range query, 
        # search for friends that have points
        # within the square we found above as long 
        # as they were  made +/- 1 hour from the 
        # timestamp and it's been over an hour since 
        # their last intersection. This gives us a
        # easy search to cover 64% of points without
        # excessive calculations. To do this we would
        # send a list - should be its own function

        confirmed_friends = get_friends(userId, point, valid_axis_modifer) #elastic search result
        for friend in confirmed_friends:
            new_intersection = False

            # 2a) Add/Update all found userIds (key) and 
            # this point's timestamp + 1hr (value) 
            # into a dictionary to track when to
            # next add intersection between these 
            # two friends
            if friend.id in last_intersections:
                last_intersect = last_intersections[friend.id]
                hours_diff = abs(point.timestamp - last_intersect.timestamp) / 3600
                if hours_diff > 1:
                    new_intersection = True
                    if point.timestamp > last_intersect.timestamp:
                        last_intersections[friend.id].timestamp = point.timestamp
            else:
                new_intersection = True
                last_intersections[friend.id] = point.timestamp

            if new_intersection:
                intersection = {}
                intersection['from'] = userId
                intersection['to'] = friend.id
                intersection['time'] = point.timestamp
                intersections.append(intersection)

        # 3) Using elasticsearch range query, 
        # Search for all points made by friends 
        # between an hour before and after the 
        # timestamp as long both lon and lat 
        # are <= +/-50m away,  the point falls
        # outside of the square, and ignore 
        # friends found in the previous search or 
        # have an intersection within the last hour
        # O(k) - should be its own function

        possible_intersections = get_friend_points(userId, point, valid_axis_modifer)  #elasticsearch results

        for intersect in possible_intersections:
            new_intersection = False
            valid_intersection = False

            # 4) Check to see if the point is an 
            # intersection using pythagros theorem to 
            # determine if 
            # lonDelta^2 + latDelta^2 <= 2500 (or 50^2)
            # if it is then the coordinate is a match
            a_squared = pow(abs(intersect.point.lon - point.lon),2)
            b_squared = pow(abs(intersect.point.lat - point.lat),2)
            c_squared = pow(50, 2)

            if a_squared + b_squared <= c_squared:
                valid_intersection = True



            # 4a) Add/Update all found userIds (key) and 
            # this point's timestamp + 1hr (value) 
            # into a dictionary to track when to
            # next add intersection between these 
            # two friends
            if intersect.id in last_intersections:
                last_intersect = last_intersections[intersect.id]
                hours_diff = abs(point.timestamp - last_intersect.timestamp) / 3600
                if hours_diff > 1:
                    new_intersection = True
                    if point.timestamp > last_intersect.timestamp:
                        last_intersections[intersect.id].timestamp = point.timestamp
            else:
                new_intersection = True
                last_intersections[intersect.id] = point.timestamp

            if new_intersection and valid_intersection:
                intersection = {}
                intersection['from'] = userId
                intersection['to'] = friend.id
                intersection['time'] = point.timestamp
                intersections.append(intersection)
    return intersections

if __name__ == '__main__':
    initialize_relationships()
    app.run(host='0.0.0.0', port=3000)
