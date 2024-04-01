from flask import Flask, jsonify, request, render_template
import pymysql.cursors
from flask_swagger_ui import get_swaggerui_blueprint

debug = True
app = Flask(__name__)

api_spec = {
    "openapi": "3.0.0",
    "info": {
        "title": "Weather API",
        "description": "A simple Flask API",
        "version": "1.0"
    },
    "paths": {
    "/api/weather": {
        "get": {
            "summary": "Get data",
            "parameters": [
                {
                    "name": "date",
                    "in": "query",
                    "description": "Date of the weather record",
                    "required": False,
                    "schema": {
                        "type": "string"
                    }
                },
                {
                    "name": "station_id",
                    "in": "query",
                    "description": "ID of the weather station",
                    "required": False,
                    "schema": {
                        "type": "string"
                    }
                },
                {
                    "name": "number of records",
                    "in": "query",
                    "description": "enteries is the n value in integer to extract those many records",
                    "required": False,
                    "schema": {
                        "type": "integer"
                    }
                }
            ],
            "responses": {
                "200": {
                    "description": "Successful response",
                    "content": {
                        "application/json": {
                            "example": {
                                "data": "Some data"
                            }
                        }
                    }
                }
            }
        }
    },
    "/api/weather/stats": {
        "get": {
            "summary": "Get stats",
            "parameters": [
                {
                    "name": "date",
                    "in": "query",
                    "description": "Date for weather statistics",
                    "required": False,
                    "schema": {
                        "type": "string"
                    }
                },
                {
                    "name": "station_id",
                    "in": "query",
                    "description": "ID of the weather station",
                    "required": False,
                    "schema": {
                        "type": "string"
                    }
                }
            ],
            "responses": {
                "200": {
                    "description": "Successful response",
                    "content": {
                        "application/json": {
                            "example": {
                                "data": "Some data"
                            }
                        }
                    }
                }
            }
        }
    }
}

}
# Route for serving the OpenAPI/Swagger JSON file
@app.route('/static/swagger.json')
def swagger_json():
    return jsonify(api_spec)



SWAGGER_URL = '/api/docs'
API_URL = '/static/swagger.json'
swaggerui_blueprint = get_swaggerui_blueprint( SWAGGER_URL, API_URL, config={ 'app_name': "__main__" } )
app.register_blueprint(swaggerui_blueprint, url_prefix=SWAGGER_URL)
# Function to establish connection with MySQL database
def get_db_connection():
    connection = pymysql.connect(host='localhost',
                                 user='root',
                                 password='12345',
                                 db='weatherdb2',
                                 charset='utf8mb4',
                                 cursorclass=pymysql.cursors.DictCursor)
    return connection

# Endpoint to fetch weather records
@app.route('/api/weather', methods=["GET", "POST"])
def get_weather_records():
    connection = get_db_connection()
    stations  = []
    try:
        with connection.cursor() as cursor:
            sql = "SELECT distinct(record_fid) FROM weather_records"
            cursor.execute(sql)
            for row in cursor.fetchall():
                stations.append(row['record_fid'])
    except:
        stations  = []
    if request.method == "POST":
        station_id = request.form.get("station_id")
        date = request.form.get("date")
        entries = request.form.get("entries")
        if debug:
            print(station_id)
            print(date)
            print(entries)
        try:
            with connection.cursor() as cursor:
                # Building SQL query based on provided filters
                sql = "SELECT * FROM weather_records WHERE 1=1"
                if date:
                    sql += f" AND record_date = '{date}'"
                if station_id:
                    sql += f" AND record_fid = '{station_id}'"
                if entries:
                    sql += f" ORDER BY record_date LIMIT {int(entries)}"
                else:
                    sql += f" ORDER BY record_date LIMIT 10"

                print(sql)
                cursor.execute(sql)
                records = cursor.fetchall()

        finally:
            connection.close()
        return jsonify(records)
    else:
        return render_template("getweather.html",stations=stations)

# Endpoint to fetch weather statistics
@app.route('/api/weather/stats', methods=["GET","POST"])
def get_weather_stats():
    connection = get_db_connection()
    stations  = []
    try:
        with connection.cursor() as cursor:
            sql = "SELECT distinct(record_fid) FROM weather_records"
            cursor.execute(sql)
            for row in cursor.fetchall():
                stations.append(row['record_fid'])
    except:
        stations  = []
    if request.method == "POST":
        station_id = request.form.get("station_id")
        date = request.form.get("date")
        if debug:
            print(station_id)
            print(date)
            year = date.split("-")[0]
            print(year)
        try:
            with connection.cursor() as cursor:
                # Building SQL query based on provided filters
                sql = "SELECT * FROM station_results WHERE 1=1"
                if year:
                    sql += f" AND result_year = {year}"
                if station_id:
                    sql += f" AND result_fid = '{station_id}'"
                print(sql)
                cursor.execute(sql)
                stats = cursor.fetchall()
        finally:
            connection.close()

        return jsonify(stats)
    else:
        return render_template("getstats.html",stations=stations)

if __name__ == '__main__':
    app.run()
