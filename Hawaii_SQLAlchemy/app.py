#Defining our dependencies
import numpy as np
from flask import Flask, jsonify
import sqlalchemy
from sqlalchemy import engine,exc
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from sqlalchemy import asc, desc
import datetime as dt
from datetime import datetime

#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///C:/Users/empha/Desktop/Data_Analytics/Semana_10/sqlalchemy-challenge/Resources/hawaii.sqlite")
# reflect an existing database into a new model
Base = automap_base()
# reflect the tables
Base.prepare(engine, reflect=True)

# Save reference to the table
Station = Base.classes.station
Measurement = Base.classes.measurement


#################################################
# Flask Setup
#################################################
app = Flask(__name__)

#################################################
# Flask Routes
#################################################
@app.route("/")
def home():
    print("Server received request for 'Home' page...")
    return "Welcome to my sqlalchemy challenge. The following are the available links:<br/>\
            Date and Precipitation Dictionary-> http://127.0.0.1:5000/api/v1.0/precipitation<br/>\
            JSON List of Stations-> http://127.0.0.1:5000/api/v1.0/stations<br/>\
            JSON List of Temperatures-> http://127.0.0.1:5000/api/v1.0/tobs<br/>\
            For a given date or range of dates, temperature max, min, and avg -> http://127.0.0.1:5000/api/v1.0/start and http://127.0.0.1:5000/api/v1.0/start/end"

@app.route("/api/v1.0/precipitation")
def dictionary():
    # Create our session (link) from Python to the DB
    session = Session(engine)
    #Create dictionary based on date and prcp
    data_query = session.query(Measurement.date,Measurement.prcp).all()
    #Close session
    session.close()
    dictionary_query = dict(data_query)
    print("Server received request for 'Dictionary' page...")
    return jsonify(dictionary_query)

@app.route("/api/v1.0/stations")
def stations():
    session = Session(engine)
    moact_stations = session.query(Measurement.station).group_by(Measurement.station).all()
    session.close()
    print("Server received request for 'Stations' page...")
    all_stations  = list(np.ravel(moact_stations))
    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():
    session = Session(engine)
    #Extracting the most active session
    moact_stations = session.query(Measurement.station,func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc())
    moact_one = moact_stations[0][0]

    #Getting the earliest day
    last_date = session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    last_date_dt = datetime.strptime(last_date.date, '%Y-%m-%d')
    year_before_last_date = last_date_dt.date() - dt.timedelta(days=365)

    #Dates extracted
    temperatures = session.query(Measurement.tobs).filter(Measurement.date >= year_before_last_date).filter(Measurement.station == moact_one).order_by(Measurement.date).all()
    session.close()
    print("Server received request for 'Tobs' page...")
    all_temps  = list(np.ravel(temperatures))
    print(len(all_temps))
    return jsonify(all_temps)

@app.route("/api/v1.0/<start>")
def start(start):
    session = Session(engine)
    get_lha = session.query(func.min(Measurement.tobs),func.max(Measurement.tobs),func.avg(Measurement.tobs)).filter(Measurement.date >= start)
    session.close()
    print("Server received request for 'Start' page...")
    container_list = []
    if get_lha.first()[0] is not None:
        #Order: low, high, average
        container_list.append(get_lha[0][0])
        container_list.append(get_lha[0][1])
        container_list.append(get_lha[0][2])
        return jsonify(container_list)
    return jsonify({"error": f"Values with date:  {start} were not found"}),404

@app.route("/api/v1.0/<start>/<end>")
def start_end(start,end):
    session = Session(engine)
    get_lha = session.query(func.min(Measurement.tobs),func.max(Measurement.tobs),func.avg(Measurement.tobs)).filter(Measurement.date >= start).filter(Measurement.date <= end)
    session.close()
    container_list = []
    if get_lha.first()[0] is not None:
        #Order: low, high, average
        container_list.append(get_lha[0][0])
        container_list.append(get_lha[0][1])
        container_list.append(get_lha[0][2])
        return jsonify(container_list)
    return jsonify({"error": f"Values with date:  {start} and date: {end} were not found"}),404



if __name__ == "__main__":
    app.run(debug=True)