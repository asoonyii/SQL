#SQL Alchemy Data Analytics 
#Step 2- Climate App
#Import dependencies
import pandas as pd
import numpy as np
import datetime as dt
import sqlalchemy
from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from flask import Flask, jsonify

# Set up Flask
app = Flask(__name__)
# Create the connection engine to the sqlite database
engine = create_engine("sqlite:///11-SQLAlchemy_Homework_Instructions_Resources_hawaii.sqlite")
# Establish Base for which classes will be constructed 
Base = automap_base()
# Use the Base class to reflect the database tables
Base.prepare(engine, reflect=True)
# Assign the classes to a their respective variables
Measurement = Base.classes.measurement
Station = Base.classes.station
session = Session(bind=engine)

@app.route("/")
def home():
    return (
        f"<h1>Climate Analysis</h1><br/><br/>"
        f"Available Routes:<br/>"
        """<a href="/api/v1.0/precipitation">/api/v1.0/precipitation Returns Precipitation information </a><br/>"""
        """<a href="/api/v1.0/stations">/api/v1.0/stations List of stations</a><br/>"""
        """<a href="/api/v1.0/tobs">/api/v1.0/tobs Temperature observations</a><br/>"""
        """<a href="/api/v1.0/2017-01-01/2017-12-31">/api/v1.0/start_date/end_date Temperature statistics </a><br/>""")

@app.route("/api/v1.0/precipitation")
def prcpp():
    # Calculate the date 1 year ago from today
    year_ago = dt.date(2017,8,23) - dt.timedelta(days=365)
    
    # Query database for stations
    prcp = session.query(Measurement.date, Measurement.prcp)\
            .filter(Measurement.date >= year_ago)\
            .order_by(Measurement.date).all()
    
    # Convert object to a list
    prcp_list={}
    for pp in prcp:
        prcp_list[item[0]]=item[1]

    return (jsonify(prcp_list))

@app.route("/api/v1.0/stations")
def stations():
    # Query database for stations
    stations = session.query(Station.station).all()
    
    # Convert object to a list
    stations_list=[]
    for ss in stations:
        for ss_item in sublist:
            stations_list.append(ss_item)
    
    # Return jsonified list
    return (jsonify(stations_list))

@app.route("/api/v1.0/tobs")
def tobs():
    # Calulate the date 1 year ago from today
    year_ago = dt.date.today() - dt.timedelta(days=365)
    
    # Query database for stations
    tobs = session.query(Measurement.date, Measurement.tobs)\
            .filter(Measurement.date >= year_ago)\
            .order_by(Measurement.date).all()
    
    # Convert object to a list
    tobs_list=[]
    for tt in tobs:
        for tt_item in sublist:
            tobs_list.append(tt_item)
    
    return (jsonify(tobs_list))

@app.route("/api/v1.0/<start_date>")
@app.route("/api/v1.0/<start_date>/<end_date>")
def temp_dtt(start_date, end_date=0):
    if end_date == 0:
        end_date = dt.date.today()
    
    # Query database for tobs between start and end date
    tobs = session.query(Measurement.tobs)\
            .filter(Measurement.date >= start_date)\
            .filter(Measurement.date <= end_date).all()
   
    tobs_df = pd.DataFrame(tobs, columns=['tobs'])
    
    tobs_list = []
    tobs_list.append(np.asscalar(np.int16(tobs_df['tobs'].min())))
    tobs_list.append(np.asscalar(np.int16(tobs_df['tobs'].mean())))
    tobs_list.append(np.asscalar(np.int16(tobs_df['tobs'].max())))
    
    return (jsonify(tobs_list))

if __name__ == "__main__":
    app.run(debug=True)