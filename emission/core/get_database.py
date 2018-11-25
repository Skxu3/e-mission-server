from __future__ import unicode_literals
from __future__ import print_function
from __future__ import division
from __future__ import absolute_import
from future import standard_library
standard_library.install_aliases()
from builtins import *
from pymongo import MongoClient
import pymongo
import os
import json

import emission.core.sql_manager as sql_manager

try:
    config_file = open('conf/storage/db.conf')
except:
    print("storage not configured, falling back to sample, default configuration")
    config_file = open('conf/storage/db.conf.sample')
config_data = json.load(config_file)
url = config_data["timeseries"]["url"]

print("Connecting to database URL "+url)
_current_db = MongoClient(url).Stage_database

sql = sql_manager.SQLManager()

def _get_current_db():
    return _current_db

def get_profile_db():
    # current_db=MongoClient().Stage_database
    # Profiles=_get_current_db().Stage_Profiles
    # return Profiles 
    sql.set_table("Stage_Profiles")
    return sql 

def get_uuid_db():
    # current_db=MongoClient().Stage_database
    # UUIDs = _get_current_db().Stage_uuids
    # return UUIDs
    sql.set_table("Stage_uuids")
    return sql

def get_usercache_db():
    #current_db = MongoClient().Stage_database
    UserCache = _get_current_db().Stage_usercache
    UserCache.create_index([("user_id", pymongo.ASCENDING),
                            ("metadata.type", pymongo.ASCENDING),
                            ("metadata.write_ts", pymongo.ASCENDING),
                            ("metadata.key", pymongo.ASCENDING)])
    UserCache.create_index([("metadata.write_ts", pymongo.DESCENDING)])
    return UserCache

def get_timeseries_db():
    #current_db = MongoClient().Stage_database
    # TimeSeries = _get_current_db().Stage_timeseries
    # TimeSeries.create_index([("user_id", pymongo.HASHED)])
    # TimeSeries.create_index([("metadata.key", pymongo.HASHED)])
    # TimeSeries.create_index([("metadata.write_ts", pymongo.DESCENDING)])
    # TimeSeries.create_index([("data.ts", pymongo.DESCENDING)], sparse=True)

    # TimeSeries.create_index([("data.loc", pymongo.GEOSPHERE)], sparse=True)

    # return TimeSeries
    print('get timeseries db')
    # sql.set_table("Stage_uuids")
    return sql
def get_timeseries_location_db():
    sql.set_table("Timeseries_location")
    return sql
def get_timeseries_motionactivity_db():
    sql.set_table("Timeseries_motionactivity")
    return sql
def get_timeseries_transition_db():
    sql.set_table("Timeseries_transition")
    return sql
def get_timeseries_statsevent_db():
    sql.set_table("Timeseries_statsevent")
    return sql
def get_pipeline_state_db():
    #current_db = MongoClient().Stage_database
    # PipelineState = _get_current_db().Stage_pipeline_state
    # return PipelineState
    sql.set_table("Stage_pipeline_state")
    return sql

def get_client_db():
    # current_db=MongoClient().Stage_database
    Clients = _get_current_db().Stage_clients
    return Clients

def get_section_db():
    # current_db=MongoClient('localhost').Stage_database
    Sections= _get_current_db().Stage_Sections
    return Sections

def get_habitica_db():
    # #current_db = MongoClient('localhost').Stage_database
    # HabiticaAuth= _get_current_db().Stage_user_habitica_access
    # return HabiticaAuth
    print('skip habitica db')
    return sql

def get_analysis_timeseries_db():
    """
    " Stores the results of the analysis performed on the raw timeseries
    """
    #current_db = MongoClient().Stage_database
    # AnalysisTimeSeries = _get_current_db().Stage_analysis_timeseries
    # AnalysisTimeSeries.create_index([("user_id", pymongo.HASHED)])
    # _create_analysis_result_indices(AnalysisTimeSeries)
    # return AnalysisTimeSeries
    print("replaced")
    return sql

def get_analysis_timeseries_place_db():
    sql.set_table("Analysis_timeseries_place")
    return sql
def get_analysis_timeseries_trip_db():
    sql.set_table("Analysis_timeseries_trip")
    return sql
def get_analysis_timeseries_section_db():
    sql.set_table("Analysis_timeseries_section")
    return sql
def get_analysis_timeseries_stop_db():
    sql.set_table("Analysis_timeseries_stop")
    return sql
def get_analysis_timeseries_smoothedresults_db():
    sql.set_table("Analysis_timeseries_smoothedresults")
    return sql

def get_inference_timeseries_prediction_db():
    sql.set_table("Inference_timeseries_prediction")
    return sql

def _create_analysis_result_indices(tscoll):
    tscoll.create_index([("metadata.key", pymongo.HASHED)])

    # trips and sections
    tscoll.create_index([("data.start_ts", pymongo.DESCENDING)], sparse=True)
    tscoll.create_index([("data.end_ts", pymongo.DESCENDING)], sparse=True)
    tscoll.create_index([("data.start_loc", pymongo.GEOSPHERE)], sparse=True)
    tscoll.create_index([("data.end_loc", pymongo.GEOSPHERE)], sparse=True)
    _create_local_dt_indices(tscoll, "data.start_local_dt")
    _create_local_dt_indices(tscoll, "data.end_local_dt")

    # places and stops
    tscoll.create_index([("data.enter_ts", pymongo.DESCENDING)], sparse=True)
    tscoll.create_index([("data.exit_ts", pymongo.DESCENDING)], sparse=True)
    _create_local_dt_indices(tscoll, "data.enter_local_dt")
    _create_local_dt_indices(tscoll, "data.exit_local_dt")
    tscoll.create_index([("data.location", pymongo.GEOSPHERE)], sparse=True)
    tscoll.create_index([("data.duration", pymongo.DESCENDING)], sparse=True)
    tscoll.create_index([("data.mode", pymongo.HASHED)], sparse=True)
    tscoll.create_index([("data.section", pymongo.HASHED)], sparse=True)

    # recreated location
    tscoll.create_index([("data.ts", pymongo.DESCENDING)], sparse=True)
    tscoll.create_index([("data.loc", pymongo.GEOSPHERE)], sparse=True)
    _create_local_dt_indices(tscoll, "data.local_dt") # recreated location
    return tscoll

def _create_local_dt_indices(time_series, key_prefix):
    """
    local_dt is an embedded document, but we will query it using the individual fields
    """
    time_series.create_index([("%s.year" % key_prefix, pymongo.DESCENDING)], sparse=True)
    time_series.create_index([("%s.month" % key_prefix, pymongo.DESCENDING)], sparse=True)
    time_series.create_index([("%s.day" % key_prefix, pymongo.DESCENDING)], sparse=True)
    time_series.create_index([("%s.hour" % key_prefix, pymongo.DESCENDING)], sparse=True)
    time_series.create_index([("%s.minute" % key_prefix, pymongo.DESCENDING)], sparse=True)
    time_series.create_index([("%s.second" % key_prefix, pymongo.DESCENDING)], sparse=True)
    time_series.create_index([("%s.weekday" % key_prefix, pymongo.DESCENDING)], sparse=True)


# Static utility method to save entries to a mongodb collection.  Single
# drop-in replacement for collection.save() now that it is deprecated in 
# pymongo 3.0. 
# https://github.com/e-mission/e-mission-server/issues/533#issuecomment-349430623
def save(db, entry):
    # if '_id' in entry:
    #     db.replace_one({'_id': entry['_id']}, entry, upsert=True)
    # else:
    #     db.replace_one({}, entry, upsert=True)
    entry_dic = {}    
    for prop in entry.props:
        if prop in entry:
            entry_dic[prop] = entry[prop]

    #db.save(entry)
    db.insert_or_update(entry_dic, entry_dic)
