
from flask import Flask, jsonify, send_file, make_response
from flask_sqlalchemy import SQLAlchemy
from datetime import timedelta, timezone, time, datetime
from threading import Thread
import uuid
import pandas as pd
import pytz
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from io import BytesIO


app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///test.db'
db = SQLAlchemy(app)


# DataBase Models
class Report(db.Model):
    __tablename__ = 'reports'
    id = db.Column(db.String, primary_key=True)
    data = db.Column(db.LargeBinary)


class Store(db.Model):
    id = db.Column(db.String, primary_key=True)
    timezone = db.Column(db.String, nullable=False, default="America/Chicago")


class StoreStatus(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.String, db.ForeignKey('store.id'), nullable=False)
    timestamp_utc = db.Column(db.DateTime, nullable=False)
    status = db.Column(db.String, nullable=False)


class BusinessHours(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    store_id = db.Column(db.String, db.ForeignKey('store.id'), nullable=False)
    day_of_week = db.Column(db.Integer, nullable=False)
    start_time_local = db.Column(db.Time, nullable=False)
    end_time_local = db.Column(db.Time, nullable=False)


# API endpoint for importing the data from the csv in the database
@app.route("/import_data", methods=["POST"])
def import_data():
    try:

        store_statuses = pd.read_csv('store_status.csv')
        business_hours = pd.read_csv('business_hours.csv')
        stores = pd.read_csv('store.csv')

        # Storing the timezones of the stores
        stores['timezone_str'].fillna('America/Chicago', inplace=True)
        db.session.query(Store).delete()
        db.session.query(StoreStatus).delete()
        db.session.query(BusinessHours).delete()
        for index, row in stores.iterrows():
            store = Store(id=row['store_id'], timezone=row['timezone_str'])
            db.session.add(store)
        db.session.commit()


        # Storing the store statuses of the stores in local time after converting it from UTC
        store_dict = {store.id: store for store in Store.query.all()}
        for index, row in store_statuses.iterrows():
            timestamp_utc = pd.to_datetime(row['timestamp_utc'])
            if str(row['store_id']) in store_dict:
                store = store_dict[str(row['store_id'])]
                local_tz = pytz.timezone(store.timezone)
            else:
                local_tz=pytz.timezone("America/Chicago")
            if timestamp_utc.tzinfo is None:
                timestamp_utc = timestamp_utc.tz_localize('UTC').tz_convert(local_tz)
            else:
                timestamp_utc = timestamp_utc.tz_convert(local_tz)
            store_status = StoreStatus(store_id=row['store_id'], timestamp_utc=timestamp_utc, status=row['status'])
            db.session.add(store_status)


        # Storing the business hours of the stores
        business_hours.fillna({"start_time_local": "00:00:00", "end_time_local": "23:59:59"}, inplace=True)
        for index, row in business_hours.iterrows():
            store_id = row['store_id']
            day_of_week = row['day']
            start_time_str = row['start_time_local']
            end_time_str = row['end_time_local']

            start_time_parts = list(map(int, start_time_str.split(':')))
            end_time_parts = list(map(int, end_time_str.split(':')))

            start_time_local = time(*start_time_parts)
            end_time_local = time(*end_time_parts)

            business_hour = BusinessHours(store_id=store_id, day_of_week=day_of_week,
                                        start_time_local=start_time_local, end_time_local=end_time_local)
            db.session.add(business_hour)

        db.session.commit()

    except Exception as e:
        db.session.rollback()
        return jsonify({"status": "Import Failed", "error": str(e)}), 500

    return jsonify({"status": "Import Successful"})


engine = create_engine('sqlite:///database.db', echo=True)
Session = sessionmaker(bind=engine)

# API endpoint to trigger generation of report
@app.route("/trigger_report", methods=["POST"])
def trigger_report():
    report_id = str(uuid.uuid4())
    generate_report(report_id)
    return jsonify({"report_id": report_id})

# Helper function to generate the report
def generate_report(report_id):
    stores = Store.query.all()
    latest_timestamp = max(status.timestamp_utc for status in StoreStatus.query.all())
    report = {}

    all_statuses = StoreStatus.query.filter(StoreStatus.timestamp_utc >= latest_timestamp - timedelta(weeks=1)).all()
    statuses_dict = {}
    for status in all_statuses:
        if status.store_id not in statuses_dict:
            statuses_dict[status.store_id] = []
        statuses_dict[status.store_id].append(status)

    all_business_hours = BusinessHours.query.all()
    business_hours_dict = {}
    for b_hours in all_business_hours:
        if b_hours.store_id not in business_hours_dict:
            business_hours_dict[b_hours.store_id] = []
        business_hours_dict[b_hours.store_id].append(b_hours)

    for store in stores:
        store_timezone = pytz.timezone(store.timezone)

        latest_timestamp_local = latest_timestamp.astimezone(store_timezone)
        one_hour_ago = latest_timestamp_local - timedelta(hours=1)
        one_day_ago = latest_timestamp_local - timedelta(days=1)
        one_week_ago = latest_timestamp_local - timedelta(weeks=1)

        statuses = statuses_dict.get(store.id, [])
        business_hours = business_hours_dict.get(store.id, [])

        uptime_last_hour, downtime_last_hour = calculate_uptime_and_downtime(statuses, business_hours, one_hour_ago,
                                                                             latest_timestamp_local, store_timezone)
        uptime_last_day, downtime_last_day = calculate_uptime_and_downtime(statuses, business_hours, one_day_ago,
                                                                           latest_timestamp_local, store_timezone)
        uptime_last_week, downtime_last_week = calculate_uptime_and_downtime(statuses, business_hours, one_week_ago,
                                                                             latest_timestamp_local, store_timezone)

        report[store.id] = [uptime_last_hour, uptime_last_day / 60, uptime_last_week / 60, downtime_last_hour,
                            downtime_last_day / 60, downtime_last_week / 60]

    report_df = pd.DataFrame.from_dict(report, orient='index',
                                       columns=['uptime_last_hour', 'uptime_last_day', 'uptime_last_week',
                                                'downtime_last_hour', 'downtime_last_day', 'downtime_last_week'])
    buffer = BytesIO()
    report_df.to_csv(buffer)
    buffer.seek(0)

    report = Report(id=report_id, data=buffer.read())
    db.session.add(report)
    db.session.commit()

# Helper function to calculate the uptime and downtime
def calculate_uptime_and_downtime(statuses, business_hours, start_time, end_time, timezone):
    uptime = timedelta()
    downtime = timedelta()

    local_tz=pytz.timezone(str(timezone))
    if start_time.tzinfo is None or start_time.tzinfo.utcoffset(start_time) is None:
        start_time = local_tz.localize(start_time).astimezone(pytz.utc)
    else:
        start_time = start_time.astimezone(pytz.utc)

    if end_time.tzinfo is None or end_time.tzinfo.utcoffset(end_time) is None:
        end_time = local_tz.localize(end_time).astimezone(pytz.utc)
    else:
        end_time = end_time.astimezone(pytz.utc)

    for status in statuses:
        if status.timestamp_utc.tzinfo is None or status.timestamp_utc.tzinfo.utcoffset(status.timestamp_utc) is None:
            status.timestamp_utc = local_tz.localize(status.timestamp_utc).astimezone(pytz.utc)
        else:
            status.timestamp_utc = status.timestamp_utc.astimezone(pytz.utc)

    statuses_in_period = sorted(
        [status for status in statuses if start_time <= status.timestamp_utc <= end_time],
        key=lambda s: s.timestamp_utc)

    for business_hour in business_hours:
        
        start_date = statuses_in_period[0].timestamp_utc.date() if statuses_in_period else None
        end_date = statuses_in_period[-1].timestamp_utc.date() if statuses_in_period else None

        if start_date and end_date:
            start_time_local = local_tz.localize(datetime.combine(start_date, business_hour.start_time_local)).astimezone(pytz.utc)
            end_time_local = local_tz.localize(datetime.combine(end_date, business_hour.end_time_local)).astimezone(pytz.utc)

            for i in range(len(statuses_in_period) - 1):
                status_start = max(statuses_in_period[i].timestamp_utc, start_time_local)
                status_end = min(statuses_in_period[i + 1].timestamp_utc, end_time_local)

                if status_start < status_end:
                    duration = status_end - status_start
                    if statuses_in_period[i].status == 'active':
                        uptime += duration
                    else:
                        downtime += duration

    uptime_minutes = uptime.total_seconds() / 60
    downtime_minutes = downtime.total_seconds() / 60

    return uptime_minutes, downtime_minutes


# API endpoint to get the report generated
@app.route("/get_report/<report_id>", methods=["GET"])
def get_report(report_id):

    report = db.session.query(Report).get(report_id)
    if report is not None:
        response = make_response(report.data)
        response.headers["Content-Disposition"] = f"attachment; filename={report_id}.csv"
        response.mimetype = 'text/csv'
        return response
    else:
        return jsonify({"status": "Running"})

if __name__ == "__main__":
    with app.app_context():
        db.create_all()
    app.run(debug=True)
