# Main Components and Functionalities

## Models

The application contains four main models:

- **Store**: Stores the basic details about the stores, like ID and timezone.
- **StoreStatus**: Stores the status of the store at a particular time.
- **BusinessHours**: Stores the business hours for each store.
- **Report**: Stores the generated reports.

## Importing Data

The `import_data` endpoint accepts POST requests. It's used to import data from three CSV files:

- `store.csv` - The CSV that has data about the timezone for the stores.
- `store_status.csv` - The CSV that has data about whether the store was active or not.  
- `business_hours.csv` - The CSV that stores the business hours of all the stores.

The data from these files populates the `Store`, `StoreStatus`, and `BusinessHours` tables in the SQLite database.

## Generating Reports

The `trigger_report` endpoint triggers the generation of the report. The `generate_report` function executes in a new thread and generates a report for each store, which includes the uptime and downtime for the last hour, day, and week. The report generation process considers only the business hours for calculations. Once the report is generated, it is stored in the `Report` table with a unique report ID.

The `calculate_uptime_and_downtime` function is used to calculate the uptime and downtime for a store within a certain time period. To calculate the uptime and downtime, it first sorts the statues of a particular store by the timestamp, then it adds the duration from the last status to the current status in uptime if the current store status is active, otherwise it adds that duration in downtime.

## Fetching Reports

The `get_report` endpoint accepts a report ID and returns the status of the report. If the report generation is complete, it returns the generated report as a CSV file. If the report is still being generated, it returns the status "Running".

## Database Connection

The `engine` and `Session` objects are used to connect to the SQLite database and manage database sessions.

## Notes

- All timestamps are handled in UTC to avoid inconsistencies due to timezones.
- If any data is missing, the system uses default values (e.g., 'America/Chicago' timezone for a store).
- The application is designed for a real-time scenario where data gets updated every hour. Reports can be generated as needed based on the updated data.
- The system is designed to handle complex scenarios involving different time zones, missing data, and interpolation of data based on the status of the stores.
