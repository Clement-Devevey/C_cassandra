#include <cassandra.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
struct datas 
{
  cass_int8_t id;
  cass_uint32_t date;
  cass_int64_t time;
  cass_float_t humidite;
  cass_double_t latitude;
  cass_double_t longitude;
  cass_float_t temperature;
};

typedef struct datas datas;

void print_datas(datas* datas)
{
	    time_t time = (time_t)cass_date_time_to_epoch(datas->date, datas->time);
	//%.7f print a float to a precision of 7 decimal places
	printf("id : %i\nDate and time : %shumidite : %.2f\nlatitude :%.7f\nlongitude :%.7f\ntemperature :%.2f\n", datas->id, asctime(gmtime(&time)),datas->humidite, datas->latitude, datas->longitude, datas->temperature);
}

void print_error(CassFuture* future) 
{
  const char* message;
  size_t message_length;
  cass_future_error_message(future, &message, &message_length);
  fprintf(stderr, "Error: %.*s\n", (int)message_length, message);
}


CassError select_from_basic(CassSession* session, datas* datas) 
{
	printf("\nExecuting query : SELECT * FROM station_meteo.datas;");
	CassError rc = CASS_OK;
	CassStatement* statement = NULL;
	CassFuture* future = NULL;
	const char* query = "SELECT * FROM station_meteo.datas;";

	statement = cass_statement_new(query, 0);

	future = cass_session_execute(session, statement);
	cass_future_wait(future);

	rc = cass_future_error_code(future);

	if (rc != CASS_OK) 
	{
		print_error(future);
	} 

	else 
	{
		const CassResult* result = cass_future_get_result(future);
		CassIterator* iterator = cass_iterator_from_result(result);
		int i = 0;
		while (cass_iterator_next(iterator)) 
		{
			++i;
			printf("\nRow n°%i\n", i);
			const CassRow* row = cass_iterator_get_row(iterator);
			cass_value_get_int8(cass_row_get_column(row, 0), &datas->id);
			cass_value_get_uint32(cass_row_get_column(row, 1), &datas->date);
			cass_value_get_int64(cass_row_get_column(row, 2), &datas->time);
			cass_value_get_float(cass_row_get_column(row, 3), &datas->humidite);
			cass_value_get_double(cass_row_get_column(row,4), &datas->latitude);
			cass_value_get_double(cass_row_get_column(row, 5), &datas->longitude);
			cass_value_get_float(cass_row_get_column(row, 6), &datas->temperature);
			print_datas(datas);
		}

		cass_result_free(result);
		cass_iterator_free(iterator);
	}

	cass_future_free(future);
	cass_statement_free(statement);

	return rc;
}

CassError select_from_custom(CassSession* session, datas* datas) {
	CassError rc = CASS_OK;
	CassStatement* statement = NULL;
	CassFuture* future = NULL;
	const char* query = " SELECT * FROM station_meteo.datas WHERE id = 1 and date <= '2021-11-30' and date >= '2021-11-1' and time <= '10:00:00.000' allow filtering;";

	statement = cass_statement_new(query, 0);

	future = cass_session_execute(session, statement);
	cass_future_wait(future);

	rc = cass_future_error_code(future);
	if (rc != CASS_OK) 
	{
		print_error(future);
	} 
	else 
	{
		const CassResult* result = cass_future_get_result(future);
		CassIterator* iterator = cass_iterator_from_result(result);

		int i = 0;
		if (cass_iterator_next(iterator)) 
		{
			++i;
			printf("\nMatching row n°%i\n", i);
			const CassRow* row = cass_iterator_get_row(iterator);
			cass_value_get_int8(cass_row_get_column(row, 0), &datas->id);
			cass_value_get_uint32(cass_row_get_column(row, 1), &datas->date);
			cass_value_get_int64(cass_row_get_column(row, 2), &datas->time);
			cass_value_get_float(cass_row_get_column(row, 3), &datas->humidite);
			cass_value_get_double(cass_row_get_column(row,4), &datas->latitude);
			cass_value_get_double(cass_row_get_column(row, 5), &datas->longitude);
			cass_value_get_float(cass_row_get_column(row, 6), &datas->temperature);
			print_datas(datas);
		}

		cass_result_free(result);
		cass_iterator_free(iterator);
  	}

  cass_future_free(future);
  cass_statement_free(statement);

  return rc;
}

void execute_query(CassSession* session, char* query) {
	printf(" executing query :\n%s", query);
	/* Create a statement with zero parameters */
	CassStatement* statement = cass_statement_new(query, 0);

	CassFuture* query_future = cass_session_execute(session, statement);

	/* Statement objects can be freed immediately after being executed */
	cass_statement_free(statement);

	/* This will block until the query has finished */
	CassError rc = cass_future_error_code(query_future);
	if(rc != CASS_OK) printf("\nError: %s\n", cass_error_desc(rc));
	//
	else printf("\nSUCCESSFULLY DONE\n");
	cass_future_free(query_future);
}      



int main() {

	/* Setup and connect to cluster */
	CassFuture* connect_future = NULL;
	CassCluster* cluster = cass_cluster_new();
	CassSession* session = cass_session_new();

	/* Add contact points */
	cass_cluster_set_contact_points(cluster, "127.0.0.1");

	/* Provide the cluster object as configuration to connect the session */
	connect_future = cass_session_connect(session, cluster);

	/* This operation will block until the result is ready */
	CassError rc = cass_future_error_code(connect_future);

	if (rc != CASS_OK) {
	/* Display connection error message */
		const char* message;
		size_t message_length;
		cass_future_error_message(connect_future, &message, &message_length);
		fprintf(stderr, "Connect error: '%.*s'\n", (int)message_length, message);
	}

	/* end of setup */

	/* QUERY */
	printf("Creating keyspace -");
	execute_query(session, "CREATE KEYSPACE IF NOT EXISTS station_meteo WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };");

	printf("\nCreating TABLE -");
	execute_query(session, "CREATE TABLE IF NOT EXISTS station_meteo.datas (\
 id tinyint,\
 longitude double,\
 latitude double,\
 date date,\
 time time,\
 temperature float,\
 humidite float,\
 PRIMARY KEY (id, date, time ))\
 WITH CLUSTERING ORDER BY (date ASC, time ASC);");

//tinyint : 8 bits -> up to 255 stations
// longitude and latitude : double 64 bits because we need a lot of scale. ie the maximum number of decimal places
// we use DATE and TIME instead of timestamp because we can't process TIMESTAMP with this C driver
// TIME represents the nanoseconds since midnight
// temperature and humidite only need a 32 bits float.

	printf("\nCreating index on date -");
	execute_query(session, "CREATE INDEX IF NOT EXISTS index_on_date ON station_meteo.datas ( date );");

	printf("\nCreating index on time -");
	execute_query(session, "CREATE INDEX IF NOT EXISTS index_on_time ON station_meteo.datas ( time );");


	printf("\nInserting some measures\n1.");
	execute_query(session, "INSERT INTO station_meteo.datas (id, latitude, longitude, date, time, temperature, humidite)\
 VALUES (1,45.4521584, 4.3878329, '2021-10-30', '09:12:35.156', 6.51, 80) IF NOT EXISTS;");

	printf("\n2.");
	execute_query(session, "INSERT INTO station_meteo.datas (id, latitude, longitude, date, time, temperature, humidite)\
 VALUES (1,43.619776, 7.0477082, '2021-11-30', '09:44:15.651', 6.51, 90) IF NOT EXISTS;");

	printf("\n3.");
	execute_query(session, "INSERT INTO station_meteo.datas (id, latitude, longitude, date, time, temperature, humidite)\
 VALUES (1,54.5412485, 4.2587413, '2021-11-19', '10:00:00.001', 15.1, 10) IF NOT EXISTS;");

	printf("\n4.");
	execute_query(session, "INSERT INTO station_meteo.datas (id, latitude, longitude, date, time, temperature, humidite)\
 VALUES (2,47.776321, 6.4088928, '2021-11-29', '09:44:15.651', 6.51, 70) IF NOT EXISTS;");

	datas myData;
	select_from_basic(session, &myData);

	printf("\nLet's find all rows that are from ID = 1 and 2021-11-1 <= DATE <= 2021-11-30 and 09:00:00.000 <= TIME <= 10:00:00.000\n"); 
	select_from_custom(session, &myData);
	cass_future_free(connect_future);
	cass_session_free(session);
	cass_cluster_free(cluster);

	return 0;
}
