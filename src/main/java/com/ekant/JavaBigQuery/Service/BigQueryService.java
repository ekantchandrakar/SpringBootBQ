package com.ekant.JavaBigQuery.Service;

import com.ekant.JavaBigQuery.model.Flight;
import com.ekant.JavaBigQuery.model.FlightHolder;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.io.ClassPathResource;
import org.springframework.stereotype.Service;

import java.util.*;

@Service
@Slf4j
public class BigQueryService {

    // Replace these placeholders with your actual values
    private static final String PROJECT_ID = "springbootbq";
    private static final String DATASET_ID = "Flights";
    private static final String TABLE_ID = "sfo_jfk_flights";

    public FlightHolder basicQuery() {
        FlightHolder flightHolder = new FlightHolder();
        List<Flight> flights = new ArrayList<>();;

        flightHolder.setFlights(flights);
        flightHolder.setCount(0);
        flightHolder.setStatus("Failed");

        try {
            log.info("basicQuery Started");

            // Load the service account credentials from the classpath
            ClassPathResource resource = new ClassPathResource("springbootbq.json");
            GoogleCredentials credentials = GoogleCredentials.fromStream(resource.getInputStream());

            // Get the connection information we need for Google BQ
            BigQuery bigQuery = BigQueryOptions.newBuilder().setCredentials(credentials).build().getService();

            // Set query to run
            String query = "SELECT * FROM `springbootbq.Flights.sfo_jfk_flights` LIMIT 1000";

            QueryJobConfiguration queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build();

            String jobIdStr = UUID.randomUUID().toString();

            log.info("service jobIdStr: " + jobIdStr);

            // create a job ID so that we can safely retry.
            JobId jobId = JobId.of(jobIdStr);

            Job queryJob = bigQuery.create(JobInfo.newBuilder(queryConfig).setJobId(jobId).build());

            // wait for query to complete
            queryJob = queryJob.waitFor();

            if (queryJob == null) {
                log.error("Job is no longer exist");
                return flightHolder;
            }

            if (queryJob.getStatus().getError() != null) {
                log.error("Job failed");
                return flightHolder;
            }

            // get the result
            TableResult result = queryJob.getQueryResults();

            double total = 0.0d;

            for (FieldValueList row : result.iterateAll()) {
                String origin = row.get("Origin").getStringValue();
                String destination = row.get("Destination").getStringValue();
                String airline = row.get("Airline").getStringValue();
                int stops = row.get("Stops").getNumericValue().intValue();
                double price = row.get("Price").getDoubleValue();

                Flight flight = new Flight(origin, destination, airline, stops, price);

                flights.add(flight);

                log.info("Data origin: " + origin + " destination: " + destination + " airline: " + airline + " stops: " + stops + " price: " + price);
                total += price;
            }

            log.info("Total: " + total);

            flightHolder.setStatus("Success");
            flightHolder.setCount(flights.size());
        } catch (Exception e) {
            log.error("Exception e: " + e.getMessage());
            return flightHolder;
        }

        return flightHolder;
    }

    public String insertDB() {
        try {
            log.info("InsertDB started");

            // Load the service account credentials from the classpath
            ClassPathResource resource = new ClassPathResource("springbootbq.json");
            GoogleCredentials credentials = GoogleCredentials.fromStream(resource.getInputStream());

            // Get the connection information we need for Google BQ
            BigQuery bigQuery = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId(PROJECT_ID).build().getService();

            // Create a dataset reference
            DatasetId datasetId = DatasetId.of(DATASET_ID);

            // Create a table reference
            TableId tableId = TableId.of(String.valueOf(datasetId), TABLE_ID);

            if(bigQuery.getTable(tableId) == null) {
                Schema schema =  Schema.of(
                        Field.of("Origin", LegacySQLTypeName.STRING),
                        Field.of("Destination", LegacySQLTypeName.INTEGER),
                        Field.of("Airline", LegacySQLTypeName.STRING),
                        Field.of("Stops", LegacySQLTypeName.INTEGER),
                        Field.of("Price", LegacySQLTypeName.FLOAT)
                );
                bigQuery.create(TableInfo.of(tableId, StandardTableDefinition.of(schema)));
            }

            // Create rows to insert into the table
            List<InsertAllRequest.RowToInsert> rowsToInsert = new ArrayList<>();

            // Insert the first row
            Map<String, Object> firstRowData = new HashMap<>();
            firstRowData.put("Origin", "RPR");
            firstRowData.put("Destination", "MAA");
            firstRowData.put("Airline", "IND");
            firstRowData.put("Stops", 1);
            firstRowData.put("Price", 250.0);
            rowsToInsert.add(InsertAllRequest.RowToInsert.of(firstRowData));

            // Insert the rows into the table
            InsertAllResponse response = bigQuery.insertAll(InsertAllRequest.newBuilder(tableId).addRow((InsertAllRequest.RowToInsert) rowsToInsert).build());

            if (response.hasErrors()) {
                log.error("Error inserting rows: " + response.getInsertErrors());
                return "Error inserting rows";
            }

            log.info("Rows inserted successfully");

        } catch (Exception e) {
            log.error("Error: " + e.getMessage());
            return "Error happened";
        }
        return "Insert Completed";
    }

    public String insertDBStreaming() {
        // Not accessible in the free tier
        try {
            log.info("InsertDB started");

            // Load the service account credentials from the classpath
            ClassPathResource resource = new ClassPathResource("springbootbq.json");
            GoogleCredentials credentials = GoogleCredentials.fromStream(resource.getInputStream());

            // Get the connection information we need for Google BQ
            BigQuery bigQuery = BigQueryOptions.newBuilder().setCredentials(credentials).setProjectId(PROJECT_ID).build().getService();

            // Create InsertAll Streaming Request
            InsertAllRequest insertAllRequest = getInsertRequest();

            // Insert data into table
            InsertAllResponse response = bigQuery.insertAll(insertAllRequest);

        } catch (Exception e) {
            log.error("Error" + e.getMessage());
            return "Error Happend";
        }
        return "Insert Completed";
    }

    private InsertAllRequest getInsertRequest() {
        InsertAllRequest.Builder builder = InsertAllRequest.newBuilder(DATASET_ID, TABLE_ID);

        builder.addRow(createFlightMap("RPR", "MAA", "IND", 1, 125.0));
        builder.addRow(createFlightMap("MAA", "RPR", "IND", 2, 250.0));

        return builder.build();
    }

    private Map<String, Object> createFlightMap(String origin, String destination, String airline, Integer stops, Double price) {
        Map<String, Object> rowMap = new HashMap<>();
        rowMap.put("Origin", origin);
        rowMap.put("Destination", destination);
        rowMap.put("Airline", airline);
        rowMap.put("Stops", stops);
        rowMap.put("Price", price);

        return rowMap;
    }
}
