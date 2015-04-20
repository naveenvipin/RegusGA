/*
 * Copyright (c) 2012 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.api.services.samples.analytics.cmdline;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.DataStoreFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.analytics.Analytics;
import com.google.api.services.analytics.AnalyticsScopes;
import com.google.api.services.analytics.model.GaData;
import com.google.api.services.analytics.model.GaData.ColumnHeaders;
import com.google.api.services.analytics.model.GaData.ProfileInfo;
import com.google.api.services.analytics.model.GaData.Query;
import com.google.common.collect.Lists;
import com.mongodb.*;
import org.apache.commons.lang3.time.DateUtils;
import org.json.JSONException;
import org.mortbay.util.ajax.JSON;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.commons.lang3.StringUtils.*;

/**
 * This application demonstrates how to use the Google Analytics Java client library to access all
 * the pieces of data returned by the Google Analytics Core Reporting API v3.
 * <p/>
 * <p>
 * To run this, you must supply your Google Analytics TABLE ID. Read the Core Reporting API
 * developer guide to learn how to get this value.
 * </p>
 *
 * @author api.nickm@gmail.com
 */
public class CoreReportingApiReferenceSample {

    /**
     * Be sure to specify the name of your application. If the application name is {@code null} or
     * blank, the application will log a warning. Suggested format is "MyCompany-ProductName/1.0".
     */
    private static final String APPLICATION_NAME = "Demand Base";

    /**
     * Used to identify from which reporting profile to retrieve data. Format is ga:xxx where xxx is
     * your profile ID.
     */
    private static final String TABLE_ID = "ga:88176229";

    /**
     * Directory to store user credentials.
     */
    private static final java.io.File DATA_STORE_DIR =
            new java.io.File(System.getProperty("user.home"), ".store/analytics_sample");
    private static final String VISIT_ATTRIBUTES_METRICS = "ga:visits,ga:users,ga:pageviews,ga:sessionDuration";
    private static final String VISIT_ATTRIBUTES_DIMENSIONS = "ga:dimension11,ga:dimension2,ga:dimension3,ga:pagePath,ga:source,ga:medium";
    private static Date startDate = DateUtils.addDays(new Date(), -1); //new SimpleDateFormat("yyyy-MM-dd").format(DateUtils.addDays(new Date(), -1));
    private static Date endDate = DateUtils.addDays(new Date(), -1);

    /**
     * Global instance of the {@link DataStoreFactory}. The best practice is to make it a single
     * globally shared instance across your application.
     */
    private static FileDataStoreFactory DATA_STORE_FACTORY;

    /**
     * Global instance of the HTTP transport.
     */
    private static HttpTransport HTTP_TRANSPORT;

    /**
     * Global instance of the JSON factory.
     */
    private static final JsonFactory JSON_FACTORY = new JacksonFactory();

    /**
     * Main demo. This first initializes an Analytics service object. It then queries for the top 25
     * organic search keywords and traffic sources by visits. Finally each important part of the
     * response is printed to the screen.
     *
     * @param args command line args.
     */
    public static void main(String[] args) {

        try {
            if (args.length == 2) {
                // Start and end date are supplied
                startDate = new SimpleDateFormat("yyyy-MM-dd").parse(args[0]);
                endDate = new SimpleDateFormat("yyyy-MM-dd").parse(args[1]);
            }
            System.out.println("Retrieving for dates " + startDate + " " + endDate);

            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
            Analytics analytics = initializeAnalytics();


            MongoClient mongo = new MongoClient("localhost", 27017);
            DB regus_analytics_db = mongo.getDB("regus_analytics");

            DBCollection regus_visited_companies = regus_analytics_db.getCollection("ga");
            DBCollection regus_visit_attributes = regus_analytics_db.getCollection("visit_attrs");

            GaData gaData;

            for (Date d = startDate; !DateUtils.isSameDay(d, DateUtils.addDays(endDate, 1)); d = DateUtils.addDays(d, 1)) {
                int startIndex = 0;
                do {
                    System.out.println("Executing data query for visited companies for date: " + d);
                    gaData = executeDataQueryForVisitedCompanies(analytics, TABLE_ID, startIndex, d);
                    insertVisitedCompaniesData(gaData, regus_visited_companies, d);
                    startIndex = gaData.getQuery().getStartIndex() + gaData.getQuery().getMaxResults();
                } while (gaData.getNextLink() != null && !gaData.getNextLink().isEmpty());


                startIndex = 0;
                do {
                    System.out.println("Executing data query for visit attributes for date: " + d);
                    gaData = executeDataQueryForVisitAttributes(analytics, TABLE_ID, startIndex, d);
                    insertVisitAttributesData(gaData, regus_visit_attributes, d);

                    startIndex = gaData.getQuery().getStartIndex() + gaData.getQuery().getMaxResults();
                } while (gaData.getNextLink() != null && !gaData.getNextLink().isEmpty());
            }
        } catch (GoogleJsonResponseException e) {
            System.err.println("There was a service error: " + e.getDetails().getCode() + " : "
                    + e.getDetails().getMessage());
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    /**
     * Authorizes the installed application to access user's protected data.
     */
    private static Credential authorize() throws Exception {
        // load client secrets
        GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(
                JSON_FACTORY, new InputStreamReader(new FileInputStream(System.getProperty("user.home") + "/client_secrets.json")));
        if (clientSecrets.getDetails().getClientId().startsWith("Enter")
                || clientSecrets.getDetails().getClientSecret().startsWith("Enter ")) {
            System.out.println(
                    "Enter Client ID and Secret from https://code.google.com/apis/console/?api=analytics "
                            + "into analytics-cmdline-sample/src/main/resources/client_secrets.json");
            System.exit(1);
        }
        // set up authorization code flow
        GoogleAuthorizationCodeFlow flow = new GoogleAuthorizationCodeFlow.Builder(
                HTTP_TRANSPORT, JSON_FACTORY, clientSecrets,
                Collections.singleton(AnalyticsScopes.ANALYTICS_READONLY)).setDataStoreFactory(
                DATA_STORE_FACTORY).build();
        // authorize
        return new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
    }


    /**
     * Performs all necessary setup steps for running requests against the API.
     *
     * @return an initialized Analytics service object.
     * @throws Exception if an issue occurs with OAuth2Native authorize.
     */
    private static Analytics initializeAnalytics() throws Exception {
        // Authorization.
        Credential credential = authorize();

        String accessToken = credential.getAccessToken();

        // Set up and return Google Analytics API client.
        return new Analytics.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).setApplicationName(
                APPLICATION_NAME).build();
    }

    private static GaData executeDataQueryForVisitedCompanies(Analytics analytics, String tableId, int startIndex, Date d) throws IOException {
        Analytics.Data.Ga.Get get = analytics.data().ga().get(tableId, // Table Id.
                new SimpleDateFormat("yyyy-MM-dd").format(d), // Start date.
                new SimpleDateFormat("yyyy-MM-dd").format(d), // End date.
                "ga:visits") // Metrics.
                .setDimensions("ga:dimension20")
//        .setSort("-ga:visits")
                .setFilters("ga:dimension11!=(Non-Company Visitor)")
                .setMaxResults(5000);

        if (startIndex > 0) {
            get.setStartIndex(startIndex);
        }

        return get.execute();

    }

    private static GaData executeDataQueryForVisitAttributes(Analytics analytics, String tableId, int startIndex, Date d) throws IOException {
        Analytics.Data.Ga.Get get = analytics.data().ga().get(tableId, // Table Id.
                new SimpleDateFormat("yyyy-MM-dd").format(d), // Start date.
                new SimpleDateFormat("yyyy-MM-dd").format(d), // End date.
                VISIT_ATTRIBUTES_METRICS) // Metrics.
                .setDimensions(VISIT_ATTRIBUTES_DIMENSIONS)
//        .setSort("-ga:visits")
                .setFilters("ga:dimension11!=(Non-Company Visitor)")
                .setMaxResults(5000);

        if (startIndex > 0) {
            get.setStartIndex(startIndex);
        }

        return get.execute();

    }

    /**
     * Prints general information about this report.
     *
     * @param gaData the data returned from the API.
     */
    private static void printReportInfo(GaData gaData) {
        System.out.println();
        System.out.println("Response:");
        System.out.println("ID:" + gaData.getId());
        System.out.println("Self link: " + gaData.getSelfLink());
        System.out.println("Kind: " + gaData.getKind());
        System.out.println("Contains Sampled Data: " + gaData.getContainsSampledData());
    }

    /**
     * Prints general information about the profile from which this report was accessed.
     *
     * @param gaData the data returned from the API.
     */
    private static void printProfileInfo(GaData gaData) {
        ProfileInfo profileInfo = gaData.getProfileInfo();

        System.out.println("Profile Info");
        System.out.println("Account ID: " + profileInfo.getAccountId());
        System.out.println("Web Property ID: " + profileInfo.getWebPropertyId());
        System.out.println("Internal Web Property ID: " + profileInfo.getInternalWebPropertyId());
        System.out.println("Profile ID: " + profileInfo.getProfileId());
        System.out.println("Profile Name: " + profileInfo.getProfileName());
        System.out.println("Table ID: " + profileInfo.getTableId());
    }

    /**
     * Prints the values of all the parameters that were used to query the API.
     *
     * @param gaData the data returned from the API.
     */
    private static void printQueryInfo(GaData gaData) {
        Query query = gaData.getQuery();

        System.out.println("Query Info:");
        System.out.println("Ids: " + query.getIds());
        System.out.println("Start Date: " + query.getStartDate());
        System.out.println("End Date: " + query.getEndDate());
        System.out.println("Metrics: " + query.getMetrics()); // List
        System.out.println("Dimensions: " + query.getDimensions()); // List
        System.out.println("Sort: " + query.getSort()); // List
        System.out.println("Segment: " + query.getSegment());
        System.out.println("Filters: " + query.getFilters());
        System.out.println("Start Index: " + query.getStartIndex());
        System.out.println("Max Results: " + query.getMaxResults());
    }

    /**
     * Prints common pagination information.
     *
     * @param gaData the data returned from the API.
     */
    private static void printPaginationInfo(GaData gaData) {
        System.out.println("Pagination Info:");
        System.out.println("Previous Link: " + gaData.getPreviousLink());
        System.out.println("Next Link: " + gaData.getNextLink());
        System.out.println("Items Per Page: " + gaData.getItemsPerPage());
        System.out.println("Total Results: " + gaData.getTotalResults());
    }

    /**
     * Prints the total metric value for all rows the query matched.
     *
     * @param gaData the data returned from the API.
     */
    private static void printTotalsForAllResults(GaData gaData) {
        System.out.println("Metric totals over all results:");
        Map<String, String> totalsMap = gaData.getTotalsForAllResults();
        for (Map.Entry<String, String> entry : totalsMap.entrySet()) {
            System.out.println(entry.getKey() + " : " + entry.getValue());
        }
    }

    /**
     * Prints the information for each column. The reporting data from the API is returned as rows of
     * data. The column headers describe the names and types of each column in rows.
     *
     * @param gaData the data returned from the API.
     */
    private static void printColumnHeaders(GaData gaData) {
        System.out.println("Column Headers:");

        for (ColumnHeaders header : gaData.getColumnHeaders()) {
            System.out.println("Column Name: " + header.getName());
            System.out.println("Column Type: " + header.getColumnType());
            System.out.println("Column Data Type: " + header.getDataType());
        }
    }

    /**
     * Prints all the rows of data returned by the API.
     *  @param gaData     the data returned from the API.
     * @param collection
     * @param d
     */
    private static void insertVisitedCompaniesData(GaData gaData, DBCollection collection, Date d) throws JSONException {
        if (gaData.getTotalResults() > 0) {
            System.out.println("Data Table: " + collection);

            for (List<String> rowValues : gaData.getRows()) {
                Map jsonMap = (Map) JSON.parse(rowValues.get(0));
                if (jsonMap.get("demandbase_sid") == null) {
                    continue;
                }
                DBObject dbObject = new BasicDBObject(jsonMap);
                dbObject.removeField("ip");
                HashMap<Object, Object> map = new HashMap<Object, Object>();
                map.put("demandbase_sid", dbObject.get("demandbase_sid"));
                BasicDBObject objectToRemove = new BasicDBObject(map);
                DBObject andRemove = collection.findAndRemove(objectToRemove);
                if (andRemove == null) {
                    dbObject.put("firstVisitDate", new SimpleDateFormat("yyyy/MM/dd").format(d));
                } else {
                    dbObject.put("firstVisitDate", andRemove.get("firstVisitDate"));
                }
                collection.insert(dbObject);
            }
        } else {
            System.out.println("No data");
        }
    }

    private static void insertVisitAttributesData(GaData gaData, DBCollection collection, Date d) throws JSONException {
        if (gaData.getTotalResults() > 0) {
            System.out.println("Data Table:" + collection);

            String[] columns = (VISIT_ATTRIBUTES_METRICS + "," + VISIT_ATTRIBUTES_DIMENSIONS).split(",");
            HashMap<String, Integer> columnLookUp = new HashMap<String, Integer>();
            List<ColumnHeaders> columnHeaders = gaData.getColumnHeaders();
            for (String column : columns) {
                for (int i=0; i< columnHeaders.size(); i++) {
                    if (columnHeaders.get(i).getName().equals(column)) {
                        columnLookUp.put(column, i);
                        break;
                    }
                }
            }

            if (!gaData.getContainsSampledData()) {
                for (List<String> rowValues : gaData.getRows()) {
                    String demandBaseId = rowValues.get(columnLookUp.get("ga:dimension11"));
                    String clientId = rowValues.get(columnLookUp.get("ga:dimension2"));
                    String pagePath = rowValues.get(columnLookUp.get("ga:pagePath"));
                    String source = rowValues.get(columnLookUp.get("ga:source"));
                    String medium = rowValues.get(columnLookUp.get("ga:medium"));
                    String visits = rowValues.get(columnLookUp.get("ga:visits"));
                    String users = rowValues.get(columnLookUp.get("ga:users"));
//                    String pageViews = rowValues.get(columnLookUp.get("ga:pageviews"));
//                    String sessionDuration = rowValues.get(columnLookUp.get("ga:sessionDuration"));


                    HashMap<Object, Object> map = new HashMap<Object, Object>();
                    map.put("demandbase_sid", demandBaseId);
                    map.put("clientId", clientId);
                    map.put("pagePath", pagePath);
                    map.put("source", source);
                    map.put("medium", medium);
                    map.put("visits", visits);
                    map.put("users", users);
//                    map.put("pageViews", pageViews);
//                    map.put("sessionDuration", sessionDuration);
                    map.put("date", new SimpleDateFormat("yyyy/MM/dd").format(d));
                    BasicDBObject objectToInsert = new BasicDBObject(map);
                    collection.insert(objectToInsert);
                }
            } else {
                System.out.println(" Excluding analytics data since it has sample data");
            }
        } else {
            System.out.println("No data");
        }
    }
}
