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
import com.mongodb.*;
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
 *
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

  /** Directory to store user credentials. */
  private static final java.io.File DATA_STORE_DIR =
      new java.io.File(System.getProperty("user.home"), ".store/analytics_sample");
  private static final String startDate = "2015-03-07";
  private static final String endDate = "2015-03-08";

  /**
   * Global instance of the {@link DataStoreFactory}. The best practice is to make it a single
   * globally shared instance across your application.
   */
  private static FileDataStoreFactory DATA_STORE_FACTORY;

  /** Global instance of the HTTP transport. */
  private static HttpTransport HTTP_TRANSPORT;

  /** Global instance of the JSON factory. */
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
      HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
      DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
      Analytics analytics = initializeAnalytics();
      int startIndex = 0;

      MongoClient mongo;
      DB regus_analytics_db;


      String mongodbDbHost = System.getenv("OPENSHIFT_MONGODB_DB_HOST");
      String mongodbDbPort = System.getenv("OPENSHIFT_MONGODB_DB_PORT");

      if (isNotEmpty(mongodbDbHost) && isNotEmpty(mongodbDbPort)) {
        mongo = new MongoClient( mongodbDbHost , Integer.parseInt(mongodbDbPort) );
        regus_analytics_db = mongo.getDB("regus_analytics");
        regus_analytics_db.authenticate("admin", "yel63In1Gymd".toCharArray());
      } else {
        mongo = new MongoClient( "localhost" , 27017 );
        regus_analytics_db = mongo.getDB("regus_analytics");
      }
      DBCollection regus_visited_companies = regus_analytics_db.getCollection("ga");
      DBCollection regus_visit_attributes = regus_analytics_db.getCollection("visit_attrs");

      GaData gaData;
      do {
        gaData = executeDataQueryForVisitedComapnies(analytics, TABLE_ID, startIndex);
        insertVisitedCompaniesData(gaData, regus_visited_companies);

//        printReportInfo(gaData);
//        printProfileInfo(gaData);
//        printQueryInfo(gaData);
//        printPaginationInfo(gaData);
//        printTotalsForAllResults(gaData);
//        printColumnHeaders(gaData);
        startIndex = gaData.getQuery().getStartIndex() + gaData.getQuery().getMaxResults();
      } while (gaData.getNextLink() != null && !gaData.getNextLink().isEmpty());


      startIndex = 0;
      do {
        gaData = executeDataQueryForVisitAttributes(analytics, TABLE_ID, startIndex);
        insertVisitAttributesData(gaData, regus_visit_attributes);

//        printReportInfo(gaData);
//        printProfileInfo(gaData);
//        printQueryInfo(gaData);
//        printPaginationInfo(gaData);
//        printTotalsForAllResults(gaData);
//        printColumnHeaders(gaData);
        startIndex = gaData.getQuery().getStartIndex() + gaData.getQuery().getMaxResults();
      } while (gaData.getNextLink() != null && !gaData.getNextLink().isEmpty());


    } catch (GoogleJsonResponseException e) {
      System.err.println("There was a service error: " + e.getDetails().getCode() + " : "
          + e.getDetails().getMessage());
    } catch (Throwable t) {
      t.printStackTrace();
    }
  }

  /** Authorizes the installed application to access user's protected data. */
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
   *
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

  /**
   * Returns the top 25 organic search keywords and traffic sources by visits. The Core Reporting
   * API is used to retrieve this data.
   *
   * @param dimension
   * @param analytics the Analytics service object used to access the API.
   * @param tableId the table ID from which to retrieve data.
   * @param startIndex
   * @return the response from the API.
   * @throws IOException if an API error occured.
   */
  private static GaData executeDataQueryForVisitedComapnies(Analytics analytics, String tableId, int startIndex) throws IOException {
    Analytics.Data.Ga.Get get = analytics.data().ga().get(tableId, // Table Id.
            startDate, // Start date.
            endDate, // End date.
            "ga:visits") // Metrics.
            .setDimensions("ga:dimension20")
//        .setSort("-ga:visits")
            .setFilters("ga:dimension11!=(Non-Company Visitor)")
            .setMaxResults(500);

    if (startIndex > 0) {
      get.setStartIndex(startIndex);
    }

    return get.execute();

  }

  private static GaData executeDataQueryForVisitAttributes(Analytics analytics, String tableId, int startIndex) throws IOException {
    Analytics.Data.Ga.Get get = analytics.data().ga().get(tableId, // Table Id.
            startDate, // Start date.
            endDate, // End date.
            "ga:visits,ga:users") // Metrics.
            .setDimensions("ga:dimension11")
//        .setSort("-ga:visits")
            .setFilters("ga:dimension11!=(Non-Company Visitor)")
            .setMaxResults(500);

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
   *
   * @param gaData the data returned from the API.
   * @param collection
   */
  private static void insertVisitedCompaniesData(GaData gaData, DBCollection collection) throws JSONException {
    if (gaData.getTotalResults() > 0) {
      System.out.println("Data Table: " + collection);

      // Print the column names.
      //for (ColumnHeaders header : gaData.getColumnHeaders()) {
        //System.out.format("%-32s", header.getName());
      //}
//      System.out.println();

      // Print the rows of data.
      for (List<String> rowValues : gaData.getRows()) {
        /*for (String value : rowValues) {
          System.out.format("%s", value);
        }*/
        DBObject dbObject = new BasicDBObject((Map)JSON.parse(rowValues.get(0)));
        dbObject.removeField("ip");
        HashMap<Object, Object> map = new HashMap<Object, Object>();
        map.put("demandbase_sid", dbObject.get("demandbase_sid"));
        BasicDBObject objectToRemove = new BasicDBObject(map);
        DBObject andRemove = collection.findAndRemove(objectToRemove);
        if (andRemove == null) {
          dbObject.put("firstVisitDate", new SimpleDateFormat("yyyy/MM/dd").format(new Date()));
        } else {
          dbObject.put("firstVisitDate", andRemove.get("firstVisitDate"));
        }
//        System.out.println(andRemove);
        collection.insert(dbObject);
//        System.out.println();
      }
    } else {
      System.out.println("No data");
    }
  }

  private static void insertVisitAttributesData(GaData gaData, DBCollection collection) throws JSONException {
    if (gaData.getTotalResults() > 0) {
      System.out.println("Data Table:" + collection);

      // Print the column names.
      //for (ColumnHeaders header : gaData.getColumnHeaders()) {
      //System.out.format("%-32s", header.getName());
      //}
//      System.out.println();

      // Print the rows of data.
      if (!gaData.getContainsSampledData()) {
        for (List<String> rowValues : gaData.getRows()) {
          /*for (String value : rowValues) {
            System.out.format("%s", value);
          }*/
          String demandBaseId = rowValues.get(0);
          String visits = rowValues.get(1);
          String users = rowValues.get(2);
          HashMap<Object, Object> map = new HashMap<Object, Object>();
          map.put("demandbase_sid", demandBaseId);
          map.put("visits", visits);
          map.put("users", users);
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
