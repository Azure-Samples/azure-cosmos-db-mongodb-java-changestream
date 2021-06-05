package com.azure.cosmos.mongostream;

import java.util.List;
import java.util.Properties;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import static java.util.Arrays.asList;

import java.io.FileInputStream;
import java.io.IOException;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

import java.util.Arrays;

import static com.mongodb.client.model.Projections.*;

/**
 * This sample will demonstrate the usage of ChangeStream in Azure Cosmos DB -
 * Mongo API It will provide three variations #1 Simply reading the ChangeStream
 * #2 Resume from last state #3 Filter the stream
 * #4 it will high availability capability in consumers via lease logic
 */
public class App {

	private static final String OWNER_HEALTH_CHECK_REFRESH_TIME = "ownerHealthCheckRefreshTime";
	private static final String RESUME_TOKEN = "resumeToken";
	private static final String HOST_NAME = "hostName";
	private static final String ID = "_id";

	enum LeaseCollectionState {
		Exists, NotExists
	}

	enum LeaseOwnershipState {
		OwnerExists, OwnerDoesntExists, OwnershipExpired
	}

	enum enableLease {
		yes, no
	}

	public static void init() throws IOException {
		Properties prop = new Properties();

		FileInputStream fileRef = new FileInputStream("src/main/java/com/azure/cosmos/mongostream/config.properties");
		prop.load(fileRef);

		leaseCollectionName = prop.getProperty("leaseCollectionName");
		leaseConnectionString = prop.getProperty("leaseConnectionString");
		leaseDatabaseName = prop.getProperty("leaseDatabaseName");
		healthCheckTimeIntervalInSec = Integer.parseInt(prop.getProperty("healthCheckTimeIntervalInSec"));
		leaseOwnerName = prop.getProperty("leaseOwnerName");
		leaseHostName = prop.getProperty("leaseHostName");
		lastHealthCheck = System.currentTimeMillis();
		withLease = enableLease.valueOf(prop.getProperty("withLease"));

		connectionString = prop.getProperty("ConnectionString");
		databaseName = prop.getProperty("DatabaseName");
		collectionName = prop.getProperty("CollectionName");
	}

	private static enableLease withLease;
	private static String leaseCollectionName;
	private static String leaseConnectionString;
	private static String leaseDatabaseName;
	private static int healthCheckTimeIntervalInSec;
	private static String leaseOwnerName;
	private static String leaseHostName;
	private static long lastHealthCheck;
	private static String connectionString;
	private static String databaseName;
	private static String collectionName;

	public static void main(String[] args) throws IOException {
		init();
		//If you don't want high availability for consumer the simply refer this
		//part of the code.
		if (withLease == enableLease.no) {
			activeWorker(null);
		}
		if (withLease == enableLease.yes) {
			// Connect to lease collection
			MongoClient leaseConnection = establishLeaseConnection();
			MongoDatabase leaseDb = getLeaseDB(leaseConnection);
			MongoCollection<Document> leaseColl = null;
			try {
				// Validate existence of lease collection
				// if not exists then create with partitionkey as _id
				if (checkLease(leaseDb) == LeaseCollectionState.Exists)
					leaseColl = getLeaseColl(leaseDb);
				else
					leaseColl = createLeaseColl(leaseDb);

				// Check who owns the lease
				switch (checkOwnership(leaseColl)) {
				case OwnerExists:
					// as lease exists hence this worker will become the passive worker
					passiveWorker(leaseColl);
					break;
				case OwnerDoesntExists:
					createLeaseDocument(leaseColl);
				case OwnershipExpired:
					acquireLease(leaseColl);
					break;
				default:
					break;
				}

				activeWorker(leaseColl);
			} catch (Exception ex) {
				System.out.println(ex.toString());
			} finally {
				leaseConnection.close();
			}
		} else {
			activeWorker(null);
		}
	}

	private static void acquireLease(MongoCollection<Document> leaseColl) {
		// To acquire the lease
		leaseColl.findOneAndUpdate(Filters.eq(ID, leaseOwnerName),
				Updates.combine(Updates.set(OWNER_HEALTH_CHECK_REFRESH_TIME, System.currentTimeMillis()),
						Updates.set(HOST_NAME, leaseHostName)));
	}

	private static void createLeaseDocument(MongoCollection<Document> leaseColl) {
		Document doc = new Document();
		doc.append(ID, leaseOwnerName);
		doc.append(HOST_NAME, leaseHostName);
		doc.append(RESUME_TOKEN, "");
		doc.append(OWNER_HEALTH_CHECK_REFRESH_TIME, System.currentTimeMillis());
		leaseColl.insertOne(doc);
	}

	private static void passiveWorker(MongoCollection<Document> leaseColl) {
		// Listen to the ChangeStream matching to LeaseOwner.
		Bson match = Aggregates.match(Filters.and(Filters.in("operationType", asList("update", "replace", "insert")),
				Filters.eq(ID, leaseOwnerName)));

		// Select fulldocument
		Bson project = Aggregates.project(fields(include("fullDocument")));

		// Now time to build the pipeline
		List<Bson> pipeline = Arrays.asList(match, project);

		// Create cursor with update_lookup
		MongoChangeStreamCursor<ChangeStreamDocument<Document>> cursor = leaseColl.watch(pipeline)
				.fullDocument(FullDocument.UPDATE_LOOKUP).cursor();

		try {
			// We will keep checking for the changes
			long lastHealthCheckDB = System.currentTimeMillis();
			while (true) {

				// There you go, we got the change document.
				ChangeStreamDocument<Document> csDoc = cursor.tryNext();
				if (csDoc != null) {
					// seek the last health check time
					lastHealthCheckDB = Long
							.parseLong(csDoc.getFullDocument().get(OWNER_HEALTH_CHECK_REFRESH_TIME).toString());
				}
				System.out.println(lastHealthCheckDB);
				if (checkLeaseExpiry(lastHealthCheckDB) == LeaseOwnershipState.OwnershipExpired)
					return;
				// Wait for configured time interval, before the next iteration
				Thread.sleep(healthCheckTimeIntervalInSec * 1000);
			}
		} catch (

		InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			cursor.close();
		}

	}

	private static MongoCollection<Document> createLeaseColl(MongoDatabase leaseDb) {
		Document doc = new Document();
		doc.append("customAction", "CreateCollection");
		doc.append("collection", leaseCollectionName);
		doc.append("offerThroughput", 400);
		doc.append("shardKey", ID);
		Document result = leaseDb.runCommand(doc);
		System.out.println(result.toJson());
		return leaseDb.getCollection(leaseCollectionName);
	}

	private static MongoCollection<Document> getLeaseColl(MongoDatabase leaseDb) {
		return leaseDb.getCollection(leaseCollectionName);
	}

	private static MongoDatabase getLeaseDB(MongoClient leaseConnection) {
		// Get database
		return leaseConnection.getDatabase(leaseDatabaseName);
	}

	private static MongoClient establishLeaseConnection() {
		// TODO Auto-generated method stub
		/*
		 * Replace connection string from the Azure Cosmos DB Portal
		 */
		MongoClientURI uri = new MongoClientURI(leaseConnectionString);

		return new MongoClient(uri);
	}

	private static LeaseOwnershipState checkOwnership(MongoCollection<Document> leaseColl) {
		// Check who the name of owner & host
		Bson filter = Filters.eq(ID, leaseOwnerName);
		Document doc = leaseColl.find(filter).first();

		// if lease document doesn't exists
		if (doc == null) {
			return LeaseOwnershipState.OwnerDoesntExists;
		}

		// In case lease is owned check if the HealthCheck is being performed within the
		// healthChecktime Interval
		long lastHealthCheckDB = Long.parseLong(doc.get(OWNER_HEALTH_CHECK_REFRESH_TIME).toString());
		return checkLeaseExpiry(lastHealthCheckDB);
	}

	private static LeaseOwnershipState checkLeaseExpiry(long lastHealthCheckDB) {
		long systemTime = System.currentTimeMillis();
		long healthCheckGap = systemTime - lastHealthCheckDB;

		System.out.println(lastHealthCheckDB + "," + systemTime + "," + healthCheckGap + " checkexpiry");
		if (healthCheckGap > ((180 + healthCheckTimeIntervalInSec) * 1000)) {
			System.out.println("lease expired");
			return LeaseOwnershipState.OwnershipExpired;
		}
		System.out.println("lease not expired");
		// If lease exists and is not expired then become passive worker.
		return LeaseOwnershipState.OwnerExists;
	}

	private static LeaseCollectionState checkLease(MongoDatabase database) {

		MongoIterable<String> collectionNames = database.listCollectionNames();
		for (final String name : collectionNames) {
			if (name.equalsIgnoreCase(leaseCollectionName)) {
				return LeaseCollectionState.Exists;
			}
		}
		return LeaseCollectionState.NotExists;
	}

	private static void activeWorker(MongoCollection<Document> leaseColl) {
		/*
		 * Replace connection string from the Azure Cosmos DB Portal
		 */
		MongoClientURI uri = new MongoClientURI(connectionString);

		MongoClient mongoClient = null;
		mongoClient = new MongoClient(uri);

		// Get database
		MongoDatabase database = mongoClient.getDatabase(databaseName);

		// Get collection
		MongoCollection<Document> collection = database.getCollection(collectionName);
		MongoChangeStreamCursor<ChangeStreamDocument<org.bson.Document>> cursor = null;
		try {
			// Match is important as Cosmos DB supports Update, Replace & insert operations
			// as of now.
			// add any additional filter as fullDocument.FieldName
			Bson match = Aggregates.match(Filters.in("operationType", asList("update", "replace", "insert")));

			// Pick the field you are most interested in
			Bson project = Aggregates.project(fields(include(ID, "ns", "documentKey", "fullDocument")));

			// This variable is for second example
			BsonDocument resumeToken = null;

			// Now time to build the pipeline
			List<Bson> pipeline = Arrays.asList(match, project);

			// #1 Simple example to seek changes

			// Create cursor with update_lookup
			cursor = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).cursor();

			Document document = new Document("name", "doc-in-step-1-" + Math.random());
			collection.insertOne(document);

			if (withLease == enableLease.yes)
				leaseHealthCheck(leaseColl, resumeToken);

			while (true) {
				// There you go, we got the change document.
				ChangeStreamDocument<Document> csDoc = cursor.tryNext();

				// Let is pick the token which will help us resuming
				// You can save this token in any persistent storage and retrieve it later
				resumeToken = csDoc.getResumeToken();
				// Printing the token
				System.out.println(resumeToken);

				if (withLease == enableLease.yes)
					leaseHealthCheck(leaseColl, resumeToken);
				// Printing the document.
				System.out.println(csDoc.getFullDocument());
				// This break is intentional but in real project feel free to remove it.
				break;
			}

			cursor.close();

			// #2 Sample code to mimic failure, now while we are inserting the cursor is not
			// active state
			// And not listening to the changes
			document = new Document("name", "doc-in-step-2-" + Math.random());
			collection.insertOne(document);

			// Let us use the token and see if we are able to seek all the changes from last
			// read
			if (resumeToken != null)
				cursor = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).resumeAfter(resumeToken)
						.cursor();
			while (true) {

				// There you go we got the changes
				ChangeStreamDocument<Document> csDoc = cursor.tryNext();
				if (csDoc != null) {
					resumeToken = csDoc.getResumeToken();
					System.out.println(resumeToken);
					System.out.println(csDoc.getFullDocument());
					leaseHealthCheck(leaseColl, resumeToken);
					break;
				}
			}
			cursor.close();

			// #3 Let us filter the changes which we want to listen
			// Please note the new change fullDocument.<fieldName>
			match = Aggregates.match(Filters.and(Filters.in("operationType", asList("update", "replace", "insert")),
					Filters.eq("fullDocument.name", "cosmosdb")));

			// Now time to rebuild the pipeline
			pipeline = Arrays.asList(match, project);

			cursor = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).resumeAfter(resumeToken)
					.cursor();
			// let us insert two documents
			document = new Document("name", "doc-in-step-3-" + Math.random());
			collection.insertOne(document);
			document = new Document("name", "cosmosdb");
			collection.insertOne(document);

			while (true) {
				// We shouldn't get he change which have name!=OOP
				ChangeStreamDocument<Document> csDoc = cursor.tryNext();
				if (csDoc != null) {
					resumeToken = csDoc.getResumeToken();
					System.out.println(resumeToken);
					System.out.println(csDoc.getFullDocument());
				}
				if (withLease == enableLease.yes)
					leaseHealthCheck(leaseColl, resumeToken);

			}

		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		} finally {
			// close the cursor
			if (cursor != null)
				cursor.close();
			// clean up
			mongoClient.close();
		}
	}

	private static void leaseHealthCheck(MongoCollection<Document> leaseColl, BsonDocument resumeToken) {
		// This method will help ActiveWorker to continue with the lease
		// It will update the healthcheck timestamp after reaching close to expiry to
		// avoid multiple roundtrips to lease collection.
		long currentTimeInMs = System.currentTimeMillis();
		long timediff = (currentTimeInMs - lastHealthCheck);
		// System.out.println(timediff + "," + currentTimeInMs + "," + lastHealthCheck);

		// Check if the time gap is high enough to update the healthcheck
		if (timediff > (healthCheckTimeIntervalInSec * 1000)) {
			lastHealthCheck = currentTimeInMs;
			Document resultDoc = leaseColl.findOneAndUpdate(
					Filters.and(Filters.eq(ID, leaseOwnerName), Filters.eq(HOST_NAME, leaseHostName)),
					Updates.combine(Updates.set(OWNER_HEALTH_CHECK_REFRESH_TIME, lastHealthCheck),
							Updates.set(RESUME_TOKEN, resumeToken)));
			// System.out.println("Last updated " + timediff);
			if (resultDoc == null) {
				// if the lease got expired and passive worker took it.
				System.out.println("Lease expired for " + HOST_NAME);
				passiveWorker(leaseColl);
			}

		}
	}

}
