package com.azure.cosmos.mongostream;

import java.util.List;

import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import static java.util.Arrays.asList;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoChangeStreamCursor;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.FullDocument;

import java.util.Arrays;

import static com.mongodb.client.model.Projections.*;

/**
 * This sample will demonstrate the usage of ChangeStream in Azure Cosmos DB - Mongo API
 * It will provide three variations
 * #1 Simply reading the ChangeStream
 * #2 Resume from last state
 * #3 Filter the stream
 */
public class App {

	public static void main(String[] args) {
		/*
		 * Replace connection string from the Azure Cosmos DB Portal
		 */
		MongoClientURI uri = new MongoClientURI(
				"connection string from portal");

		MongoClient mongoClient = null;
		mongoClient = new MongoClient(uri);

		// Get database
		MongoDatabase database = mongoClient.getDatabase("ProductDatabase");

		// Get collection
		MongoCollection<org.bson.Document> collection = database.getCollection("Products");
		try {
			// Match is important as Cosmos DB supports Update, Replace & insert operations
			// as of now.
			// add any additional filter as fullDocument.FieldName
			Bson match = Aggregates.match(Filters.in("operationType", asList("update", "replace", "insert")));

			// Pick the field you are most interested in
			Bson project = Aggregates.project(fields(include("_id", "ns", "documentKey", "fullDocument")));

			// This variable is for second example
			BsonDocument resumeToken = null;

			// Now time to build the pipeline
			List<Bson> pipeline = Arrays.asList(match, project);

			//#1 Simple example to seek changes
			
			// Create cursor with update_lookup
			MongoChangeStreamCursor<ChangeStreamDocument<org.bson.Document>> cursor = collection.watch(pipeline)
					.fullDocument(FullDocument.UPDATE_LOOKUP).cursor();
			
			Document document = new Document("name", "doc-in-step-1-" + Math.random());
			collection.insertOne(document);
			
			while (cursor.hasNext()) {
				// There you go, we got the change document.
				ChangeStreamDocument<Document> csDoc = cursor.next();

				// Let is pick the token which will help us resuming
				// You can save this token in any persistent storage and retrieve it later
				resumeToken = csDoc.getResumeToken();
				//Printing the token
				System.out.println(resumeToken);
				
				//Printing the document.
				System.out.println(csDoc.getFullDocument());
				//This break is intentional but in real project feel free to remove it.
				break;
			}
			
			cursor.close();
			
			//#2 Sample code to mimic failure, now while we are inserting the cursor is not active state
			//And not listening to the changes
			 document = new Document("name", "doc-in-step-2-" + Math.random());
			collection.insertOne(document);
			
			//Let us use the token and see if we are able to seek all the changes from last read
			if (resumeToken != null)
				cursor = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).resumeAfter(resumeToken)
						.cursor();
			while (cursor.hasNext()) {
				//There you go we got the changes
				ChangeStreamDocument<Document> csDoc = cursor.next();
				resumeToken = csDoc.getResumeToken();
				System.out.println(resumeToken);
				System.out.println(csDoc.getFullDocument());
				break;
			}
			cursor.close();

			//#3 Let us filter the changes which we want to listen
			//Please note the new change fullDocument.<fieldName>
			match = Aggregates.match(Filters.and(Filters.in("operationType", asList("update", "replace", "insert")),
					Filters.eq("fullDocument.name", "cosmosdb")));
			
			// Now time to rebuild the pipeline
			pipeline = Arrays.asList(match, project);
			
			cursor = collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).resumeAfter(resumeToken)
					.cursor();
			//let us insert two documents
			document = new Document("name", "doc-in-step-3-" + Math.random());
			collection.insertOne(document);
			document = new Document("name", "cosmosdb");
			collection.insertOne(document);
			
			while (cursor.hasNext()) {
				//We shouldn't get he change which have name!=OOP
				ChangeStreamDocument<Document> csDoc = cursor.next();
				resumeToken = csDoc.getResumeToken();
				System.out.println(resumeToken);
				System.out.println(csDoc.getFullDocument());
			}
			//close the cursor
			cursor.close();
			
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		} finally {
			//clean up
			mongoClient.close();
		}
	}

}
