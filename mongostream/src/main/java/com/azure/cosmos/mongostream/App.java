package com.azure.cosmos.mongostream;

import java.util.List;

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
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		/*
		 * Replace connection string from the Azure Cosmos DB Portal
		 */
		MongoClientURI uri = new MongoClientURI(
				"mongodb://test-mongo-36:XbSltl4UutoN1qMnxouDfYqttXxZN4OqDD456VMv1wtGtfZNrWxlsGVTNwcnf5q5gLkEYno5hqNFJ3x0sBjkgg==@test-mongo-36.mongo.cosmos.azure.com:10255/?ssl=true&replicaSet=globaldb&retrywrites=false&maxIdleTimeMS=120000&appName=@test-mongo-36@");

		MongoClient mongoClient = null;
		mongoClient = new MongoClient(uri);

		// Get database
		MongoDatabase database = mongoClient.getDatabase("ProductDatabase");

		// Get collection
		MongoCollection<org.bson.Document> collection = database.getCollection("Products");
		try {
			Bson match=Aggregates.match(Filters.in("operationType", 
					asList("update", "replace", "insert")));
			Bson project=Aggregates.project(fields(include("_id","ns","documentKey","fullDocument")));
			
			List<Bson> pipeline =Arrays.asList(match,project);
			MongoChangeStreamCursor<ChangeStreamDocument<org.bson.Document>> cursor  = 
					collection.watch(pipeline).fullDocument(FullDocument.UPDATE_LOOKUP).cursor();
			
			while (cursor.hasNext()) {
				System.out.println(cursor.next());
			}
			cursor.close();
		} catch (Exception ex) {
			System.out.println(ex.getMessage());
		} finally {
				mongoClient.close();
		}
	}

}
