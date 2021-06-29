package io.github.szingaro.jmdb;

import java.util.List;

import com.mongodb.MongoSocketOpenException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import jolie.runtime.FaultException;
import jolie.runtime.JavaService;
import jolie.runtime.Value;
import jolie.runtime.ValueVector;

@SpringBootApplication
public class JmdbApplication extends JavaService {

	public final static String DATABASE = "database";
    public final static String COLLECTION = "collection";
    public final static String COLLECTION_NAME = "name";
    public final static String COLLECTION_DATA = "data";
    public final static String QUERY = "query";
    public final static String RESULT = "result";
    public final static String QUERY_TIME = "queryTime";

    /**
     *
     * @param request
     *
     * @return response
     */
    public Value query( Value request ) throws FaultException {

        String db_name = request.getFirstChild( DATABASE ).strValue();

        MongoDatabase db;
        try{

            MongoClient mongoClient = MongoClients.create( "mongodb://127.0.0.1:27017" );
            db = mongoClient.getDatabase( db_name );

        } catch (MongoSocketOpenException e) {
            throw new FaultException( e.getMessage() );
        }

        long start = System.currentTimeMillis();
        ValueVector collections = request.getChildren( COLLECTION );

        for (int i = 0; i < collections.size(); i++) {

            String collection_name = collections.get( i ).getFirstChild( COLLECTION_NAME ).strValue();
            
            String data = collections.get( i ).getFirstChild( COLLECTION_DATA ).strValue();

            List< Document > documents = ( List< Document > ) Document.parse( "{ \"data\" : " + data + " } " ).get( "data" );
            
            MongoCollection<Document> collection = db.getCollection( collection_name );

            try {
                collection.insertMany( documents );
            } catch (Exception e) {
                throw new FaultException( e.getMessage() );
            }
            
        }

        ValueVector queries = request.getChildren( QUERY );

        String commandResponse = "";
        for (int i = 0; i < queries.size(); i++) {

            String query = queries.get( i ).strValue();
            Document command = Document.parse( query );
            commandResponse = db.runCommand( command ).toJson();
        
        }

        Value response = Value.create();
        response.getFirstChild( RESULT ).setValue( commandResponse );
        response.getFirstChild( QUERY_TIME ).setValue( System.currentTimeMillis() - start );
        return response;
    }

    /**
     *
     * @param request
     *
     * @return response
     * @throws FaultException
     */
    public Value drop( Value request ) throws FaultException {

        String db_name = request.getFirstChild( DATABASE ).strValue();

        MongoDatabase db;
        try{

            MongoClient mongoClient = MongoClients.create( "mongodb://127.0.0.1:27017" );
            db = mongoClient.getDatabase( db_name );

        } catch (MongoSocketOpenException e) {
            throw new FaultException( e.getMessage() );
        }

        long start = System.currentTimeMillis();
        ValueVector collections = request.getChildren( COLLECTION );

        for (int i = 0; i < collections.size(); i++) {

            String collection_name = collections.get( i ).strValue();
            MongoCollection<Document> collection = db.getCollection( collection_name );

            try {
                collection.drop();  
            } catch (Exception e) {
                throw new FaultException( e.getMessage() );
            }
        
        }
        
        Value response = Value.create();
        response.getFirstChild( QUERY_TIME ).setValue( System.currentTimeMillis() - start );
        return response;
    }

	public static void main(String[] args) {
		SpringApplication.run(JmdbApplication.class, args);
	}

}
