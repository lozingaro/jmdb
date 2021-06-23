/*
 * Copyright (C) 2019 <stefanopio.zingaro@unibo.it>
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.jolie.driver;

import java.util.List;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;

import jolie.runtime.JavaService;
import jolie.runtime.Value;
import jolie.runtime.ValueVector;

/**
 *
 * @author stefanopiozingaro
 */
public class DriverService extends JavaService {

    MongoClient mongoClient = MongoClients.create( "mongodb://127.0.0.1:27017" );
    
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
    public Value query( Value request ) {

        long start = System.currentTimeMillis();

        String db_name = request.getFirstChild( DATABASE ).strValue();
        MongoDatabase db = mongoClient.getDatabase( db_name );

        ValueVector collections = request.getChildren( COLLECTION );

        for (int i = 0; i < collections.size(); i++) {

            String collection_name = collections.get( i ).getFirstChild( COLLECTION_NAME ).strValue();
            
            String data = collections.get( i ).getFirstChild( COLLECTION_DATA ).strValue();

            List< Document > documents = ( List< Document > ) Document.parse( "{ \"data\" : " + data + " } " ).get( "data" );
            
            MongoCollection<Document> collection = db.getCollection( collection_name );
            collection.insertMany( documents );

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
     */
    public Value drop( Value request ) {

        long start = System.currentTimeMillis();

        String db_name = request.getFirstChild( DATABASE ).strValue();
        MongoDatabase db = mongoClient.getDatabase( db_name );

        ValueVector collections = request.getChildren( COLLECTION );

        for (int i = 0; i < collections.size(); i++) {

            String collection_name = collections.get( i ).strValue();
            MongoCollection<Document> collection = db.getCollection( collection_name );
            collection.drop();
        
        }
        
        Value response = Value.create();
        response.getFirstChild( QUERY_TIME ).setValue( System.currentTimeMillis() - start );
        return response;
    }
}
