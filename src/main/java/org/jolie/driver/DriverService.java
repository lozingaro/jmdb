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

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import org.bson.Document;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import jolie.runtime.JavaService;
import jolie.runtime.Value;

/**
 *
 * @author stefanopiozingaro
 */
public class DriverService extends JavaService {

    MongoClient mongoClient = MongoClients.create( "mongodb://127.0.0.1:27017" );
    
    public final static String DATABASE = "database";
    public final static String COLLECTION = "collection";
    public final static String DATA = "data";
    public final static String QUERY = "query";
    public final static String RESULT = "result";

    /**
     *
     * @param request
     *
     * @return responseValue
     */
    public Value query( Value request ) {

        MongoDatabase mongoDatabase = mongoClient.getDatabase( 
            request.getFirstChild( DATABASE ).strValue()
            );

        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection( 
          request.getFirstChild( COLLECTION ).strValue()
          );

        String jsonData = request.getFirstChild( DATA ).strValue();

        JSONParser parser = new JSONParser();
        try {
            JSONArray list = ( JSONArray ) parser.parse( jsonData );
            var i = list.iterator();

            while ( i.hasNext() ) {
                JSONObject element = ( JSONObject ) i.next();
                Document document = Document.parse( element.toJSONString() );
                mongoCollection.insertOne( document );
            }
        } catch ( ParseException ex ) {
        }

        Document command = Document.parse(
            request.getFirstChild( QUERY ).strValue()
            );
        
        Value responseValue = Value.create();

        responseValue.getFirstChild( RESULT ).setValue(
            mongoDatabase.runCommand( command ).toJson()
            );

        return responseValue;
    }

    /**
     *
     * @param request
     *
     * @return responseValue
     */
    public Value drop( Value request ) {

        MongoDatabase mongoDatabase = mongoClient.getDatabase( 
            request.getFirstChild( DATABASE ).strValue()
            );
        
        mongoDatabase.getCollection( 
        request.getFirstChild( COLLECTION ).strValue()
        ).drop();

        Value responseValue = Value.create();
        return responseValue;
    }
}
