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
import java.util.Iterator;
import java.util.logging.Level;
import java.util.logging.Logger;
import jolie.runtime.JavaService;
import jolie.runtime.Value;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

/**
 *
 * @author stefanopiozingaro
 */
public class DriverService extends JavaService {

    MongoClient mongoClient = MongoClients.create( "mongodb://127.0.0.1:27017" );

    /**
     *
     * @param request
     *
     * @return
     */
    public Value query( Value request ) {

        Logger.getLogger( "org.mongodb.driver" ).setLevel( Level.SEVERE );

        Value responseValue = Value.create();

        String databaseName = request.getFirstChild( "database" ).strValue();
        String collectionName = request.getFirstChild( "collection" ).strValue();
        String jsonData = request.getFirstChild( "data" ).strValue();
        String aggregationQuery = request.getFirstChild( "query" ).strValue();

        MongoDatabase mongoDatabase = mongoClient.getDatabase( databaseName );
        MongoCollection<Document> mongoCollection = mongoDatabase.getCollection( collectionName );

        JSONParser parser = new JSONParser();
        try {
            JSONArray list = ( JSONArray ) parser.parse( jsonData );
            Iterator i = list.iterator();

            while ( i.hasNext() ) {
                JSONObject element = ( JSONObject ) i.next();
                Document document = Document.parse( element.toJSONString() );
                mongoCollection.insertOne( document );
            }
        } catch ( ParseException ex ) {
        }

        Document command = Document.parse( aggregationQuery );
        Document result = mongoDatabase.runCommand( command );

        responseValue.getFirstChild( "result" ).setValue( result.toJson() );

        mongoCollection.drop();

        return responseValue;
    }
}
