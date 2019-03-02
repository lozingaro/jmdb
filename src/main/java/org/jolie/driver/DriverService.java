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
import jolie.runtime.JavaService;
import jolie.runtime.Value;
import org.bson.Document;

/**
 *
 * @author stefanopiozingaro
 */
public class DriverService extends JavaService {

    public Value insert( Value requestValue ) {
        Value responseValue = Value.create();

        String databaseName = requestValue.getFirstChild( "database" ).strValue();
        String collectionName = requestValue.getFirstChild( "collection" ).strValue();
        String data = requestValue.getFirstChild( "data" ).strValue();
        String cleanData = data.substring( data.indexOf( "[" ) + 1, data.lastIndexOf( "]" ) );

        MongoClient client = MongoClients.create( "mongodb://127.0.0.1:27017" );
        MongoDatabase database = client.getDatabase( databaseName );
        MongoCollection<Document> collection = database.getCollection( collectionName );

        Document dataDocument = Document.parse( cleanData );
        collection.insertOne( dataDocument );

        return responseValue;
    }

    public Value query( Value requestValue ) {
        Value responseValue = Value.create();

        String databaseName = requestValue.getFirstChild( "database" ).strValue();
        String query = requestValue.getFirstChild( "query" ).strValue();

        MongoClient client = MongoClients.create( "mongodb://127.0.0.1:27017" );
        MongoDatabase database = client.getDatabase( databaseName );

        Document command = Document.parse( query );
        Document result = database.runCommand( command );

        responseValue.getFirstChild( "result" ).setValue( result.toJson() );

        return responseValue;
    }

    public Value delete( Value requestValue ) {
        Value responseValue = Value.create();

        String databaseName = requestValue.getFirstChild( "database" ).strValue();
        String collectionName = requestValue.getFirstChild( "collection" ).strValue();

        MongoClient client = MongoClients.create( "mongodb://127.0.0.1:27017" );
        MongoDatabase database = client.getDatabase( databaseName );
        MongoCollection<Document> collection = database.getCollection( collectionName );

        collection.drop();

        return responseValue;
    }
}
