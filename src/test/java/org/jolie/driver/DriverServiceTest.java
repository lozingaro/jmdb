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

import jolie.runtime.Value;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author stefanopiozingaro
 */
public class DriverServiceTest {

    public DriverServiceTest() {
    }

    @BeforeAll
    public static void setUpClass() {
    }

    @AfterAll
    public static void tearDownClass() {
    }

    @BeforeEach
    public void setUp() {
    }

    @AfterEach
    public void tearDown() {
    }

    /**
     * Test of insert method, of class DriverService.
     */
    @org.junit.jupiter.api.Test
    public void testInsert() {
        System.out.println( "insert" );
        Value requestValue = Value.create();
        requestValue.getFirstChild( "database" ).setValue( "biometrics" );
        requestValue.getFirstChild( "collection" ).setValue( "collection_1" );
        requestValue.getFirstChild( "data" ).setValue(
          "[{\"date\":\"2018-01-01\",\"t\":[36.7, 38.5]}]" );
        DriverService instance = new DriverService();
        Value expResult = Value.create();
        Value result = instance.insert( requestValue );
        assertEquals( expResult.toPrettyString( "root" ), result.toPrettyString( "root" ) );
    }

    /**
     * Test of query method, of class DriverService.
     */
    @org.junit.jupiter.api.Test
    public void testQuery() {
        System.out.println( "query" );
        testInsert();
        Value requestValue = Value.create();
        requestValue.getFirstChild( "database" ).setValue( "biometrics" );
        requestValue.getFirstChild( "query" ).setValue( "{ aggregate: \"collection_1\", pipeline: [ { $match : { $or: [ { date: '2018-11-27' }, { date: '2018-11-28' } ] } } ], cursor: { } }" );
        DriverService instance = new DriverService();
        testDelete();
        Value expResult = Value.create();
        expResult.getFirstChild( "result" );
        Value result = instance.query( requestValue );
        assertEquals( false, result.getFirstChild( "result" ).getFirstChild( "ok" ).isLong() );
    }

    /**
     * Test of delete method, of class DriverService.
     */
    @org.junit.jupiter.api.Test
    public void testDelete() {
        System.out.println( "delete" );
        testInsert();
        Value requestValue = Value.create();
        requestValue.getFirstChild( "database" ).setValue( "biometrics" );
        requestValue.getFirstChild( "collection" ).setValue( "collection_1" );
        DriverService instance = new DriverService();
        Value expResult = Value.create();
        Value result = instance.delete( requestValue );
        assertEquals( expResult.toPrettyString( "root" ), result.toPrettyString( "root" ) );
    }

}
