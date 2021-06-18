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

import java.io.IOException;

import jolie.runtime.Value;

/**
*
* @author stefanopiozingaro
*/
public class DriverServiceNGTest {
  
  public DriverServiceNGTest() {
  }
  
  @org.testng.annotations.BeforeClass
  public static void setUpClass() throws Exception {
  }
  
  @org.testng.annotations.AfterClass
  public static void tearDownClass() throws Exception {
  }
  
  @org.testng.annotations.BeforeMethod
  public void setUpMethod() throws Exception {
  }
  
  @org.testng.annotations.AfterMethod
  public void tearDownMethod() throws Exception {
  }
  
  /**
  * Test of query method, of class DriverService.
  */
  @org.testng.annotations.Test
  public void testQuery() {
    Value request = Value.create();
    request.getFirstChild( DriverService.DATABASE ).setValue( "db_encephalopathy" );
    
    Value collection0 = Value.create();
    collection0.getFirstChild( DriverService.COLLECTION_NAME ).setValue( "collection_0_biometric" );
    String biometric_data = "[{\"date\":[20201127],\"t\":[37,36,36],\"hr\":[64,58,57]},{\"date\":[20201128],\"t\":[37,36,36],\"hr\":[64,58,57]},{\"date\":[20201129],\"t\":[37,36,36],\"hr\":[64,58,57]},{\"date\":[20201130],\"t\":[37,36,36],\"hr\":[64,58,57]}]";
    collection0.getFirstChild( DriverService.COLLECTION_DATA ).setValue( biometric_data );
    request.getChildren( DriverService.COLLECTION ).add( collection0 );

    Value collection1 = Value.create();
    collection1.getFirstChild( DriverService.COLLECTION_NAME ).setValue( "collection_0_sleeplog" );
    String sleeplog_data = "[{\"y\":[2020],\"M\":[{\"m\":[11],\"D\":[{\"d\":[27],\"L\":[{\"s\":[\"23:33\"],\"e\":[\"07:04\"],\"q\":[\"poor\"]}]},{\"d\":[28],\"L\":[{\"s\":[\"21:13\"],\"e\":[\"09:34\"],\"q\":[\"good\"]}]},{\"d\":[29],\"L\":[{\"s\":[\"21:01\"],\"e\":[\"03:12\"],\"q\":[\"good\"]},{\"s\":[\"03:36\"],\"e\":[\"09:58\"],\"q\":[\"good\"]}]},{\"d\":[30],\"L\":[{\"s\":[\"20:33\"],\"e\":[\"01:14\"],\"q\":[\"poor\"]},{\"s\":[\"01:32\"],\"e\":[\"06:15\"],\"q\":[\"good\"]}]}]}]}]";
    collection1.getFirstChild( DriverService.COLLECTION_DATA ).setValue( sleeplog_data );
    request.getChildren( DriverService.COLLECTION ).add( collection1 );

    String biometric_query = "{\"aggregate\":\"collection_0_biometric\",\"pipeline\":[{\"$match\":{\"$or\":[{\"date\":[20201128]},{\"date\":[20201129]},{\"date\":[20201130]}]}},{\"$group\":{\"_id\":1,\"t\":{\"$push\":\"$t\"}}},{\"$project\":{\"_id\":1,\"t\":{\"$reduce\":{\"input\":\"$t\",\"initialValue\":[],\"in\":{\"$concatArrays\":[\"$$value\",\"$$this\"]}}},\"pseudoId\":{\"$literal\":123456}}},{\"$out\":\"collection_0_biometric\"}],\"cursor\":{}}";
    request.getChildren( DriverService.QUERY ).add( 
      Value.create( biometric_query )
     );

    String sleeplog_query = "{\"aggregate\":\"collection_0_sleeplog\",\"pipeline\":[{\"$unwind\":{\"path\":\"$M\"}},{\"$unwind\":{\"path\":\"$M.D\"}},{\"$unwind\":{\"path\":\"$M.D.L\"}},{\"$project\":{\"year\":\"$y\",\"month\":\"$M.m\",\"day\":\"$M.D.d\",\"quality\":\"$M.D.L.q\"}},{\"$match\":{\"$and\":[{\"year\":[2020]},{\"$and\":[{\"month\":[11]},{\"$or\":[{\"day\":[29]},{\"day\":[30]}]}]}]}},{\"$group\":{\"_id\":1,\"quality\":{\"$push\":\"$quality\"}}},{\"$project\":{\"pseudoId\":{\"$literal\":123456},\"quality\":{\"$reduce\":{\"input\":\"$quality\",\"initialValue\":[],\"in\":{\"$concatArrays\":[\"$$value\",\"$$this\"]}}}}},{\"$lookup\":{\"from\":\"collection_0_biometric\",\"localField\":\"pseudoId\",\"foreignField\":\"pseudoId\",\"as\":\"temperatures\"}},{\"$project\":{\"quality\":\"$quality\",\"pseudoId\":\"$pseudoId\",\"temperatures\":{\"$reduce\":{\"input\":\"$temperatures.t\",\"initialValue\":[],\"in\":{\"$concatArrays\":[\"$$value\",\"$$this\"]}}}}}],\"cursor\":{}}";
    request.getChildren( DriverService.QUERY ).add( 
      Value.create(sleeplog_query)
     );
    
    new DriverService().query( request );
  }
  
  /**
  * Test of drop method, of class DriverService.
  * @throws IOException
  */
  @org.testng.annotations.Test
  public void testDrop() throws IOException {
    
    Value request = Value.create();
    request.getFirstChild( DriverService.DATABASE ).setValue( "db" );

    request.getChildren( DriverService.COLLECTION ).add( 
      Value.create( "collection_0_biometric" )
    );

    request.getChildren( DriverService.COLLECTION ).add( 
      Value.create( "collection_0_sleeplog" )
    );

    new DriverService().drop( request );
  }
}
