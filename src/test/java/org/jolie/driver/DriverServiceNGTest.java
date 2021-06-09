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
    request.getFirstChild( DriverService.DATABASE ).setValue( "biometrics" );
    request.getFirstChild( DriverService.COLLECTION ).setValue( "collection_1" );
    request.getFirstChild( DriverService.DATA ).setValue(
    "[{\"date\":\"2018-11-27\",\"t\":[36.7, 38.5]},"
    + "{\"date\":\"2018-11-28\",\"t\":[36.7, 38.5]}]" );
    request.getFirstChild( DriverService.QUERY ).setValue( "{ aggregate: "
    + "\"collection_1\", pipeline: [ { $match : { $or: [ "
    + "{ date: \"2018-11-27\" }, { date: \"2018-11-28\" } ] } }, "
    + "{ $project : { temperatures: [ \"$t\" ], pseudoId: [ \"new\" ] } } "
    + "], cursor: { } }" );
    new DriverService().query( request );
  }
  
  /**
  * Test of drop method, of class DriverService.
  * @throws IOException
  */
  @org.testng.annotations.Test
  public void testDrop() throws IOException {
    Value request = Value.create();
    request.getFirstChild( DriverService.DATABASE ).setValue( "biometrics" );
    request.getFirstChild( DriverService.COLLECTION ).setValue( "collection_1" );
    
    new DriverService().drop( request );
  }
}
