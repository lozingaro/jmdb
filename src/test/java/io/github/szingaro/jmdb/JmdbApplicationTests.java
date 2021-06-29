package io.github.szingaro.jmdb;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import jolie.runtime.FaultException;
import jolie.runtime.Value;

@SpringBootTest
class JmdbApplicationTests {

	@Test
	public void testQuery() {
		Value request = Value.create();
		request.getFirstChild( JmdbApplication.DATABASE ).setValue( "db_encephalopathy" );
		
		Value collection0 = Value.create();
		collection0.getFirstChild( JmdbApplication.COLLECTION_NAME ).setValue( "collection_0_biometric" );
		String biometric_data = "[{\"date\":[20201127],\"t\":[37,36,36],\"hr\":[64,58,57]},{\"date\":[20201128],\"t\":[37,36,36],\"hr\":[64,58,57]},{\"date\":[20201129],\"t\":[37,36,36],\"hr\":[64,58,57]},{\"date\":[20201130],\"t\":[37,36,36],\"hr\":[64,58,57]}]";
		collection0.getFirstChild( JmdbApplication.COLLECTION_DATA ).setValue( biometric_data );
		request.getChildren( JmdbApplication.COLLECTION ).add( collection0 );
	
		Value collection1 = Value.create();
		collection1.getFirstChild( JmdbApplication.COLLECTION_NAME ).setValue( "collection_0_sleeplog" );
		String sleeplog_data = "[{\"y\":[2020],\"M\":[{\"m\":[11],\"D\":[{\"d\":[27],\"L\":[{\"s\":[\"23:33\"],\"e\":[\"07:04\"],\"q\":[\"poor\"]}]},{\"d\":[28],\"L\":[{\"s\":[\"21:13\"],\"e\":[\"09:34\"],\"q\":[\"good\"]}]},{\"d\":[29],\"L\":[{\"s\":[\"21:01\"],\"e\":[\"03:12\"],\"q\":[\"good\"]},{\"s\":[\"03:36\"],\"e\":[\"09:58\"],\"q\":[\"good\"]}]},{\"d\":[30],\"L\":[{\"s\":[\"20:33\"],\"e\":[\"01:14\"],\"q\":[\"poor\"]},{\"s\":[\"01:32\"],\"e\":[\"06:15\"],\"q\":[\"good\"]}]}]}]}]";
		collection1.getFirstChild( JmdbApplication.COLLECTION_DATA ).setValue( sleeplog_data );
		request.getChildren( JmdbApplication.COLLECTION ).add( collection1 );
	
		String biometric_query = "{\"aggregate\":\"collection_0_biometric\",\"pipeline\":[{\"$match\":{\"$or\":[{\"date\":[20201128]},{\"date\":[20201129]},{\"date\":[20201130]}]}},{\"$group\":{\"_id\":1,\"t\":{\"$push\":\"$t\"}}},{\"$project\":{\"_id\":1,\"t\":{\"$reduce\":{\"input\":\"$t\",\"initialValue\":[],\"in\":{\"$concatArrays\":[\"$$value\",\"$$this\"]}}},\"pseudoId\":{\"$literal\":123456}}},{\"$out\":\"collection_0_biometric\"}],\"cursor\":{}}";
		request.getChildren( JmdbApplication.QUERY ).add( 
		  Value.create( biometric_query )
		 );
	
		String sleeplog_query = "{\"aggregate\":\"collection_0_sleeplog\",\"pipeline\":[{\"$unwind\":{\"path\":\"$M\"}},{\"$unwind\":{\"path\":\"$M.D\"}},{\"$unwind\":{\"path\":\"$M.D.L\"}},{\"$project\":{\"year\":\"$y\",\"month\":\"$M.m\",\"day\":\"$M.D.d\",\"quality\":\"$M.D.L.q\"}},{\"$match\":{\"$and\":[{\"year\":[2020]},{\"$and\":[{\"month\":[11]},{\"$or\":[{\"day\":[29]},{\"day\":[30]}]}]}]}},{\"$group\":{\"_id\":1,\"quality\":{\"$push\":\"$quality\"}}},{\"$project\":{\"pseudoId\":{\"$literal\":123456},\"quality\":{\"$reduce\":{\"input\":\"$quality\",\"initialValue\":[],\"in\":{\"$concatArrays\":[\"$$value\",\"$$this\"]}}}}},{\"$lookup\":{\"from\":\"collection_0_biometric\",\"localField\":\"pseudoId\",\"foreignField\":\"pseudoId\",\"as\":\"temperatures\"}},{\"$project\":{\"quality\":\"$quality\",\"pseudoId\":\"$pseudoId\",\"temperatures\":{\"$reduce\":{\"input\":\"$temperatures.t\",\"initialValue\":[],\"in\":{\"$concatArrays\":[\"$$value\",\"$$this\"]}}}}}],\"cursor\":{}}";
		request.getChildren( JmdbApplication.QUERY ).add( 
		  Value.create(sleeplog_query)
		 );
		
		try {
		  Value response = new JmdbApplication().query( request );
		  System.out.println( response.getFirstChild( JmdbApplication.RESULT ).strValue() );
		System.out.println( response.getFirstChild( JmdbApplication.QUERY_TIME ).longValue() );
		} catch (FaultException e) {
		  // TODO Auto-generated catch block
		  e.printStackTrace();
		}
		
	  }

	  @Test
	  public void testDrop() {
    
		Value request = Value.create();
		request.getFirstChild( JmdbApplication.DATABASE ).setValue( "db" );
	
		request.getChildren( JmdbApplication.COLLECTION ).add( 
		  Value.create( "collection_0_biometric" )
		);
	
		request.getChildren( JmdbApplication.COLLECTION ).add( 
		  Value.create( "collection_0_sleeplog" )
		);
	
		try {
		  Value response = new JmdbApplication().drop( request );
		  System.out.println( response.getFirstChild( JmdbApplication.QUERY_TIME ).longValue() );
		} catch (FaultException e) {
		  // TODO Auto-generated catch block
		  e.printStackTrace();
		}
	  }

}
