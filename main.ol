/*****************************************************************************
 *  Copyright (C) 2018 by Larisa Safina <safina@imada.sdu.dk>                *
 *  Copyright (C) 2018 by Stefano Pio Zingaro <stefanopio.zingaro@unibo.it>  *
 *  Copyright (C) 2019 by Saverio Giallorenzo <saverio.giallorenzo@gmail.com>*
 *                                                                           *
 *  This program is free software; you can redistribute it and/or modify     *
 *  it under the terms of the GNU Library General Public License as          *
 *  published by the Free Software Foundation; either version 2 of the       *
 *  License, or (at your option) any later version.                          *
 *                                                                           *
 *  This program is distributed in the hope that it will be useful,          *
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of           *
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the            *
 *  GNU General Public License for more details.                             *
 *                                                                           *
 *  You should have received a copy of the GNU Library General Public        *
 *  License along with this program; if not, write to the                    *
 *  Free Software Foundation, Inc.,                                          *
 *  59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.                *
 *                                                                           *
 *  For details about the authors of this software, see the AUTHORS file.    *
 *****************************************************************************/

type QueryRequest      : void {
  database             : string
  collection*          : void {
    name               : string
    data*              : undefined
  }
  query*               : string
}

type QueryResponse    : void {
  result*              : undefined
  queryTime?           : long
}

type DropRequest       : void {
  database             : string
  collection*          : string
}


type DropResponse    : void {
  queryTime?           : long
}

interface MongoDBDriverInterface {
  RequestResponse: 
    query( undefined )( QueryResponse ) throws MongoConnectionException( string ),
    drop( undefined )( DropResponse ) throws MongoConnectionException( string )
}

service MongoDBDriver {
  inputPort IP {
    location: "local"
    interfaces: MongoDBDriverInterface
  }  

  foreign java {
    class: "org.jolie.driver.DriverService"
  }
}
