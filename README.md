# Java Service MongoDB Drivers for Jolie

The library wraps two operations of the MongoDB framework (version `4.2.3`) and allows to embed those as a **Microservice** in a [Jolie](www.jolie-lang.org) program.

The operations are:

1. `query` - it allows to insert data in a collection and query those by using the `runCommand` for MongoDB;
2. `drop`- it allows to drop a list of collections.

## Requirements

For the complete list of dependencies refer to the ['pom.xml'](pom.xml) file.

To compile the project, I used the following:

1. Target `OpenJDK Runtime Environment AdoptOpenJDK-11.0.11+9 (build 11.0.11+9)`
2. Built using `Apache Maven 3.8.1`

## Installation

To install the drivers use `jpm`available via `npm install -g jpm` using `Node`:

`jpm add szingaro/jolie-mongodb-driver`

## Usage

Insert at the beginning of your Jolie service the following:

`from szingaro.jolie-mongodb-driver.main import MongoDBDriver`
