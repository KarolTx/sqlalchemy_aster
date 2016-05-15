# sqlalchemy_aster

ALPHA support of Aster
* based on MS Access sample and multiple PostgreSQL and Oracle dialects
* the dialect "JDBC" needs some improvement -> so that "/?jar=" clause can be specified as parameter for create_engine and doesn't have to be part of the engine connection string
* no automatic limit clause or internal limitations are set -> beware of the size of requested data set
* mainly SELECTs have been tested
* ORM is not tested
* ORM-table creation does not work -> adding the replication or distribute clause is not functional at all
* automatic object creation from Aster metadata is not implemented -> needs somebody with the knowledge and time
