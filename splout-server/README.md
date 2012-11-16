Splout SQL Server
=================

The Splout SQL Server implements the REST API for being able to serve SQL queries and deploy or rollback an arbitrary number of *tablespaces*. The server is made up by two services: the *DNode* and the *QNode*. The *DNode* is in charge of communicating directly with the database files whereas the *QNode* implements the REST API and delegates queries to the appropriate *DNode*s.

