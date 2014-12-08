namespace java com.splout.db.thrift

exception DNodeException {
  1: i32 code,
  2: string msg
}

struct PartitionMetadata {
  1: string minKey,
  2: string maxKey,
  3: i32 nReplicas,
  4: i64 deploymentDate,
  5: optional list<string> initStatements,
  6: optional string engineId
}

struct DeployAction {
  1: string tablespace,
  2: i64 version,
  3: string dataURI,
  4: i32 partition,
  5: PartitionMetadata metadata
}

struct RollbackAction {
  1: string tablespace,
  2: i64 version
}

struct TablespaceVersion {
  1: string tablespace,
  2: i64 version
}

service DNodeService {

	binary binarySqlQuery(1:string tablespace, 2:i64 version, 3:i32 partition, 4:string query) throws (1:DNodeException excep)
	string sqlQuery(1:string tablespace, 2:i64 version, 3:i32 partition, 4:string query) throws (1:DNodeException excep)
	string deleteOldVersions(1:list<TablespaceVersion> versions) throws (1:DNodeException excep)
	string deploy(1:list<DeployAction> deployActions, 2:i64 version) throws (1:DNodeException excep)
	string rollback(1:list<RollbackAction> rollbackActions, 2:string distributedBarrier) throws (1:DNodeException excep)
	string status() throws (1:DNodeException excep)
	string abortDeploy(1:i64 version) throws (1:DNodeException excep)
	string testCommand(1:string command) throws (1:DNodeException excep)
}