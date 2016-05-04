
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RecordBasedFileManager rbfm;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

RelationManager* RelationManager::instance()
{
  if(!_rm)
    _rm = new RelationManager();

  return _rm;
}


/*




Tables
(1, "Tables", "Tables")
(2, "Columns", "Columns") (3, "Employee", "Employee")
Columns
(1, "table-id", TypeInt, 4 , 1)
(1, "table-name", TypeVarChar, 50, 2) (1, "file-name", TypeVarChar, 50, 3) (2, "table-id", TypeInt, 4, 1)
(2, "column-name", TypeVarChar, 50, 2) (2, "column-type", TypeInt, 4, 3)
(2, "column-length", TypeInt, 4, 4)
(2, "column-position", TypeInt, 4, 5) (3, "empname", TypeVarChar, 30, 1)
(3, "age", TypeInt, 4, 2)
(3, "height", TypeReal, 4, 3)
(3, "salary", TypeInt, 4, 4)


  Tables (table-id:int, table-name:varchar(50), file-name:varchar(50))
Columns(table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)

*/

RelationManager::RelationManager()
{

  vector<Attribute> table_descriptor;    // table vector holds table-id:int, table-name:varchar(50), file-name:varchar(50)
  vector<Attribute> column_descriptor;   // column vector holds table-id:int, column-name:varchar(50), column-type:int, column-length:int, column-position:int)


  //Initialize all the table attributes//
Attribute table_id;
table_id.name = "table-id";
table_id.type = TypeInt;
table_id.length = INT_SIZE;

Attribute table_name;
table_name.name = "table-name";
table_name.type = TypeVarChar;
table_name.length = 50;

Attribute table_fileName;
table_fileName.name = "file-name";
table_fileName.type = TypeVarChar;
table_fileName.length = 50; 

  //Initialize all the column attributes//
Attribute column_table_id;
column_table_id.name = "table-id"; 
column_table_id.type = TypeInt;
column_table_id.length = INT_SIZE;

Attribute column_name;
column_name.name = "column-name";
column_name.type = TypeVarChar;
column_name.length = 50;

Attribute column_type;
column_type.name = "column-type";
column_type.type = TypeInt;
column_type.length = INT_SIZE;

Attribute column_length;
column_length.name = "column-length";
column_length.type = TypeInt;
column_length.length = INT_SIZE;

Attribute column_position;
column_position.name = "column-position";
column_position.type = TypeInt;
column_position.length = INT_SIZE;

  //push into vector table_descriptor all of the attributes//
table_descriptor.push_back(table_id);
table_descriptor.push_back(table_name);
table_descriptor.push_back(table_fileName);

  //push into vector column_descriptor all of the attributes//
column_descriptor.push_back(column_table_id);
column_descriptor.push_back(column_name);
column_descriptor.push_back(column_type);
column_descriptor.push_back(column_length);
column_descriptor.push_back(column_position);
}

RelationManager::~RelationManager()
{
}

RC RelationManager::createCatalog()
{
  return -1;
}

RC RelationManager::deleteCatalog()
{
  return -1;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
  int rc=-1;
  rc=rbfm->createFile(tableName);
  if (rc=0) {
      //create data using tableCount, "tableName", "TableName"
    const void *data;
    int32_t offset_in_data;
    int8_t nullind=0;
    //put null indicator into data
    memcpy(data, &nullind, 1);
    offset_in_data++;
    //Scan through tables to find the maximum existing table number
    RBFM_ScanIterator rbfmsi;
    FileHandle handle;
    rc=_pf_manager->openFile(tableName.c_str(), handle));
    vector<string> attrs;
    attrs.push_back("table-id");
    /*RC rc = rbfm->scan(handle, "table-id", NO_OP, NULL, attrs, rmsi);
    RID rid;
    void *returnedData = malloc(2000);
    int32_t maxTable=0;
    int32_t currentTable;
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF) {
      memcpy(&currentTable, returnedData+sizeof(int8_t), sizeof(int32_t));
      if (currentTable > maxTable) {
        maxTable=currentTable;
      }
    }
    rmsi.close();*/ //translate to rbfm scanner
    maxTable++;
    //put first field, which is the table number, into data. 
    memcpy(data+offset_in_data, &maxTable, sizeof(int32_t));
    offset_in_data+=sizeof(int32_t);
    //next field
    int32_t nameSize = tableName.length();
    memcpy(data+offset_in_data, &nameSize, sizeof(nameSize);
    offset_in_data+=sizeof(NameSize);
    memcpy(data+offset_in_data, tableName.c_str(), nameSize);
    offset_in_data+=nameSize);
    //next field
    memcpy(data+offset_in_data, &nameSize, sizeof(nameSize);
    offset_in_data+=sizeof(NameSize);
    memcpy(data+offset_in_data, tableName.c_str(), nameSize);
    offset_in_data+=nameSize);

    RID rid =0;
    rc=insertTuple("Tables", data, rid);
    int columnCount=1;
    for (attr : attrs) {
         //create data using the same number as above, attr->name, attr->type, attr->length, columnCount
      RID rid =0;
      rc=insertTuple("Columns", data, rid);
      columnCount ++;
    }
  }
  return rc;
}

RC RelationManager::deleteTable(const string &tableName)
{
  return -1;
}

bool RelationManager::file_exists(string &tableName){
  if (FILE *file = fopen(tableName.c_str(), "r")) {
    fclose(file);
    return true;
  } else {
    return false;
  }   
}
/*


For handling the file io, you don't need to manually mess with FILE*s anymore. Instead, you can use RBFM's openFile() method, and it will handle all of the lower level stuff for you.

Check out rm_test12.cc for an example of how to use the iterator. Essentially, you make an iterator, you initialize it with scan, and then you use getNextTuple() in a loop to iterate over it.

The general flow of this method will be:

1. Get the tableID of the table with the name tableName (you may want to make a helper function for this).
2. Make a file handle and use rbfm.openFile() to open the file holding the columns table
3. Make an RBFM_ScanIterator and use rbfm.scan() to initialize it. Point it at the columns table and use the tableID you got in step one. It may be helpful to hardcode the recordDescriptor vector for the columns table.
4. Repeatedly call the getNextRecord() method of your iterator to read in all of the columns with a tableID equal to the one you got in step 1.
5. Take all of the columns you get this way and convert this into Attributes and push them onto the back of the attrs struct
6. Clean up the files and iterators you've opened

*/

int get_tableId(tableName){

  String name = tableName; // --> convert tableName to data format with null-indicator at end of string // 
  FileHandle handle;       // --> rbfm FileHandle handle = rbfm -> openFile(table_descriptor);
  RC h = openFile(table_descriptor, handle);
  assert(h==0 && "openFile Success");
  rbfm->RBFM_ScanIterator rmsi;

  RC rc = rbfm->scan(handle, table_descriptor, "table-name", EQ_OP, name, rmsi);
  //allocate memory to data_pointer for getNextRecord
  RID id = 0;
  RC result =  rmsi.getNextRecord(id, data_pointer);
  return data_pointer; //parse result in data_pointer --> will return bytes (int is 4 bytes long) --> ask alix//
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{

  int tid = get_tableId(tableName);
  String colName = col
  const vector<Attribute> td = table_descriptor;
  FileHandle handle;
  RM_ScanIterator rmsi;
  RC iterator = scan(handle, recordDescriptor, "tableName", comp, result, rmsi);

  int tableId = 0;
  rc=-1;
  FileHandle handle; 
  const vector<Attribute> recordDescriptor = attrs;
  const vector<string> result = NULL;
  rc =rbfm->openFile(tableName.c_str(), handle);  
  void comp;

  //after getting table_id do a scan but with the columns_table and table_id//

  int columnCount = 0;
  // need scan_iterator//
  RC iterator = scan(handle, recordDescriptor, "tableName", comp, result, rmsi);

  assert(iterator == success && "RelationManager::scan() should not fail.");

  if(file_exists(tableName)==true){
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF){
      RC v = iterator.getNextRecord(tableId, handle);
      recordDescriptor = (Attributes)v;
    }
    return recordDescriptor;
  }
  cout<<"Error tableName does not exist" <<endl;
  return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
  rc=-1;
  FileHandle handle;
  rc=_pf_manager->openFile(tableName.c_str(), handle));
if(rc=0){
  RID rid=0;
  vector<Attribute> recordDiscriptor;
  getAttributes(tableName, recordDiscriptor);
  rc=rbfm->insertRecord(handle, recordDescriptor, data, rid);
}
return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
  return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
  FileHandle update;
  FILE * updateTuple = fopen(tables.tbl.c_str(), "r+");

  vector<Attributes> tableAttrs;
  return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
  rc= -1;
  FileHandle handle;
  rc=_pf_manager->openFile(tableName.c_str(), handle));
if(rc=0){
  vector<Attribute> recordDiscriptor;
  getAttributes(tableName, recordDiscriptor);
  rc=rbfm->readRecord(handle, crecordDescriptor, rid, data);
  return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
	return -1;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
  return -1;
}

RC RelationManager::scan(const string &tableName,
  const string &conditionAttribute,
  const CompOp compOp,                  
  const void *value,                    
  const vector<string> &attributeNames,
  RM_ScanIterator &rm_ScanIterator)
{
  int rc=-1;
  FileHandle handle;
  rc=_pf_manager->openFile(tableName.c_str(), handle));
if(rc=0){
  vector<Attribute> recordDiscriptor;
  getAttributes(tableName, recordDiscriptor);
  rc=rbfm->scan(handle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
}  
return rc;
}
}

RC RelationManager::createDataForTables(int32_t table_id, const string &tableName, const void *data){
    int32_t offset_in_data;
    int8_t nullind=0;
    //put null indicator into data
    memcpy(data, &nullind, 1);
    offset_in_data++;
    //put first field, which is the table number, into data. 
    memcpy(data+offset_in_data, &table_id, sizeof(int32_t));
    offset_in_data+=sizeof(int32_t);
    //next field, table name
    int32_t nameSize = tableName.length();
    memcpy(data+offset_in_data, &nameSize, sizeof(nameSize);
    offset_in_data+=sizeof(NameSize);
    memcpy(data+offset_in_data, tableName.c_str(), nameSize);
    offset_in_data+=nameSize);
    //next field, name again
    memcpy(data+offset_in_data, &nameSize, sizeof(nameSize);
    offset_in_data+=sizeof(NameSize);
    memcpy(data+offset_in_data, tableName.c_str(), nameSize);
    offset_in_data+=nameSize);
}

RC RelationManager::createDataForColumns(int32_t table_id, const string &columnName, int32_t type, int32_t length, int32_t position, const void *data) {
    int32_t offset_in_data;
    int8_t nullind=0;
    //put null indicator into data
    memcpy(data, &nullind, 1);
    offset_in_data++;
    //put first field, which is the table number, into data. 
    memcpy(data+offset_in_data, &table_id, sizeof(int32_t));
    offset_in_data+=sizeof(int32_t);
    //next field, column name
    int32_t nameSize = columnName.length();
    memcpy(data+offset_in_data, &nameSize, sizeof(nameSize);
    offset_in_data+=sizeof(NameSize);
    memcpy(data+offset_in_data, tableName.c_str(), nameSize);
    offset_in_data+=nameSize);
    //next field

}



