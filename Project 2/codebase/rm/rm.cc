
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
	FileHandle fileHandle;
	RID rid;
	vector<Attribute> tableAttr = GenerateTablesAttr();
	vector<Attribute> columnAttr = GenerateColumnsAttr();

	char* tableTableData = (char*)malloc(TABLE_DATA_SIZE);
	char* columnTableData = (char*)malloc(TABLE_DATA_SIZE);
	RC rc;
  unsigned int table_id = 1;
  unsigned int column_id = 2;

  string tableName = "Tables";
  string columnName = "Columns";

	memcpy(tableTableData, &table_id, 4);
	memcpy(tableTableData + 4, tableName, 50);
	memcpy(tableTableData + 4 + 50, tableName, 50);

	memcpy(columnTableData, &column_id, 4);
	memcpy(columnTableData + 4, columnName, 50);
	memcpy(columnTableData + 4 + 50, columnName, 50);

	if (rbfm.createFile("Tables") != SUCCESS)
	{
		return RBFM_CREATE_FAILED;
	}

	if (rbfm.openFile("Tables", fileHandle) != SUCCESS)
	{
		return RBFM_OPEN_FAILED;
	}

	rc = rbfm.insertRecord(fileHandle, tableAttr, tableTableData, rid);
	if (rc != SUCCESS)
	{
		return rc;
	}
	rc = rbfm.insertRecord(fileHandle, tableAttr, columnTableData, rid);
	if (rc != SUCCESS)
	{
		return rc;
	}
	rc = rbfm.closeFile(fileHandle);
	if (rc != SUCCESS)
	{
		return rc;
	}

	if (rbfm.createFile("Columns") != SUCCESS)
	{
		return RBFM_CREATE_FAILED;
	}

	if (rbfm.openFile("Columns", fileHandle) != SUCCESS)
	{
		return RBFM_OPEN_FAILED;
	}

	for (int i = 0; i<8; i++)
	{
		rc = rbfm.insertRecord(fileHandle, columnAttr, StartingCatalogInfo(i), rid);

		if (rc != SUCCESS)
		{
			return rc;
		}
	}

	rc = rbfm.closeFile(fileHandle);
	if (rc != SUCCESS)
	{
		return rc;
	}

	return SUCCESS;
}
 /* delete the "Table" and "Column" tables */

RC PagedFileManager::destroyFile(const string &fileName)
{
  if(!file_exists(fileName)){
    cout<< "Error: File does not exist";
    return -1;
  }else{
    remove(fileName.c_str());
    return 0;
  }
}

RC RelationManager::deleteCatalog()
{
  if(PagedFileManager->destroyFile(&Tables)!= SUCCESS||PagedFileManager->destroyFile(&table_descriptor)!= SUCCESS||PagedFileManager->destroyFile(&column_descriptor)){
    return -1;
  }
  return SUCCESS;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{
  int rc=-1;
  rc=rbfm->createFile(tableName);
  if (rc=0) {
    const void *data;
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
    RID rid =0;
    rc= createDataForTables(maxTable, &tableName, data);
    rc=insertTuple("Tables", data, rid);
    int columnCount=1;
    for (attr : attrs) {
      const void *data;
      createDataForColumns(maxTable, attr->name, attr->type, attr->length, columnCount, data);
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
  char null_indicator = 0;
  int32_tlen = tableName.length();

  const vector<Attribute> nameVector;
  String name = tableName.c_str(); // --> convert tableName to data format with null-indicator at end of string // 
  nameVector = name;

  tN = malloc(55);
  memcpy(tN, &null_indicator, 1);
  memcpy(tN + 1, &int32_tlen, 1);
  memcpy(tN + 5, tableName.c_str(), tableName.length());

  FileHandle handle;       // --> rbfm FileHandle handle = rbfm -> openFile(table_descriptor);
  tD = openFile("table", handle);
  RC h = openFile(table_descriptor, handle);
  assert(h==0 && "openFile Success");
  rbfm->RBFM_ScanIterator rmsi;

  RC rc = rbfm->scan(handle, table_descriptor, "table-name", EQ_OP, tN, nameVector, rmsi);
  //allocate memory to data_pointer for getNextRecord
  RID id = 0;
  data_pointer =  malloc(55);

  RC r =  rmsi.getNextRecord(id, data_pointer);
 /*
  int tid; 
  memcpy(&tid, data_pointer + 1, 4);
  return tid; 
 */

  return data_pointer;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{

  FileHandle handle;
  int rid = get_tableId(tableName);
  RID = 1;
  data = malloc(PAGE_SIZE);
  int i = 0;
  RC colVal = openFile("column", handle);                         /* create catalog should hold column values == column*/
  while(rmsi.getNextRecord(RID, data) != RM_EOF){             // use malloc to get memory PAGE_SIZE bytes of memory //
    string col_name = colVal.at(i).name;                           /* create attrbiute with type name, type, length*/
    unsigned char* col_type = new unsigned char[66];               /* pushBack() attribute into vector attr*/ //(, "empname", TypeVarChar, 30,3 1)

    attrs.push_back();                                             // put colVal attributes into vector attr //
    attrs.push_back();                                             // parse attributes before putting in attr//
    attrs.push_back();                                
  }

  const vector<Attribute> td = table_descriptor;
  
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

  if(iterator == SuccessCESS)
    cout << "RelationManager::scan() should not fail." << endl;

  if(file_exist(tableName.c_str())==true){
    while(rmsi.getNextTuple(rid, returnedData) != RM_EOF){
      //DONT think this is right  
      //RC v = iterator.getNextRecord(tableId, handle);
      //recordDescriptor = (Attributes)v;
    }
    return recordDescriptor;
  }
  cout<<"Error tableName does not exist" <<endl;
  return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
  RC rc=-1;
  FileHandle handle;
  rc=rbfm.openFile(tableName.c_str(), handle);
if(rc=0){
  RID rid;
  vector<Attribute> recordDescriptor;
  getAttributes(tableName, recordDescriptor);
  rc=rbfm.insertRecord(handle, recordDescriptor, data, rid);
}
return rc;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
  RC rc;
  FileHandle filehandle;
  vector<Attribute> attrs;
  if(getAttributes(tableName, attrs) != SUCCESS)
    return RBFM_OPEN_FAILED;

  if(rbfm.openFile(tableName, filehandle) != SUCCESS)
    return RBFM_OPEN_FAILED;
  
  if(rbfm.deleteRecord(filehandle, attrs, rid) != SUCCESS)
    return RBFM_WRITE_FAILED;

  if(rbfm.closeFile(filehandle) != SUCCESS)
    return RBFM_OPEN_FAILED;

  return SUCCESS;
}

/*
This method updates a tuple identified by the specified rid. Note: if the tuple grows (i.e., the size of the tuple increases) and there 
is no space in the page to store the tuple (after the update), then the tuple is migrated to a new page with enough free space. Since you
 will implement an index structure (e.g., B-tree) in Project 3, tuples are identified by their rids and when they migrate, you must 
 leave a forwarding address behind identifying the new location of the tuple. Also, each time a tuple is updated and becomes smaller, 
 you need to compact the underlying page. That is, keep the free space in the middle of the page -- the slot table should be at one 
 end of the page, the tuple data area should be at the other end, and the free space should be in the middle. Again, the structure 
 for *data is the same as for insertRecord().
Return an error if a table called tableName does not exist. Also return a (different) error if there is no tuple with the specified rid.
updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid)
*/
RC RelationManager::updateTuple(const string &tableName, void *data, const RID &rid)
{
  vector<Attribute> recordDiscriptor;
  FileHandle handle;
  getAttributes(tableName, recordDiscriptor);
  RC rc=rbfm.readRecord(handle, recordDiscriptor, rid, data);
  RC update_tuple = rbfm.openFile(tableName.c_str(), handle);
  vector <Attribute> descriptor;
  getAttributes(tableName, descriptor);
  if(updateRecord(handle, handle, descriptor, data, rid) == SUCCESS){
    return 1;
  }
  return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data){
  rc= -1;
  FileHandle handle;
  rc=_pf_manager->openFile(tableName.c_str(), handle));
if(rc=0){
  vector<Attribute> recordDiscriptor;
  getAttributes(tableName, recordDiscriptor);
  rc=rbfm->readRecord(handle, recordDiscriptor, rid, data);
}
return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data){
  rbfm.printRecord(attrs, data);
	return SUCCESS;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data){
  FileHandle handle;
  RC rc = openFile(tableName.c_str(), handle);

  
  const vector<Attribute> descriptor;                     
  if (getAttributes(tableName, descriptor) == SUCCESS){                              /* gets the attributes from the table */
    RC x = rbfm->readAttribute(handle, descriptor, rid, attributeName, data);              /* utilizes RBFM-> getAttribute() */
  }
return -1;
}

RC RelationManager::scan(const string &tableName,
  const string &conditionAttribute,
  const CompOp compOp,                  
  const void *value,                    
  const vector<string> &attributeNames,
  RM_ScanIterator &rm_ScanIterator){
  int rc=-1;
  FileHandle handle;
  rc=_pf_manager.openFile(tableName.c_str(), handle));
if(rc=0){
  vector<Attribute> recordDescriptor;
  getAttributes(tableName, recordDescriptor);
  rc=rbfm.scan(handle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rm_ScanIterator);
}  
return rc;
}

RC RelationManager::createDataForTables(int32_t table_id, const string &tableName, void *data){
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
  memcpy(data+offset_in_data, &nameSize, sizeof(nameSize));
    offset_in_data+=sizeof(nameSize);
    memcpy(data+offset_in_data, tableName.c_str(), nameSize);
    offset_in_data+=nameSize;
    //next field, name again
  memcpy(data+offset_in_data, &nameSize, sizeof(nameSize));
    offset_in_data+=sizeof(nameSize);
    memcpy(data+offset_in_data, tableName.c_str(), nameSize);
    offset_in_data+=nameSize;
}

RC RelationManager::createDataForColumns(int32_t table_id, const string &columnName, int32_t type, int32_t length, int32_t position, void *data) {
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
  memcpy(data+offset_in_data, &nameSize, sizeof(nameSize));
    offset_in_data+=sizeof(nameSize);
    memcpy(data+offset_in_data, columnName.c_str(), nameSize);
    offset_in_data+=nameSize;
    //next field, type
  memcpy(data+offset_in_data, &type, sizeof(int32_t));
  offset_in_data+=sizeof(int32_t);
    //next field, length
  memcpy(data+offset_in_data, &length, sizeof(int32_t));
  offset_in_data+=sizeof(int32_t);
    //next field, position
  memcpy(data+offset_in_data, &position, sizeof(int32_t));
  return 0;
}


vector<Attribute> RelationManager::GenerateTablesAttr(){
  vector<Attribute> tableAttributes;

  Attribute table_id;
  Attribute table_name;
  Attribute file_name;

  table_id.name = "table-id";
  table_id.type = TypeInt;
  table_id.length = 4;

  table_name.name = "table-name";
  table_name.type = TypeVarChar;
  table_name.length = 50;

  file_name.name = "file-name";
  file_name.type = TypeVarChar;
  file_name.length = 50;

  tableAttributes.push_back(table_id);
  tableAttributes.push_back(table_name);
  tableAttributes.push_back(file_name);

  return tableAttributes;
}
vector<Attribute> RelationManager::GenerateColumnsAttr(){
  vector<Attribute> columnAttributes;

  Attribute table_id;
  Attribute column_name;
  Attribute column_type;
  Attribute column_length;

  table_id.name = "table-id";
  table_id.type = TypeInt;
  table_id.length = 4;

  column_name.name   = "column-name";
  column_name.type   = TypeVarChar;
  column_name.length = 50;

  column_type.name   = "column-type";
  column_type.type   = TypeInt;
  column_type.length = 4;

  column_length.name   = "column-length";
  column_length.type   = TypeInt;
  column_length.length = 4;

  columnAttributes.push_back(table_id);
  columnAttributes.push_back(column_name);
  columnAttributes.push_back(column_type);
  columnAttributes  .push_back(column_length);

  return columnAttributes;
}

int RelationManager::file_exist (char *filename)
{
  ifstream ifile(filename);

  if(ifile)
  {
    return 0;
  }
  else
  {
    return 1;
  }
}

void* RelationManager::StartingCatalogInfo(unsigned int i)
{
  void* columnData = (void*)malloc(COLUMN_DATA_SIZE);
  unsigned int table_id;
  string column_name; 
  unsigned int column_type;
  unsigned int column_length;
  unsigned int column_position;

  switch(i)
  {
    case 0: table_id =1;
            column_name = "table-id";
            column_type = TypeInt;
            column_length = 4;
            column_position = 1;
            break;
    case 1: table_id =1;
            column_name = "table-name";
            column_type = TypeVarChar;
            column_length = 50;
            column_position = 2;
            break;
    case 2: table_id =1;
            column_name = "file-name";
            column_type = TypeVarChar;
            column_length = 50;
            column_position = 3 ;
            break;
    case 3: table_id =2;
            column_name = "table-id";
            column_type = TypeInt;
            column_length = 4;
            column_position = 1;
            break;
    case 4: table_id =2;
            column_name = "column-name";
            column_type = TypeVarChar;
            column_length = 50;
            column_position = 2;
            break;
    case 5: table_id =2;
            column_name = "column-length";
            column_type = TypeInt;
            column_length = 4;
            column_position = 3;
            break;
    case 6: table_id =2;
            column_name = "column-length";
            column_type = TypeInt;
            column_length = 4;
            column_position = 4;
            break;
    case 7: table_id =2;
            column_name = "column-position";
            column_type = TypeInt;
            column_length = 4;
            column_position = 5;
            break;
    }

    memcpy(columnData,    &table_id, 4);
    memcpy(columnData+4,  column_name.c_str(), 50);
    memcpy(columnData+54, &column_type, 4);
    memcpy(columnData+58, &column_length, 4);    
    memcpy(columnData+62, &column_position, 4);
  }