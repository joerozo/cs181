
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

RelationManager::RelationManager()
{
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
      //scan through Tables to get number of this table
      //create data using that number, "tableName", "TableName"
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
RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
  rc=-1;
  FileHandle handle; 
  const vector<Attribute> recordDescriptor = attrs;
  const vector<string> result = NULL;
  rc=_pf_manager->openFile(tableName.c_str(), handle));  
  void comp;

  int columnCount = 0;
  // need scan_iterator//
  iterator = scan(handle, recordDescriptor, "tableName", comp, result, scan_iterator);
  if(tuple_result == NULL){
    cout<< tableName << "Was not valid" << endl;
  }

  

  const void iterator =  scan(handle, );
  if(file_exists(tableName)==true){
    while (tableName != EOF){
      recordDescriptor = 
    }


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
  RBFM_ScanIterator rbfm_ScanIterator;
  rc=rbfm->scan(handle, recordDescriptor, conditionAttribute, compOp, value, attributeNames, rbfm_ScanIterator);
}  
return rc;
}



