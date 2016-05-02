
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

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
  FileHandle handle;
  rc=-1; 
  const vector<Attribute> descriptor = attrs;

/*
  i need the scan() and the iterator right here. I can scan through and iterate through
*/
  if(file_exists(tableName)==true){
    rbfm->openFile(fileName, handle);
    RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
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



