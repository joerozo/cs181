
#include "rm.h"

RelationManager* RelationManager::_rm = 0;

RecordBasedFileManager rbfm;

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
    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{
    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
  if(tableName.compare(tables.tbl)==0||tableName.compare(columns.tbl)==0){ // tables.tbl should refer to current instantiation of table 
    attrs =  table.tbl.attributes ; // attrs needs to be set to current tables' attributes --> idk if I am doing this right?
    cout<< "Successfully Returned Attributes";
    return 1;
  }else{
    return -1;
  }

  // Need to iterate through columns
  FileHandle fh;
  FILE * table_file = fopen(tables.tbl.c_str(), "r+");

  if(table_file == NULL) {
    cout << "Failed to open file" << endl;
  }
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    return -1;
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
    return -1;
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
    return -1;
}



