
#include "rm.h"
#include <fstream>
#include <iostream>
#include <cstring>
#include <bitset>
using namespace std;

RelationManager* RelationManager::_rm = 0;

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

  _rm.createFile("tables.tbl");
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

/*
This method gets the attributes (attrs) of the table called tableName by looking in the catalog tables. Return
an error if a table called tableName does not exist.
*/
/* 
Assume Tables.tbl and Col.tbl
are the constant instance variables
rbfm: readAttribute
rc: deteleCatalog
rc: getAttributes
rc: updateTuple
rc: readAttribute 
*/
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
/*
This method mainly exists for debugging purposes. This method reads a specific attribute of a tuple
identified by a given rid. The structure for *data is the same as for insertRecord(). That is, a null-indicator
will be placed in the beginning of *data. However, for this function, since it returns a value for just one
attribute, exactly one byte of null-indicators should be returned, not a set of the null-indicators for all of the
tuple's attributes.
Return an error if a table called tableName does not exist. Also return a (different) error if there is no tuple
with the specified rid.
*/

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



