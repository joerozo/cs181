#ifndef _rm_h_
#define _rm_h_

#include <iostream>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>
#include <fstream> 
#include <assert.h>
#include "../rbf/rbfm.h"

using namespace std;
#define TABLE_DATA_SIZE 104
#define COLUMN_DATA_SIZE 66
# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};
  RBFM_ScanIterator rbfm_iterator;
  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) { return RM_EOF; };
  RC close() { return -1; };
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();

  RecordBasedFileManager rbfm;
  
  //Alec
  RC createCatalog();

  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  //Table
  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  //Alec
  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple( string &tableName, const RID &rid, void *data);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  //Alec
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);


protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  vector<Attribute> table_descriptor; 
  vector<Attribute> column_descriptor;
  RC createDataForTables(int table_id, const string &tableName, void *data);
  RC createDataForColumns(int table_id, const string &columnName, int type, int length, int position, void *data);
  vector<Attribute> GenerateTablesAttr();
  vector<Attribute> GenerateColumnsAttr();
  RC insertTablesToCatalog();
  RC insertColumnsToCatalog();
  int file_exist (char *filename);

  void* StartingCatalogInfo(unsigned int i);
};

#endif
