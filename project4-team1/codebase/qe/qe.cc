
#include "qe.h"

Filter::Filter(Iterator* input, const Condition &condition) : iter(input) // calling copy consturtor
{
	//cleaning attributes
	attrs.clear();

	//extracting conditions	
	op         = condition.op;
	this->data = condition.rhsValue.data;
	this->type = condition.rhsValue.type;

	//getting attributes
	this->iter->getAttributes(attrs);

	for(unsigned i = 0; i < attrs.size(); i++)
	{
		if(attrs[i].name == condition.lhsAttr)
		{
			pos = i;
			value = malloc(attrs[i].length + sizeof(int));
			break;
		}
	}
}

Filter::~Filter()
{
	free(value);
}

RC Filter::getNextTuple(void *data)
{
	RC result;
	do
	{
		result = iter->getNextTuple(data);
		if(result != SUCCESS)
		{
			return result;
		}

		ReadTupleField(data, this->value, attrs, pos, type);
	}while(!Compare(this->value, data, type, op));
	
	return result;
}


void Filter::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	attrs = this->attrs;
}

void Filter::ReadTupleField(void * inputData, void *outputData, vector<Attribute> attrs, 
							int pos, AttrType type)
{
	unsigned int offset = 0;
	unsigned int attrLength = 0;

	for (int i = 0; i < pos; i++) 
	{
		if (attrs[i].type == TypeInt)
			offset += sizeof(int);
		else if (attrs[i].type == TypeReal)
			offset += sizeof(float);
		else 
		{
			int stringLength = *(int *) ((char *) inputData + offset);
			offset += sizeof(int) + stringLength;
		}
	}

	if (type == TypeInt) {
		attrLength = sizeof(int);
	} else if (type == TypeReal) {
		attrLength = sizeof(float);
	} else {
		attrLength = *(int *) ((char *) inputData + offset) + sizeof(int);
	}

	memcpy(outputData, (char *) inputData + offset, attrLength);

}

bool Filter::Compare(const void *attribute, const void *condition, AttrType type, CompOp op)
{
	int attrInt = 0;
	int condInt = 0;
	float attrFloat = 0.0;
	float condFloat = 0.0;
	int attributeLength = 0;
	int conditionLength = 0;
	if(condition == NULL)		
		return true;

	bool result = true;

	switch(type)
	{
		case TypeInt:
			attrInt = *(int*)attribute;
			condInt = *(int*)condition;
			switch(op)
			{
				case EQ_OP:
					result = (attrInt == condInt);
					break;
				case LT_OP:
					result = (attrInt < condInt);
					break;			
				case GT_OP:
					result = (attrInt > condInt);
					break;
				case LE_OP:
					result = (attrInt <= condInt);
					break;			
				case GE_OP:
					result = (attrInt >= condInt);
					break;			
				case NE_OP:
					result = (attrInt != condInt);
					break;		
				case NO_OP:
					break;
			}
			break;
		case TypeReal:
			attrFloat = *(float *) attribute;
			condFloat = *(float *) condition;

			switch(op)
			{
				case EQ_OP:
					result = (attrFloat == condFloat);
					break;
				case LT_OP:
					result = (attrFloat < condFloat);
					break;			
				case GT_OP:
					result = (attrFloat > condFloat);
					break;
				case LE_OP:
					result = (attrFloat <= condFloat);
					break;			
				case GE_OP:
					result = (attrFloat >= condFloat);
					break;			
				case NE_OP: 
					result = (attrFloat != condFloat);
					break;		
				case NO_OP:
					break;
			}
			break;
		
		case TypeVarChar:
			attributeLength = *(int *) attribute;
			string attrStr;
			attrStr.assign((char*)attribute + sizeof(int), 0, attributeLength);
			conditionLength = *(int *) condition;
			string condStr;
			condStr.assign((char*)condition + sizeof(int),0 ,conditionLength);

			switch(op)
			{
				case EQ_OP:
					result = strcmp(attrStr.c_str() , condStr.c_str()) == 0;
					break;
				case LT_OP:
					result = strcmp(attrStr.c_str() , condStr.c_str()) < 0;
					break;			
				case GT_OP:
					result = strcmp(attrStr.c_str() , condStr.c_str()) > 0;
					break;
				case LE_OP:
					result = strcmp(attrStr.c_str() , condStr.c_str()) <= 0;
					break;			
				case GE_OP:
					result = strcmp(attrStr.c_str() , condStr.c_str()) >= 0;
					break;			
				case NE_OP:
					result = strcmp(attrStr.c_str() , condStr.c_str()) != 0;
					break;		
				case NO_OP:
					break;
			}
			break;
		
	}
	return result;
}

Project::Project(Iterator *input, const vector<string> &attrNames) : iter(input) {
	//cleaning attributes
	attrs.clear();
	this->attrNames=attrNames;
	//getting attributes
	this->iter->getAttributes(attrs);
	this->type=type;

}

Project::~Project() {
	iter->close();
	free(value);
}

RC Project::getNextTuple(void *data) {
	RC result;
	void *initialdata;
	result = iter->getNextTuple(initialdata);
	if(result != SUCCESS)
	{
		return result;
	}
	void *tempdata;
	unsigned offset=0;
	for (uint i=0; i<attrNames.size(); i++){
		for (uint j=0;j<attrs.size(); j++) {
			if(attrNames[i]==attrs[j].name) {
				ReadTupleField(initialdata, tempdata, attrs, j, type);
				memcpy(data+offset, tempdata, sizeof(tempdata));
				offset+=sizeof(tempdata);
			}
		}
	}
	return result;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
	attrs.clear();
	attrs = this->attrs;
}

void Project::ReadTupleField(void * inputData, void *outputData, vector<Attribute> attrs, 
							int pos, AttrType type)
{
	unsigned int offset = 0;
	unsigned int attrLength = 0;

	for (int i = 0; i < pos; i++) 
	{
		if (attrs[i].type == TypeInt)
			offset += sizeof(int);
		else if (attrs[i].type == TypeReal)
			offset += sizeof(float);
		else 
		{
			int stringLength = *(int *) ((char *) inputData + offset);
			offset += sizeof(int) + stringLength;
		}
	}

	if (type == TypeInt) {
		attrLength = sizeof(int);
	} else if (type == TypeReal) {
		attrLength = sizeof(float);
	} else {
		attrLength = *(int *) ((char *) inputData + offset) + sizeof(int);
	}

	memcpy(outputData, (char *) inputData + offset, attrLength);

}

INLJoin::INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        ) 
{
	inneriter=rightIn;
	outeriter=leftIn;
	outeriter->getNextTuple(outerdata);
	//cleaning attributes
	attrs.clear();
	this->condition=condition;

	//getting attributes
	this->inneriter->getAttributes(attrs);

	for(unsigned i = 0; i < attrs.size(); i++)
	{
		if(attrs[i].name == condition.lhsAttr)
		{
			pos = i;
			value = malloc(attrs[i].length + sizeof(int));
			break;
		}
	}
}
        

INLJoin::~INLJoin(){
	inneriter->close();
	outeriter->close();
	free(value);
}

RC INLJoin::getNextTuple(void *data){
	RC result;
	void *innerdata;
	result = inneriter->getNextTuple(innerdata);
	if(result != SUCCESS)
	{
		if(outerdata==NULL) {
			return result;
		}
		else {
			getNextTuple(outerdata);
			inneriter->close();
			getNextTuple(data);
		}
	}
	void *innerattr;
	void *outerattr;
	for (int j=0;j<attrs.size(); j++) {
		if(condition.lhsAttr==attrs[j].name) {
			ReadTupleField(outerdata, outerattr, attrs, j, type);
		}
		if(condition.rhsAttr==attrs[j].name) {
			ReadTupleField(innerdata, innerattr, attrs, j, type);
		}
	}
	if (innerattr==outerattr) {
		unsigned outsize=sizeof(outerdata);
		memcpy(data, outerdata, outsize);
		memcpy(data+outsize, innerdata, sizeof(innerdata));
		return result;
	}
	result=getNextTuple(data);
	return result;
}
       
void INLJoin::getAttributes(vector<Attribute> &attrs) const{
	attrs.clear();
	attrs = this->attrs;
}


void INLJoin::ReadTupleField(void * inputData, void *outputData, vector<Attribute> attrs, 
							int pos, AttrType type)
{
	unsigned int offset = 0;
	unsigned int attrLength = 0;

	for (int i = 0; i < pos; i++) 
	{
		if (attrs[i].type == TypeInt)
			offset += sizeof(int);
		else if (attrs[i].type == TypeReal)
			offset += sizeof(float);
		else 
		{
			int stringLength = *(int *) ((char *) inputData + offset);
			offset += sizeof(int) + stringLength;
		}
	}

	if (type == TypeInt) {
		attrLength = sizeof(int);
	} else if (type == TypeReal) {
		attrLength = sizeof(float);
	} else {
		attrLength = *(int *) ((char *) inputData + offset) + sizeof(int);
	}

	memcpy(outputData, (char *) inputData + offset, attrLength);

}
