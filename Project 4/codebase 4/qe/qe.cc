
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

	for(unsigned i = 0; i < attrs.size(); ,i++)
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

RC Filter::getNextTuple(void *data): itr(input) 
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
	}while(!Compare(this->value, condition, type, op))
	
	return result
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
			int stringLength = *(int *) ((char *) input + offset);
			offset += sizeof(int) + stringLength;
		}
	}

	if (type == TypeInt) {
		attrLength = sizeof(int);
	} else if (type == TypeReal) {
		attrLength = sizeof(float);
	} else {
		attrLength = *(int *) ((char *) input + offset) + sizeof(int);
	}

	memcpy(outputData, (char *) inputData + offset, attrLength);

}

bool Filter::Compare(const void *attribute, const void *condition, AttrType type, CompOp op)
{
	if(condition == NULL)		
		return true;

	bool result = true;

	switch(type)
	{
		case TypeInt:
			int attrInt = *(int*)attribute;
			int condInt = *(int*)condition;
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
			float attrFloat = *(float *) attribute;
			float condFloat = *(float *) condition;

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
			int attributeLength = *(int *) attribute;
			string attrStr;
			attrStr.assign((char*)attribute + sizeof(int), 0, attributeLength);
			int conditionLength = *(int *) condition;
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
}