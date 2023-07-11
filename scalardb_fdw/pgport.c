#include "pgport.h"

#if PG_VERSION_NUM < 150000
extern Value *makeBoolean(bool val)
{
	return makeInteger((int)val);
}
#endif
