#ifndef _page_h_
#define _page_h_

#include "abstract_serializable.h"

class Page: public Serializable {
public:
	int pageId;
	Page(int pageId);

	int getBytes();
	int mapFromObject(void* data);
	int mapToObject(void* data);

};

#endif
