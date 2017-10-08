#ifndef _page_h_
#define _page_h_

#include "abstract_serializable.h"

class Page: public Serializable {
public:
	void * data;
	Page();
	Page(void * data);
	~Page();

	int getBytes();
	int mapFromObject(void* data);
	int mapToObject(void* data);

};

#endif
