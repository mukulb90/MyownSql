#ifndef _pf_h_
#define _pf_h_

#include <string>

using namespace std;

class PagedFile {

public:
	string name;
	int numberOfPages;

	PagedFile(string fileName);
	PagedFile(string fileName, int numberOfPages);
	~PagedFile();

	int getOccupiedMemory() const;
	static int serialize(const PagedFile *  pagedFile);
	static int deserialize(string fileName, PagedFile * pagedFile);

};

class PagedFileObjectMapper {

public:

	static int mapToObject(PagedFile * pagedFile, const void *);
	static int mapFromObject(void* , const PagedFile * pagedFile);

};

#endif
