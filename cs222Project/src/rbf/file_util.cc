#include "file_util.h"

using namespace std;

bool fileExists(string fileName) {
	FILE* handle = fopen(fileName.c_str(), "rb");
	bool exists = handle != 0;
	fclose(handle);
	return exists;
}

unsigned long fsize(char * fileName) {
	if(!fileExists(fileName)) {
		return 0;
	}
	FILE* f = fopen(fileName, "rb");
	fseek(f, 0, SEEK_END);
	unsigned long len = (unsigned long) ftell(f);
	fclose(f);
	return len;
}

string getParentPath(string &fileName) {
	size_t found;
	found=fileName.find_last_of("/\\");
	return fileName.substr(0,found);
}

