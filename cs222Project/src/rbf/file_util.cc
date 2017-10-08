#include "file_util.h"

using namespace std;

bool fileExists(string fileName) {
	FILE* handle = fopen(fileName.c_str(), "rb");
	return handle != 0;
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
