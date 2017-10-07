#include <sys/stat.h>
#include <cstdio>
#include <iostream>
#include <string>

bool fileExists(std::string fileName) {
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) == 0)
		return true;
	else
		return false;
}

unsigned long fsize(FILE* f) {
	fseek(f, 0, SEEK_END);
	unsigned long len = (unsigned long) ftell(f);
	return len;
}
