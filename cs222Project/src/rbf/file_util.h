#include <sys/stat.h>
#include <iostream>
#include <string>

bool fileExists(std::string fileName) {
	struct stat stFileInfo;

	if (stat(fileName.c_str(), &stFileInfo) == 0)
		return true;
	else
		return false;
}
