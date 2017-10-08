#ifndef _file_util_h_
#define _file_util_h_

#include <cstdio>
#include <iostream>
#include <string>

using namespace std;

bool fileExists(string fileName);

unsigned long fsize(char * fileName);

string getParentPath(string &fileName);

#endif
