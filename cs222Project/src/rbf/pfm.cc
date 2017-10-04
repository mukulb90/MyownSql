#include "pfm.h"

#include <stdlib.h>
#include <cstdio>
#include <exception>
#include <fstream>

#include "file_util.h"


PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}


PagedFileManager::PagedFileManager()
{
}


PagedFileManager::~PagedFileManager()
{
	free(_pf_manager);
}


RC PagedFileManager::createFile(const string &fileName)
{
//	TODO File should be created in a way that it is easy to differentiate these files
	if(fileExists(fileName)) {
		return -1;
	}
	ofstream stream;
	stream.open(fileName);
	stream.close();
	return 0;
}


RC PagedFileManager::destroyFile(const string &fileName)
{
	if(!fileExists(fileName)){
		return -1;
	}
	return remove(fileName.c_str());
}


RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
	if(!fileExists(fileName)){
		return -1;
	}
	fstream stream;
	try {
		stream.open(fileName.c_str());
		fileHandle.openFile(fileName);
	} catch (exception &e) {
		return -1;
	}
	return 0;
}


RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return -1;
}


FileHandle::FileHandle()
{
    readPageCounter = 0;
    writePageCounter = 0;
    appendPageCounter = 0;
    fileName = string();
}


FileHandle::~FileHandle()
{
}

RC FileHandle::openFile(const string &fileName) {
	this -> fileName = fileName;
	return 0;
}

RC FileHandle::closeFile(const string &fileName) {
	this->fileName = "";
	return 0;
}

RC FileHandle::readPage(PageNum pageNum, void *data)
{

    return -1;
}


RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    return -1;
}


RC FileHandle::appendPage(const void *data)
{
    return -1;
}


unsigned FileHandle::getNumberOfPages()
{
    return -1;
}


RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    return -1;
}
