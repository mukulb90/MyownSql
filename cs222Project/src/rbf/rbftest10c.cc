#include <fstream>
#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h> 
#include <string.h>
#include <stdexcept>
#include <stdio.h> 

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_10(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Open Record-Based File
    // 2. Read Multiple Records
    // 3. Close Record-Based File
    // 4. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case 10 *****" << endl;
   
    RC rc;
    string fileName = "test9";

    // Open the file "test9"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");
    
    int numRecords = 2000;
    void *record = malloc(1000);
    void *returnedData = malloc(1000);

    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    vector<RID> rids;
    vector<int> sizes;
    RID tempRID;

    // Read rids from the disk - do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    ifstream ridsFileRead("test9rids", ios::in | ios::binary);

    unsigned pageNum;
    unsigned slotNum;

    if (ridsFileRead.is_open()) {
        ridsFileRead.seekg(0,ios::beg);
        for (int i = 0; i < numRecords; i++) {
            ridsFileRead.read(reinterpret_cast<char*>(&pageNum), sizeof(unsigned));
            ridsFileRead.read(reinterpret_cast<char*>(&slotNum), sizeof(unsigned));
            if (i % 1000 == 0) {
                cout << "loaded RID #" << i << ": " << pageNum << ", " << slotNum << endl;
            }
            tempRID.pageNum = pageNum;
            tempRID.slotNum = slotNum;
            rids.push_back(tempRID);
        }
        ridsFileRead.close();
    }

    assert(rids.size() == (unsigned) numRecords && "Reading records should not fail.");

    // Read sizes vector from the disk - do not use this code in your codebase. This is not a PAGE-BASED operation - for the test purpose only.
    ifstream sizesFileRead("test9sizes", ios::in | ios::binary);

    int tempSize;
    
    if (sizesFileRead.is_open()) {
        sizesFileRead.seekg(0,ios::beg);
        for (int i = 0; i < numRecords; i++) {
            sizesFileRead.read(reinterpret_cast<char*>(&tempSize), sizeof(int));
            if (i % 1000 == 0) {
                cout << "loaded Sizes #" << i << ": " << tempSize << endl;
            }
            sizes.push_back(tempSize);
        }
        sizesFileRead.close();
    }

    assert(sizes.size() == (unsigned) numRecords && "Reading records should not fail.");

    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);


    vector<string> projectedAttributes;
    vector<Attribute> newAttrs;

    for(int i=1; i< 5; i++) {
    	Attribute attr = recordDescriptor[i];
    	projectedAttributes.push_back(attr.name);
    	newAttrs.push_back(attr);
    }

    Attribute compareAttribute = recordDescriptor[1];
    string compareAttributeName = compareAttribute.name;
    const int query = 2;

    RBFM_ScanIterator* iterator = new RBFM_ScanIterator();
    rbfm->scan(fileHandle, recordDescriptor, compareAttributeName , EQ_OP, (void*)&query , projectedAttributes, *iterator);

    RID rid;
    int count = 0;
    while(iterator->getNextRecord(rid, returnedData) != -1) {
    	rbfm->printRecord(newAttrs, returnedData);
        count++;
    }
    
    assert("number of records with id as 1 should be 1" && count == 1);


    iterator = new RBFM_ScanIterator();
	rbfm->scan(fileHandle, recordDescriptor, compareAttributeName , NE_OP, (void*)&query , projectedAttributes, *iterator);

	count = 0;
	while(iterator->getNextRecord(rid, returnedData) != -1) {
		count++;
	}

	assert("number of records with id  not equal 1 should be 1999" && count == numRecords - 1);



//	==============================================================================================
		iterator = new RBFM_ScanIterator();
		int query2 = 1000;
		rbfm->scan(fileHandle, recordDescriptor, compareAttributeName , GT_OP, (void*)&query2 , projectedAttributes, *iterator);

		count = 0;
		while(iterator->getNextRecord(rid, returnedData) != -1) {
			count++;
		}
		assert("number of records with id greater than 1000 should be equal to 999" && count == 999);

// ===============================================================================================

//	==============================================================================================
		iterator = new RBFM_ScanIterator();
		query2 = 1000;
		rbfm->scan(fileHandle, recordDescriptor, compareAttributeName , GE_OP, (void*)&query2 , projectedAttributes, *iterator);

		count = 0;
		while(iterator->getNextRecord(rid, returnedData) != -1) {
			count++;
		}

		assert("number of records with id greater than or equal to 1000 should be equal to 1001" && count == 1000);

// ===============================================================================================

//	==============================================================================================
		iterator = new RBFM_ScanIterator();
		query2 = 500;
		rbfm->scan(fileHandle, recordDescriptor, compareAttributeName , LT_OP, (void*)&query2 , projectedAttributes, *iterator);

		count = 0;
		while(iterator->getNextRecord(rid, returnedData) != -1) {
			if(count == 499) {
				cout << "print";
			}
			count++;
		}

		assert("number of records with id less than 500 should be equal to 500" && count == 500);

// ===============================================================================================

//	==============================================================================================
		iterator = new RBFM_ScanIterator();
		query2 = 500;
		rbfm->scan(fileHandle, recordDescriptor, compareAttributeName , LE_OP, (void*)&query2 , projectedAttributes, *iterator);

		count = 0;
		while(iterator->getNextRecord(rid, returnedData) != -1) {
			count++;
		}

		assert("number of records with id less than or equalto 500 should be equal to 500" && count == 501);

// ===============================================================================================

    cout << endl;

    // Close the file "test9"
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");
    
//    rc = rbfm->destroyFile(fileName);
//    assert(rc == success && "Destroying the file should not fail.");

//    rc = destroyFileShouldSucceed(fileName);
//    assert(rc == success  && "Destroying the file should not fail.");

    free(record);
    free(returnedData);

    cout << "RBF Test Case 10 Finished! The result will be examined." << endl << endl;
    
    return 0;
}

int main()
{
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 
     
    RC rcmain = RBFTest_10(rbfm);
    return rcmain;
}
