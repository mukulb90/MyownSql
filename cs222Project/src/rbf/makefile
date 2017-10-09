include ../makefile.inc

all: librbf.a rbftest1 rbftest2 rbftest3 rbftest4 rbftest5 rbftest6 rbftest7 rbftest8 rbftest8b rbftest9 rbftest10 rbftest11 rbftest12 rbtest13 file_handle_test slot_test slot_directory_test

# c file dependencies
abstract_serializable.o: abstract_serializable.h
file_util.o : file_util.h
pfm.o: pfm.h
rbfm.o: rbfm.h
pf.o: pf.h
page.o : page.h
slot.o : slot.h
slot_directory.o : slot_directory.h

# lib file dependencies
librbf.a: librbf.a(pfm.o)
librbf.a: librbf.a(file_util.o)
librbf.a: librbf.a(abstract_serializable.o)
librbf.a: librbf.a(pf.o)
librbf.a: librbf.a(rbfm.o)
librbf.a: librbf.a(page.o)
librbf.a: librbf.a(slot.o)
librbf.a: librbf.a(slot_directory.o)

# and possibly other .o files
rbftest1.o: pfm.h rbfm.h
rbftest2.o: pfm.h rbfm.h
rbftest3.o: pfm.h rbfm.h
rbftest4.o: pfm.h rbfm.h
rbftest5.o: pfm.h rbfm.h
rbftest6.o: pfm.h rbfm.h
rbftest7.o: pfm.h rbfm.h
rbftest8.o: pfm.h rbfm.h
rbftest8b.o: pfm.h rbfm.h
rbftest9.o: pfm.h rbfm.h
rbftest10.o: pfm.h rbfm.h
rbftest11.o: pfm.h rbfm.h
rbftest12.o: pfm.h rbfm.h
rbtest13.o: pf.h
file_handle_test.o: pfm.h

# binary dependencies
rbftest1: rbftest1.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest2: rbftest2.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest3: rbftest3.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest4: rbftest4.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest5: rbftest5.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest6: rbftest6.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest7: rbftest7.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest8: rbftest8.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest8b: rbftest8b.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest9: rbftest9.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest10: rbftest10.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest11: rbftest11.o librbf.a $(CODEROOT)/rbf/librbf.a
rbftest12: rbftest12.o librbf.a $(CODEROOT)/rbf/librbf.a
rbtest13: rbtest13.o librbf.a $(CODEROOT)/rbf/librbf.a
file_handle_test: file_handle_test.o librbf.a $(CODEROOT)/rbf/librbf.a
slot_test: slot_test.o librbf.a $(CODEROOT)/rbf/librbf.a
slot_directory_test: slot_directory_test.o librbf.a $(CODEROOT)/rbf/librbf.a

# dependencies to compile used libraries
.PHONY: $(CODEROOT)/rbf/librbf.a
$(CODEROOT)/rbf/librbf.a:
	$(MAKE) -C $(CODEROOT)/rbf librbf.a

.PHONY: clean
clean:
	-rm rbftest1 rbftest2 rbftest3 rbftest4 rbftest5 rbftest6 rbftest7 rbftest8 rbftest8b rbftest9 rbftest10 rbftest11 rbftest12 rbtest13 file_handle_test *.a *.o slot_test slot_directory_test 
