#include<string>

class Serializable {
public :
	int deserialize(FILE* handle);
	int serialize(FILE* handle);
	virtual int getBytes() = 0;
	virtual int mapFromObject(void* data) = 0;
	virtual int mapToObject(const void* data) = 0;
};
