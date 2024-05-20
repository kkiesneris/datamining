OCI_INCLUDE_DIR=/usr/include/oracle/21/client64
OCI_LIB_DIR=/usr/lib/oracle/21/client64/lib

all: LoadPOSData LoadATMData

LoadPOSData: LoadPOSData.cpp
	g++ -g -o LoadPOSData LoadPOSData.cpp \
	tinyxml2.cpp tinyxml2.h \
	-I$(OCI_INCLUDE_DIR) \
	-L$(OCI_LIB_DIR) -lclntsh -locci

LoadATMData: LoadATMData.cpp
	g++ -g -o LoadATMData LoadATMData.cpp \
	tinyxml2.cpp tinyxml2.h \
	-I$(OCI_INCLUDE_DIR) \
	-L$(OCI_LIB_DIR) -lclntsh -locci

clean:
	rm -f LoadPOSData
	rm -f LoadATMData
